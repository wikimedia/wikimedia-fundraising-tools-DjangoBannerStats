from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.utils import IntegrityError

import gc
from datetime import datetime, timedelta
import glob
import gzip
import logging
import MySQLdb
from optparse import make_option
import os
import traceback
from urllib import unquote
import urlparse

from fundraiser.analytics.functions import *
from fundraiser.analytics.models import *
from fundraiser.analytics.regex import *
from fundraiser.settings import UDP_LOG_PATH

class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        make_option('-f', '--file',
            dest='filename',
            default=None,
            help='Specify the input file'),
        make_option('', '--verbose',
            dest='verbose',
            action='store_true',
            default=False,
            help='Provides more verbose output.'),
        make_option('', '--debug',
            dest='debug',
            action='store_true',
            default=False,
            help='Do not save the impressions. Parse only.'),
        make_option('', '--recent',
            dest='recent',
            action='store_true',
            default=False,
            help='Process recent logs.'),
        )

    help = 'Parses the specified squid log file and stores the impression in the database.'

    def handle(self, *args, **options):
        try:
            starttime = datetime.now()
            filename = options.get('filename')
            self.debug = options.get('debug')
            self.verbose = options.get('verbose')
            recent = options.get('recent')

            self.matched = 0
            self.nomatched = 0
            self.ignored = 0

            files = []
            if recent:
                now = "landingpages-%s*" % datetime.now().strftime("%Y%m%d-%H")
                pasthour = "landingpages-%s*" % (datetime.now() - timedelta(hours=1)).strftime("%Y%m%d-%H")

                files.extend(glob.glob(os.path.join(UDP_LOG_PATH, now)))
                files.extend(glob.glob(os.path.join(UDP_LOG_PATH, pasthour)))
            else:
                if os.path.isdir(filename):
                    self.logger.info("Processing directory")
                    filename = filename.rstrip('/')
                    files = glob.glob("%s/*.gz" % filename)
                else:
                    self.logger.info("Processing files matching %s" % filename)
                    files = glob.glob(filename)

            for f in files:
                path, filename_only = f.rsplit('/', 1)
                if not os.path.isdir(f):
                    existing = SquidLog.objects.filter(filename=filename_only)
                    if existing:
                        self.logger.debug("Already processed %s  - skipping" % f)
                        continue

                    results = self.process_file(f)

                    sq = SquidLog(filename=filename_only, impressiontype="landingpage")
                    sq.timestamp = sq.filename2timestamp()
                    if not self.debug:
                        sq.save()

                    self.matched += results["squid"]["match"]
                    self.nomatched += results["squid"]["nomatch"]

                    self.logger.info("DONE - %s" % f)
                    self.logger.info("\tSQUID: %d OKAY / %d FAILED" % (
                        int(results["squid"]["match"]),
                        int(results["squid"]["nomatch"])
                        ))
                    self.logger.info("\tIMPRESSIONS: %d MATCHED / %d NOMATCH with %d IGNORED / %d ERROR" % (
                        results["impression"]["match"],
                        results["impression"]["nomatch"],
                        results["impression"]["ignored"],
                        results["impression"]["error"],
                        ))

            endtime = datetime.now()
            self.logger.info("DONE")
            self.logger.info("Total squid matched: %d" % self.matched)
            self.logger.info("Total squid not matched: %d" % self.nomatched)
            self.logger.info("Finished in %d.%d seconds" % ((endtime - starttime).seconds, (endtime - starttime).microseconds))
        except Exception:
            self.logger.exception("Error processing files")

        for c,v in self.counts["countries"].iteritems():
            print "%s - %s" % (c, v)
        print "----------------------\n----------------------"
        for l,v in self.counts["languages"].iteritems():
            print "%s - %s" % (l, v)


    def process_file(self, filename=None):
        if filename is None:
            print "Error loading banner impressions - No file specified"
            return

        if not os.path.exists(filename):
            print "Error loading banner impressions - File %s does not exist" % filename
            return

        print "Processing %s" % filename

        matched = 0
        nomatched = 0

        batch_size = 1500
        batch_models = []

        with open(filename, 'r') as file:
            for l in file:
                m = squidline.match(l)
                if not m:
                    # TODO: do we want to write the failed lines to another file for reprocessing?
                    nomatched += 1
                    if self.debug:
                        print "NO MATCH - %s" % l
                    if self.verbose:
                        if nomatched < 100:
                            print "*** NO MATCH FOR BANNER IMPRESSION ***"
                            print l
                            print "*** END OF NO MATCH ***"
                else:
                    matched += 1

                    squid = lookup_squidhost(hostname=m.group("squid"), verbose=self.verbose)
                    seq = int(m.group("sequence"))
                    timestamp = datetime.strptime(m.group("timestamp"), "%Y-%m-%dT%H:%M:%S.%f")
                    url = urlparse.urlparse(m.group("url"))
                    qs = urlparse.parse_qs(url.query, keep_blank_values=True)

                    banner = ""
                    if "banner" in qs:
                        banner = qs["banner"][0]
                    campaign = ""
                    if "campaign" in qs:
                        campaign = qs["campaign"][0]
                    project = None
                    if "db" in qs:
                        project = lookup_project(qs["db"][0])
                    language = None
                    if "userlang" in qs:
                        language = lookup_language(qs["userlang"][0])
                    country = None
                    if "country" in qs:
                        country = lookup_country(qs["country"][0])

                    # not using the models here saves a lot of wall time
                    batch_models.append((
                        (squid.id, seq, timestamp.strftime("%Y-%m-%d %H:%M:%S")),
                        (timestamp.strftime("%Y-%m-%d %H:%M:%S"), banner, campaign,
                            project.id, language.id, country.id)
                    ))

                    # write the models in batch
                    if len(batch_models) % batch_size == 0:
                        self.write(batch_models)
                        batch_models = []

            # write out any remaining in the list
            self.write(batch_models)
            batch_models = []

        return matched, nomatched

    @transaction.commit_manually
    def write(self, list):
        """
        Commits a batch of transactions. Attempts a single query per model by splitting the
        tuples of each banner impression and grouping by model.  If that fails, the function
        falls back to a single transaction per banner impression
        """
        cursor = connections['default'].cursor()

        squid_sql = "INSERT INTO `squidrecord` (squid_id, sequence, timestamp) VALUES %s"
        banner_sql = "INSERT INTO `bannerimpression_raw` (timestamp, banner, campaign, project_id, language_id, country_id) VALUES %s"

        squid_values = []
        banner_values = []

        for s,b in list:
            squid_values.append(
                "(%s, %s, '%s')" % (
                    MySQLdb.escape_string(str(s[0])),
                    MySQLdb.escape_string(str(s[1])),
                    MySQLdb.escape_string(str(s[2]))
                )
            )
            banner_values.append(
                "('%s', '%s', '%s', %s, %s, %s)" % (
                    MySQLdb.escape_string(str(b[0])),
                    MySQLdb.escape_string(str(b[1])),
                    MySQLdb.escape_string(str(b[2])),
                    MySQLdb.escape_string(str(b[3])),
                    MySQLdb.escape_string(str(b[4])),
                    MySQLdb.escape_string(str(b[5]))
                )
            )
        try:
            # attempt to create all in batches
            cursor.execute(squid_sql % ', '.join(squid_values))
            cursor.execute(banner_sql % ', '.join(banner_values))
            transaction.commit('default')
        except IntegrityError:
            # someone was not happy, likely a SquidRecord
            # TODO: break the batch into smaller batches and retry
            transaction.rollback('default')