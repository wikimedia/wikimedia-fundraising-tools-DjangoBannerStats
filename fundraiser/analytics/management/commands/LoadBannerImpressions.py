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

    logger = logging.getLogger("fundraiser.analytics.load_banners")

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

    impression_sql = "INSERT INTO `bannerimpression_raw` (timestamp, squid_id, squid_sequence, banner, campaign, project_id, language_id, country_id, sample_rate) VALUES %s"

    pending_impressions = []

    debug_info = []
    debug_count = 0

    counts = {
        "countries" : {},
        "languages" : {},
    }

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
                # TODO: lots of things
                now = "bannerimpressions-%s*" % datetime.now().strftime("%Y%m%d-%H")
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
                    self.logger.info("\tSQUID: %d OKAY / %d FAILED with %d IGNORED and %d 404s" % (
                        int(results["squid"]["match"]),
                        int(results["squid"]["nomatch"]),
                        int(results["squid"]["ignored"]),
                        int(results["squid"]["404"])
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
            self.logger.error("Error loading banner impressions - No file specified")
            return

        if not os.path.exists(filename):
            self.logger.error("Error loading banner impressions - File %s does not exist" % filename)
            return

        self.logger.error("Processing %s" % filename)

        results = {
            "squid" : {
                "match" : 0,
                "nomatch" : 0,
                "ignored" : 0,
                "404" : 0,
            },
            "impression" : {
                "match" : 0,
                "nomatch" : 0,
                "ignored" : 0,
                "error" : 0
            }
        }

        batch_size = 1500

        sample_rate = 1 # TODO: calculate the sample rate

        file = gzip.open(filename, 'rb')
        try:
            i = 0

            for l in file:
                i += 1
                try:
                    m = squidline.match(l)
                    if not m:
                        # TODO: do we want to write the failed lines to another file for reprocessing?
                        results["squid"]["nomatch"] += 1
                        if self.verbose:
                            if results["squid"]["nomatch"] < 100:
                                self.logger.info("*** NO MATCH FOR LANDING PAGE IMPRESSION ***")
                                self.logger.info("--- File: %s | Line: %d ---" % (filename, i+1))
                                self.logger.info(l[:500])
                                if len(l) > 500:
                                    self.logger.info("...TRUNCATED...")
                                self.logger.info("*** END OF NO MATCH ***")
                    else:
                        results["squid"]["match"] += 1

                        # Go ahead and ignore SSL termination logs since they are missing GET params
                        # and are followed by a proper squid log for the request
                        if m.group("squid")[:3] == "ssl":
                            results["squid"]["ignored"] += 1
                            continue

                        # Ignore 404s
                        if m.group("squidstatus")[-3:] == "404":
                            results["squid"]["404"] += 1
                            continue

                        # Also ignore anything coming from ALuminium or Grosley
                        if m.group("client") == "208.80.154.6" or m.group("client") == "208.80.152.164":
                            results["impression"]["ignored"] += 1
                            continue

                        # Also ignore anything forward for ALuminium or Grosley
                        if m.group("xff") == "208.80.154.6" or m.group("xff") == "208.80.152.164":
                            results["impression"]["ignored"] += 1
                            continue

                        # And ignore all of our testing UA's
                        for ua in ignore_uas:
                            if ua.match(m.group("useragent")):
                                results["impression"]["ignored"] += 1
                                continue

                        squid = lookup_squidhost(hostname=m.group("squid"), verbose=self.verbose)
                        seq = int(m.group("sequence"))
                        timestamp = datetime.strptime(m.group("timestamp"), "%Y-%m-%dT%H:%M:%S.%f")
                        url = urlparse.urlparse(m.group("url"))
                        qs = urlparse.parse_qs(url.query, keep_blank_values=True)

                        country = qs["country"][0] if "country" in qs else "XX"
                        language = qs["uselang"][0] if "uselang" in qs else "en"

                        if country in self.counts["countries"]:
                            self.counts["countries"][country] += 1
                        else:
                            self.counts["countries"][country] = 1

                        if language in self.counts["languages"]:
                            self.counts["languages"][language] += 1
                        else:
                            self.counts["languages"][language] = 1

                        banner = ""
                        if "banner" in qs:
                            banner = qs["banner"][0]
                        campaign = ""
                        if "campaign" in qs:
                            campaign = qs["campaign"][0]
                        project = None
                        if "db" in qs:
                            project = lookup_project(qs["db"][0])

                        language = lookup_language(language)
                        country = lookup_country(country)

                        # not using the models here saves a lot of wall time
                        try:
                            banner_tmp = "('%s', %d, %d, '%s', '%s', %d, %d, %d, %d)" % (
                                MySQLdb.escape_string(timestamp.strftime("%Y-%m-%d %H:%M:%S")),
                                squid.id,
                                seq,
                                MySQLdb.escape_string(banner),
                                MySQLdb.escape_string(campaign),
                                project.id,
                                language.id,
                                country.id,
                                sample_rate
                                )
                            self.pending_impressions.append(banner_tmp)
                            results["impression"]["match"] += 1

                        except Exception:
                            results["impression"]["error"] += 1
                            self.logger.exception("** UNHANDLED EXCEPTION WHILE PROCESSING BANNER IMPRESSION **")
                            self.logger.error("********************\n%s\n********************" % l)

                        finally:
                            banner_tmp = ""

                        # write the models in batch
                        if len(self.pending_impressions) % batch_size == 0:
                            try:
                                if not self.debug:
                                    self.write(self.pending_impressions)
                            except Exception:
                                self.logger.exception("Error writing impressions to the database")
                            finally:
                                self.pending_impressions = []

                except Exception as e:
                    results["impression"]["error"] += 1
                    self.logger.exception("** UNHANDLED EXCEPTION WHILE PROCESSING BANNER IMPRESSION **")
                    self.logger.error("********************\n%s\n********************" % l)

            try:
                # write out any remaining records
                if not self.debug:
                    self.write(self.pending_impressions)
                self.pending_impressions = []

            except Exception as e:
                self.logger.exception("** UNHANDLED EXCEPTION WHILE PROCESSING LANDING PAGE IMPRESSION **")
                self.logger.error("********************")

        except IOError:
            pass
        finally:
            file.close()

        gc.collect()

        return results


    @transaction.commit_manually
    def write(self, impressions):
        """
        Commits a batch of transactions. Attempts a single query per model by splitting the
        tuples of each banner impression and grouping by model.  If that fails, the function
        falls back to a single transaction per banner impression
        """
        cursor = connections['default'].cursor()

        i_len = len(impressions)

        if not i_len:
            return

        try:
            # attempt to create all in batches
            cursor.execute(self.impression_sql % ', '.join(impressions))

            transaction.commit('default')

        except IntegrityError as e:
            # some impression was not happy
            transaction.rollback('default')

            if i_len == 1:
                return

            for i in impressions:
                self.write([impressions[i]])

        except Exception as e:
            transaction.rollback()

            self.logger.exception("UNHANDLED EXCEPTION")

            if self.debug:
                self.logger.info(self.impression_sql % ', '.join(impressions))

                for r in self.debug_info:
                    self.logger.info("\t%s" % r)

            if i_len == 1:
                return

            for i in impressions:
                self.write([impressions[i]])