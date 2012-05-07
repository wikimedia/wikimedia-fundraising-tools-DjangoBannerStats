from django.core.management.base import BaseCommand
from django.db import connections, transaction

from datetime import datetime
import MySQLdb
from optparse import make_option
import os
import urlparse
from django.db.utils import IntegrityError

from fundraiser.analytics.functions import *
from fundraiser.analytics.regex import *

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
        )
    help = 'Parses the specified squid log file and stores the impression in the database.'


    def handle(self, *args, **options):
        try:
            starttime = datetime.now()
            filename = options.get('filename')
            self.debug = options.get('debug')
            self.verbose = options.get('verbose')

            self.matched = 0
            self.nomatched = 0
            self.ignored = 0

            if os.path.isdir(filename):
                print "Processing directory"
                for f in os.listdir(filename):

                    subfile = os.path.join(filename,f)

                    if not os.path.isdir(subfile):
                        m, n, i = self.process_file(subfile)
                        self.matched += m
                        self.nomatched += n
                        self.ignored += i

#                        if n == 0:
#                            os.renames(subfile, os.path.join(filename, 'processed', f))

                        if n == -1:
                            break

                        print "DONE - %d OKAY / %d FAILED (IGNORED: %d) - %s" % (m, n, i, subfile)
            else:
                m, n, i = self.process_file(filename)

                self.matched += m
                self.nomatched += n
                self.ignored += i

#                if n == 0:
#                    os.renames(subfile, os.path.join(filename, 'processed', f))

                print "DONE - %d OKAY / %d FAILED (IGNORED: %d) - %s" % (m, n, i, filename)

            endtime = datetime.now()
            print "DONE"
            print "Total matched: %d" % self.matched
            print "Total not matched: %d" % self.nomatched
            print "Finished in %d seconds" % (endtime - starttime).seconds
        except Exception as e:
            import traceback
            traceback.print_exc()
            print e


    def process_file(self, filename=None):
        if filename is None:
            print "Error loading landing page impressions - No file specified"
            return

        if not os.path.exists(filename):
            print "Error loading landing page impressions - File %s does not exist" % filename
            return

        print "Processing %s" % filename

        matched = 0
        nomatched = 0
        ignored = 0

        batch_size = 20
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
                            print "*** NO MATCH FOR LANDING PAGE IMPRESSION ***"
                            print l
                            print "*** END OF NO MATCH ***"
                else:
                    matched += 1

                    # check to see if we want this impression
                    record = re.search(r"utm_source", m.group("url"), flags=re.VERBOSE|re.IGNORECASE)
                    if not record:
                        ignored += 1
                        continue

                    url = urlparse.urlparse(m.group("url"))
                    qs = urlparse.parse_qs(url.query, keep_blank_values=True)

                    squid = lookup_squidhost(hostname=m.group("squid"), verbose=self.verbose)
                    seq = int(m.group("sequence"))
                    timestamp = datetime.strptime(m.group("timestamp"), "%Y-%m-%dT%H:%M:%S.%f")

                    utm_source = ""
                    if "utm_source" in qs:
                        utm_source = qs["utm_source"][0]
                    utm_campaign = ""
                    if "utm_campaign" in qs:
                        utm_campaign = qs["utm_campaign"][0]
                    utm_medium = ""
                    if "utm_medium" in qs:
                        utm_medium = lookup_project(qs["utm_medium"][0])

                    landingpage = ""
                    if "title" in qs:
                        if qs["title"][0].lower() == "Special:LandingCheck".lower():
                            if "landing_page" in qs:
                                landingpage = lookup_language(qs["landing_page"][0])
                        else:
                            landingpage = qs["title"][0]
                    elif "landing_page" in qs:
                        landingpage = qs["landing_page"][0]
                    elif url.path:
                        tmp = re.match(r"wiki/(?P<page>[\S]+)")
                        if tmp:
                            landingpage = tmp.group("page")

                    language = None
                    if "language" in qs:
                        language = lookup_language(qs["language"][0])
                    country = None
                    if "country" in qs:
                        country = lookup_country(qs["country"][0])

                    # TODO: implement some wmfwiki/donate check
                    project_id = 0

                    if landingpage is "":
                        print m.group("url")

#                    continue

                    # not using the models here saves a lot of wall time
                    batch_models.append((
                        (squid.id, seq, timestamp.strftime("%Y-%m-%d %H:%M:%S")),
                        (timestamp.strftime("%Y-%m-%d %H:%M:%S"), utm_source,
                            utm_campaign, utm_medium, landingpage, project_id,
                            0, 0)
#                            language.id, country.id)
                    ))



                    # write the models in batch
                    if len(batch_models) % batch_size == 0:
                        self.write(batch_models)
                        batch_models = []
#                        return matched, nomatched, ignored

            # write out any remaining in the list
            self.write(batch_models)
            batch_models = []

        return matched, nomatched, ignored

    @transaction.commit_manually
    def write(self, list):
        """
        Commits a batch of transactions. Attempts a single query per model by splitting the
        tuples of each banner impression and grouping by model.  If that fails, the function
        falls back to a single transaction per banner impression
        """
        cursor = connections['default'].cursor()

        squid_sql = "INSERT INTO `squidrecord` (squid_id, sequence, timestamp) VALUES %s"
        lp_sql = "INSERT INTO `landingpageimpression_raw` (timestamp, utm_source, utm_campaign, utm_medium, landingpage, project_id, language_id, country_id) VALUES %s"

        squid_values = []
        lp_values = []

        try:
            for s,b in list:
                squid_values.append(
                    "(%s, %s, '%s')" % (
                        MySQLdb.escape_string(str(s[0])),
                        MySQLdb.escape_string(str(s[1])),
                        MySQLdb.escape_string(str(s[2]))
                    )
                )
                lp_values.append(
                    "('%s', '%s', '%s', '%s', '%s', %s, %s, %s)"% (
                        MySQLdb.escape_string(str(b[0])),
                        MySQLdb.escape_string(str(b[1])),
                        MySQLdb.escape_string(str(b[2])),
                        MySQLdb.escape_string(str(b[3])),
                        MySQLdb.escape_string(str(b[4])),
                        MySQLdb.escape_string(str(b[5])),
                        MySQLdb.escape_string(str(b[6])),
                        MySQLdb.escape_string(str(b[7]))
                    )
                )
        except Exception as e:
            import traceback
            traceback.print_exc()
            print e
            print "UNHANDLED EXCEPTION"
        try:
            # attempt to create all in batches
#            print "INSERTING SQUID"
            cursor.execute(squid_sql % ', '.join(squid_values))
#            print "INSERTING LP"
            cursor.execute(lp_sql % ', '.join(lp_values))
#            print "COMMITTING"
            transaction.commit('default')
#            print "DONE"
        except IntegrityError:
#            print "ROLLING BACK"
            # someone was not happy, likely a SquidRecord
            # TODO: break the batch into smaller batches and retry
            transaction.rollback('default')
            if len(list) > 1:
                for l in list:
                    self.write([l])
        except Exception as e:
            import traceback
            traceback.print_exc()
            print e
            print "UNHANDLED EXCEPTION"
            transaction.rollback()
            if len(list) > 1:
                for l in list:
                    self.write([l])