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
            print "Finished in %d.%d seconds" % ((endtime - starttime).seconds, (endtime - starttime).microseconds)
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

        batch_size = 1000
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
                            print l,
                            print "*** END OF NO MATCH ***"
                else:
                    matched += 1

                    # FOR DEBUG ONLY TO SEE IF WE ARE MATCHING EVERYTHING WE WANT
#                    ignore = False
                    # check to see if we want this impression
#                    for r in landingpages_ignore:
#                        if r.match(m.group("url")):
#                            ignore = True
#                            break
#                    if ignore:
#                        ignored += 1
#                        continue

                    record = False

                    # check the landing page patterns
                    for r in landingpages:
                        record = r.match(m.group("url"))
                        if record:
#                            print m.group("url")
                            break

                    if not record:
                        ignored += 1
#                        print "NOT IGNORED & NOT MATCHED: %s" % m.group("url")
                        continue

                    # go ahead and parse the URL
                    url = urlparse.urlparse(m.group("url"))
                    qs = urlparse.parse_qs(url.query, keep_blank_values=True)

                    # grab the tracking information that should be common to any LP
                    utm_source = ""
                    utm_campaign = ""
                    utm_medium = ""

                    if "utm_source" in qs:
                        utm_source = qs["utm_source"][0]
                    if "utm_campaign" in qs:
                        utm_campaign = qs["utm_campaign"][0]
                    if "utm_medium" in qs:
                        utm_medium = qs["utm_medium"][0]

                    landingpage = ""
                    language = None
                    country = None
                    project = None

                    if record.group("sitename") == "wikimediafoundation.org":
                        project = lookup_project("foundationwiki")

                        split = record.group("landingpage").rsplit('/', 2)
                        lang, coun = ('','')

                        if len(split) == 3:
                            landingpage, lang, coun = split
                        elif len(split) == 2:
                            landingpage, lang = split
                            coun = 'XX'
                        elif len(split) == 1:
                            landingpage = split
                            coun = 'XX'
                            lang = 'en'
                        else:
                            # uh oh, TODO: do something informative
                            pass

                        # deal with the payment-processing chapters and their pages
                        if lang in ('CH','DE','GB','FR'):
                            language = lookup_language(coun)
                            country = lookup_country(lang)
                        else:
                            language = lookup_language(lang)
                            country = lookup_country(coun)

                    elif record.group("sitename") == "donate.wikimedia.org":
                        project = lookup_project("donatewiki")

                        flp_vars = {
                            "appeal" : qs["appeal"][0] if "appeal" in qs else "default",
                            "country" : qs["country"][0] if "country" in qs else "XX",
                            "language" : qs["uselang"][0] if "uselang" in qs else "en",
                            "template" : qs["template"][0] if "template" in qs else "default",
                            "form-template" : qs["form-template"][0] if "form-template" in qs else "default",
                            "appeal-template" : qs["appeal-template"][0] if "appeal-template" in qs else "default",
                            "form-countryspecific" : qs["form-countryspecific"][0] if "form-countryspecific" in qs else "default",
                        }

                        # go ahead and remove the cruft from the lp variables
                        for k,v in flp_vars.iteritems():
                            before,sep,after = v.rpartition('-')
                            if after:
                                flp_vars[k] = after

                        landingpage = '~'.join([
                            flp_vars["template"],
                            flp_vars["appeal-template"],
                            flp_vars["appeal"],
                            flp_vars["form-template"],
                            flp_vars["form-countryspecific"]
                        ])
                        language = lookup_language(flp_vars["language"])
                        country = lookup_language(flp_vars["country"])

                    else:
                        # TODO: we have a weird problem, do something
                        pass

                    if landingpage is "" or language is None or country is None or project is None:
                        # something odd does not quite match in this request
                        # TODO: do something informative
                        print m.group("url")
                        continue

                    squid = lookup_squidhost(hostname=m.group("squid"), verbose=self.verbose)
                    seq = int(m.group("sequence"))
                    timestamp = datetime.strptime(m.group("timestamp"), "%Y-%m-%dT%H:%M:%S.%f")

                    # not using the models here saves a lot of wall time
                    batch_models.append((
                        (squid.id, seq, timestamp.strftime("%Y-%m-%d %H:%M:%S")),
                        (timestamp.strftime("%Y-%m-%d %H:%M:%S"), utm_source,
                            utm_campaign, utm_medium, landingpage, project.id,
                            language.id, country.id)
                    ))

                    # write the models in batch
                    if len(batch_models) % batch_size == 0:
                        self.write(batch_models)
                        batch_models = []

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
            if len(squid_values) > 0 and len(lp_values) > 0:
            # attempt to create all in batches
                cursor.execute(squid_sql % ', '.join(squid_values))
                cursor.execute(lp_sql % ', '.join(lp_values))
                transaction.commit('default')
                return
            transaction.rollback('default')
        except IntegrityError:
            # someone was not happy, likely a SquidRecord
            # TODO: break the batch into smaller batches and retry
            transaction.rollback('default')
            if len(list) > 1:
                for l in list:
                    self.write([l])
        except TypeError:
            # this seems to happen when lp_values is empty
            # TODO: fix that
            transaction.rollback('default')
        except Exception as e:
            import traceback
            traceback.print_exc()
            print e
            print "UNHANDLED EXCEPTION"
            transaction.rollback()
            if len(list) > 1:
                for l in list:
                    self.write([l])
        transaction.rollback('default')