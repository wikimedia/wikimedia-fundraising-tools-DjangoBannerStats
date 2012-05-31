from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.utils import IntegrityError

from datetime import datetime
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

class Command(BaseCommand):

    logger = logging.getLogger("fundraiser.analytics.load_lps")

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

    squid_sql = "INSERT INTO `squidrecord` (squid_id, sequence, timestamp) VALUES %s"
    impression_sql = "INSERT INTO `landingpageimpression_raw` (timestamp, utm_source, utm_campaign, utm_key, utm_medium, landingpage, project_id, language_id, country_id) VALUES %s"

    pending_squids = []
    pending_impressions = []

    debug_info = []

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
                self.logger.info("Processing directory")
                for f in os.listdir(filename):

                    subfile = os.path.join(filename,f)

                    if not os.path.isdir(subfile):
                        existing = SquidLog.objects.filter(filename=subfile)
                        if existing:
                            self.logger.debug("Already processed %d  - skipping" % subfile)
                            continue

                        results = self.process_file(subfile)

                        path, filename_only = subfile.rsplit('/', 1)
                        sq = SquidLog(filename=filename_only, impressiontype="landingpage")
                        sq.timestamp = sq.filename2timestamp()
                        sq.save()

                        self.matched += results["squid"]["match"]
                        self.nomatched += results["squid"]["nomatch"]

#                        os.renames(subfile, os.path.join(filename, 'processed', f))

                        self.logger.info("DONE - %s" % subfile)
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
            else:
                existing = SquidLog.objects.filter(filename=filename)
                if existing:
                    self.logger.debug("Already processed %d  - skipping" % filename)
                    return

                results = self.process_file(filename)

                self.matched += results["squid"]["match"]
                self.nomatched += results["squid"]["nomatch"]

                path, filename_only = filename.rsplit('/', 1)
                sq = SquidLog(filename=filename_only, impressiontype="landingpage")
                sq.timestamp = sq.filename2timestamp()
                sq.save()

#                os.renames(filename, os.path.join(filename, 'processed', f))

                self.logger.info("DONE - %s" % filename)
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


    def process_file(self, filename=None):
        if filename is None:
            self.logger.error("Error loading landing page impressions - No file specified")
            return

        if not os.path.exists(filename):
            self.logger.error("Error loading landing page impressions - File %s does not exist" % filename)
            return

        self.logger.info("Processing %s" % filename)

        results = {
            "squid" : {
                "match" : 0,
                "nomatch" : 0
            },
            "impression" : {
                "match" : 0,
                "nomatch" : 0,
                "ignored" : 0,
                "error" : 0
            }
        }

        batch_size = 1500

        file = gzip.open(filename, 'rb')
        try:
#        with gzip.open(filename, 'rb') as file: # incompatible with python2.6
            for i, l in enumerate(file):
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

                        record = False

                        url_uni = unquote(m.group("url"))
                        while unquote(url_uni) != url_uni:
                            url_uni = unquote(url_uni)

                        url_uni = unicode(url_uni, 'latin_1').encode('utf-8')

                        # check the landing page patterns
                        for r in landingpages:
                            record = r.match(url_uni)
                            if record:
                                results["impression"]["match"] += 1
                                break

                        if not record:
                            results["impression"]["nomatch"] += 1
                            continue

                        # go ahead and parse the URL
                        url = urlparse.urlparse(url_uni)
                        qs = urlparse.parse_qs(url.query, keep_blank_values=True)

                        # grab the tracking information that should be common to any LP
                        utm_source = ""
                        utm_campaign = ""
                        utm_medium = ""
                        utm_key = ""

                        if "utm_source" in qs:
                            utm_source = qs["utm_source"][0]
                        if "utm_campaign" in qs:
                            utm_campaign = qs["utm_campaign"][0]
                        if "utm_medium" in qs:
                            utm_medium = qs["utm_medium"][0]
                        if "utm_key" in qs:
                            utm_key = qs["utm_key"][0]

                        landingpage = ""
                        language = None
                        country = None
                        project = None

                        self.debug_info = []

                        if record.group("sitename") == "wikimediafoundation.org":
                            project = lookup_project("foundationwiki")

                            self.debug_info.append(record.group("landingpage"))
                            self.debug_info.append(unquote(record.group("landingpage")))

                            split = record.group("landingpage").rsplit('/', 2)
                            lang, coun = ('','')

                            self.debug_info.append(split)

                            if len(split) == 3:
                                landingpage, lang, coun = split
                            elif len(split) == 2:
                                landingpage, lang = split
                                coun = 'XX'
                            elif len(split) == 1:
                                landingpage = split[0]
                                coun = 'XX'
                                lang = 'en'
                            else:
                                # uh oh, TODO: do something informative
                                pass


                            self.debug_info.append("LP: %s" % landingpage)

                            # deal with the payment-processing chapters and their pages
                            if lang in ('CH','DE','GB','FR'):
                                # the language and country are backwards
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
                            results["impression"]["error"] += 1
                            self.logger.info("*** INVALID DOMAIN FOR LANDING PAGE IMPRESSION ***")
                            self.logger.info("--- File: %s | Line: %d ---" % (filename, i+1))
                            self.logger.info(m.group("url")[:200])
                            if len(m.group("url")) > 200:
                                self.logger.info("...TRUNCATED...")
                            self.logger.info("*** END ***")
                            continue

                        if landingpage is "" or language is None or country is None or project is None:
                            # something odd does not quite match in this request
                            results["impression"]["error"] += 1
                            self.logger.info("*** NOT ALL VARIABLES CAPTURED FOR LANDING PAGE IMPRESSION ***")
                            self.logger.info("--- File: %s | Line: %d ---" % (filename, i+1))
                            self.logger.info(m.group("url")[:200])
                            if len(m.group("url")) > 200:
                                self.logger.info("...TRUNCATED...")
                            self.logger.info("*** END ***")
                            continue

                        # Truncate the landing page name if it longer than supported by the database
                        # Don't lookup the attribute the vase majority of the time
                        if len(landingpage) > 50:
                            lp_max = LandingPageImpression._meta.get_field('landing_page').max_length
                            if len(landingpage) > lp_max:
                                landingpage = landingpage[:lp_max]

                        squid = lookup_squidhost(hostname=m.group("squid"), verbose=self.verbose)
                        seq = int(m.group("sequence"))
                        timestamp = datetime.strptime(m.group("timestamp"), "%Y-%m-%dT%H:%M:%S.%f")

                        # not using the models here saves a lot of wall time
                        try:
                            sq_tmp = "(%s, %s, '%s')" % (
                                MySQLdb.escape_string(str(squid.id)),
                                MySQLdb.escape_string(str(seq)),
                                MySQLdb.escape_string(str(timestamp.strftime("%Y-%m-%d %H:%M:%S")))
                            )
                            lp_tmp = "('%s', '%s', '%s', '%s', '%s', '%s', %s, %s, %s)" % (
                                MySQLdb.escape_string(str(timestamp.strftime("%Y-%m-%d %H:%M:%S"))),
                                MySQLdb.escape_string(utm_source),
                                MySQLdb.escape_string(utm_campaign),
                                MySQLdb.escape_string(utm_key),
                                MySQLdb.escape_string(utm_medium),
                                MySQLdb.escape_string(landingpage),
                                MySQLdb.escape_string(str(project.id)),
                                MySQLdb.escape_string(str(language.id)),
                                MySQLdb.escape_string(str(country.id))
                            )
                            self.pending_squids.append(sq_tmp)
                            self.pending_impressions.append(lp_tmp)

                        except Exception:
                            results["impression"]["error"] += 1
                            self.logger.exception("** UNHANDLED EXCEPTION WHILE PROCESSING LANDING PAGE IMPRESSION **")
                            self.logger.error("********************\n%s\n********************" % l)

                        finally:
                            sq_tmp = ""
                            lp_tmp = ""

                        # write the models in batch
                        if len(self.pending_squids) % batch_size == 0:
                            try:
                                self.write(self.pending_squids, self.pending_impressions)
                            except Exception:
                                self.logger.exception("Error writing impressions to the database")
                            finally:
                                self.pending_squids = []
                                self.pending_impressions = []

                except Exception as e:
                    results["impression"]["error"] += 1
                    self.logger.exception("** UNHANDLED EXCEPTION WHILE PROCESSING LANDING PAGE IMPRESSION **")
                    self.logger.error("********************\n%s\n********************" % l)

            try:
                # write out any remaining records
                self.write(self.pending_squids, self.pending_impressions)
                self.pending_squids = []
                self.pending_impressions = []

            except Exception as e:
                self.logger.exception("** UNHANDLED EXCEPTION WHILE PROCESSING LANDING PAGE IMPRESSION **")
                self.logger.error("********************")

        except IOError:
            pass
        finally:
            file.close()

        return results

    @transaction.commit_manually
    def write(self, squids, impressions):
        """
        Commits a batch of transactions. Attempts a single query per model by splitting the
        tuples of each banner impression and grouping by model.  If that fails, the function
        falls back to a single transaction per banner impression
        """
        cursor = connections['default'].cursor()

        s_len = len(squids)
        i_len = len(impressions)

        if s_len != i_len:
            raise Exception("Length mismatch between squid records and landing page impressions")

        if not s_len:
            return

        try:
            # attempt to create all in batches
            cursor.execute(self.squid_sql % ', '.join(squids))
            cursor.execute(self.impression_sql % ', '.join(impressions))

            transaction.commit('default')

        except IntegrityError as e:
            # someone was not happy, likely a SquidRecord
            transaction.rollback('default')

            if s_len == 1 or i_len == 1:
                return

            for i in range(s_len):
                self.write([squids[i]], [impressions[i]])

        except Exception as e:
            transaction.rollback()

            self.logger.exception("UNHANDLED EXCEPTION")

            if self.debug:
                self.logger.info(self.squid_sql % ', '.join(squids))
                self.logger.info(self.impression_sql % ', '.join(impressions))

                for r in self.debug_info:
                    self.logger.info("\t%s" % r)