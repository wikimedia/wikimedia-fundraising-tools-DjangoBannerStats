from django.core.management.base import BaseCommand
from django.db import connections, transaction, reset_queries
from django.db.utils import IntegrityError

from pympler import muppy

import gc
from datetime import datetime, timedelta
import glob
import gzip
import logging
import MySQLdb
from optparse import make_option
import os
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
        make_option('', '--top',
            dest='top',
            action='store_true',
            default=False,
            help='Only separate out top languages and projects'),
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

    debug_info = []
    debug_count = 0

    detail_languages = [
        "en", "fr", "it", "ja", "nl", "es", "ru", "hi",
        "de", "pt", "sv", "no", "he", "da", "zh", "fi",
        "pl", "cs", "ar", "el", "ko", "tr"
    ]

    def handle(self, *args, **options):
#        gc.set_debug(gc.DEBUG_LEAK)

        try:
            starttime = datetime.now()
            filename = options.get('filename')
            self.debug = options.get('debug')
            self.verbose = options.get('verbose')
            self.top = options.get('top')
            self.recent = options.get('recent')

            self.matched = 0
            self.nomatched = 0
            self.ignored = 0

            files = []
            if self.recent:
                now = "bannerImpressions-sampled100.tsv-%s*" % datetime.now().strftime("%Y%m%d-%H")
                pasthour = "bannerImpressions-sampled100.tsv-%s*" % (datetime.now() - timedelta(hours=1)).strftime("%Y%m%d-%H")

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

            for f in sorted(files):
                path, filename_only = f.rsplit('/', 1)
                if not os.path.isdir(f):
                    existing = SquidLog.objects.filter(filename=filename_only)
                    if existing:
                        self.logger.debug("Already processed %s  - skipping" % f)
                        continue

                    sq = SquidLog(filename=filename_only, impressiontype="banner")
                    sq.timestamp = sq.filename2timestamp()

                    if sq.timestamp > datetime(2012, 10, 1):
                        self.recent = True

                    results = self.process_file(f)

                    if not self.debug:
                        sq.save()

                    self.matched += results["squid"]["match"]
                    self.nomatched += results["squid"]["nomatch"]

                    self.logger.info("DONE - %s" % f)
                    self.logger.info("\tSQUID: %d OKAY / %d FAILED with %d IGNORED and ..." % (
                        int(results["squid"]["match"]),
                        int(results["squid"]["nomatch"]),
                        int(results["squid"]["ignored"]),
                        int(results["squid"]["ignored"])
                        ))
                    for code in results['squid']['codes']:
                        self.logger.info("\t\tIGNORED CACHE RESPONSE CODE %d: %d" % (
                            int(code),
                            results['squid']['codes'][code]
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
                "codes": {
                    302: 0,
                    404: 0,
                }
            },
            "impression" : {
                "match" : 0,
                "nomatch" : 0,
                "ignored" : 0,
                "error" : 0
            }
        }

        sample_rate = 1

        counts = dict()

        path, filename_only = filename.rsplit('/', 1)
        if sampled.match(filename_only):
            sample_rate = int(sampled.match(filename_only).group("samplerate"))

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
                                self.logger.info("*** NO MATCH FOR BANNER IMPRESSION ***")
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

                        # Ignore everything but status 200
                        squidstatus = m.group("squidstatus")[-3:]
                        if squidstatus != 200:
                            if squidstatus not in results['squid']['codes']:
                                results['squid']['codes'][squidstatus] = 0
                            results['squid']['codes'][squidstatus] += 1

                        if self.recent:
                            # yeah, ignore this too
                            if "Special:BannerRandom" in m.group("url"):
                                results["impression"]["ignored"] += 1
                                continue

                            # Also ignore anything coming from Aluminium or Grosley
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
                            if phantomJS.search(m.group("useragent")):
                                results["impression"]["ignored"] += 1
                                continue

                        try:
                            timestamp = datetime.strptime(m.group("timestamp"), "%Y-%m-%dT%H:%M:%S.%f")
                        except ValueError:
                            timestamp = datetime.strptime(m.group("timestamp"), "%Y-%m-%dT%H:%M:%S")
                        url = urlparse.urlparse(m.group("url"))
                        qs = urlparse.parse_qs(url.query, keep_blank_values=True)

                        country = qs["country"][0] if "country" in qs else "XX"
                        language = qs["userlang"][0] if "userlang" in qs else "en"
                        language = qs["uselang"][0] if "uselang" in qs else language

                        banner = ""
                        if "banner" in qs:
                            banner = qs["banner"][0].replace("%", "%%")
                        campaign = ""
                        if "campaign" in qs:
                            campaign = qs["campaign"][0].replace("%", "%%")
                        project = None
                        if "db" in qs:
                            project = lookup_project(qs["db"][0])
                            if self.top and not qs["db"][-4:] == "wiki":
                                project = lookup_project("other_project")

                        if self.top:
                            if language in self.detail_languages:
                                language = lookup_language(language)
                            else:
                                language = lookup_language("other")
                        else:
                            language = lookup_language(language)

                        country = lookup_country(country)

                        if banner == "" or campaign == "" or project is None:
                            if "BannerController" in l:
                                # we really don't care about these, so there is no need to log them as errors
                                results["impression"]["ignored"] += 1
                                continue
                            if "result" in qs and qs["result"][0] == "hide":
                                continue
                            results["impression"]["error"] += 1
                            if self.verbose:
                                self.logger.exception("** INVALID BANNER IMPRESSION - NOT ENOUGH DATA TO RECORD **")
                                self.logger.error("********************\n%s\n********************" % l.strip())
                            continue

                        # not using the models here saves a lot of wall time
                        try:
                            k = "'%s', '%s', '%s', %d, %d, %d" % (
                                roundtime(timestamp, 1, False).strftime("%Y-%m-%d %H:%M:%S"),
                                MySQLdb.escape_string(banner),
                                MySQLdb.escape_string(campaign),
                                project.id,
                                language.id,
                                country.id,
                            )
                            if k in counts:
                                counts[k] += sample_rate
                            else:
                                counts[k] = sample_rate

                            results["impression"]["match"] += 1

                        except Exception:
                            results["impression"]["error"] += 1
                            self.logger.exception("** UNHANDLED EXCEPTION WHILE PROCESSING BANNER IMPRESSION **")
                            self.logger.error("********************\n%s\n********************" % l.strip())

                        finally:
                            del m, url, qs, k
                            del timestamp, banner, campaign, project, language, country

                except Exception:
                    results["impression"]["error"] += 1
                    self.logger.exception("** UNHANDLED EXCEPTION WHILE PROCESSING BANNER IMPRESSION **")
                    self.logger.error("********************\n%s\n********************" % l.strip())

                finally:
                    del l

            try:
                if not self.debug:
                    self.write(counts)

            except Exception as e:
                self.logger.exception("** UNHANDLED EXCEPTION WHILE PROCESSING LANDING PAGE IMPRESSION **")
                self.logger.error("********************")

        except IOError:
            pass
        finally:
            file.close()

        del counts, file

        gc.collect()
#        muppy.print_summary()

        reset_queries()

        return results

    @transaction.commit_manually
    def write(self, impressions):

        insert_sql = "INSERT INTO bannerimpressions (timestamp, banner, campaign, project_id, language_id, country_id, count) VALUES (%s) ON DUPLICATE KEY update count=count+%d"

        cursor = connections['default'].cursor()

        if not len(impressions):
            return

        try:
            for k,c in impressions.iteritems():
                try:
                    cursor.execute(insert_sql % (
                        "%s, %d" % (k, c), c
                        ))
                except (MySQLdb.Warning, _mysql_exceptions.Warning) as e:
                    pass # We don't care about the message
                transaction.commit('default')


        except Exception as e:
            import sys
            transaction.rollback("default")
            self.logger.exception("UNHANDLED EXCEPTION: %s" % str(e))
            self.logger.exception(sys.exc_info()[0])
            if self.debug:
                if len(impressions) == 1:
                    self.logger.info(impressions)

                for r in self.debug_info:
                    self.logger.info("\t%s" % r)
        finally:
            reset_queries()
            del impressions
            del cursor
