"""Parses the specified squid log file and stores the impression in the database."""
import glob
import gzip
import logging
import os
import sys
import urllib.parse
from datetime import datetime, timedelta
from dateutil.parser import parse as dateparse
import MySQLdb

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections, transaction, reset_queries

from fundraiser.analytics.functions import lookup_country, lookup_project, \
    lookup_language, roundtime
from fundraiser.analytics.models import SquidLog
from fundraiser.analytics.regex import ignore_uas, phantomJS, sampled, squidline


class Command(BaseCommand):

    logger = logging.getLogger("fundraiser.analytics.load_banners")

    help = 'Parses the specified squid log file and stores the impression in the database.'

    debug_info = []
    debug_count = 0

    detail_languages = [
        "en", "fr", "it", "ja", "nl", "es", "ru", "hi",
        "de", "pt", "sv", "nb", "he", "da", "zh", "fi",
        "pl", "cs", "ar", "el", "ko", "tr", "ms", "uk"
    ]

    def add_arguments(self, parser):
        parser.add_argument(
            '-f',
            '--file',
            dest='filename',
            default=None,
            help='Specify the input file')
        parser.add_argument(
            '--verbose',
            dest='verbose',
            action='store_true',
            default=False,
            help='Provides more verbose output.')
        parser.add_argument(
            '--top',
            dest='top',
            action='store_true',
            default=False,
            help='Only separate out top languages and projects')
        parser.add_argument(
            '--debug',
            dest='debug',
            action='store_true',
            default=False,
            help='Do not save the impressions. Parse only.')
        parser.add_argument(
            '--recent',
            dest='recent',
            action='store_true',
            default=False,
            help='Process recent logs.')

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
                time_now = datetime.now()
                time_minus1hr = time_now - timedelta(hours=1)

                now = "beaconImpressions-sampled*.tsv[.-]%s*" % \
                    time_now.strftime("%Y%m%d-%H")
                pasthour = "beaconImpressions-sampled*.tsv[.-]%s*" % \
                    time_minus1hr.strftime("%Y%m%d-%H")

                files.extend(glob.glob(os.path.join(
                    settings.UDP_LOG_PATH,
                    time_now.strftime("%Y"),
                    now
                )))
                files.extend(glob.glob(os.path.join(
                    settings.UDP_LOG_PATH,
                    time_minus1hr.strftime("%Y"),
                    pasthour
                )))
            else:
                if os.path.isdir(filename):
                    self.logger.info("Processing directory")
                    filename = filename.rstrip('/')
                    files = glob.glob("%s/*.gz" % filename)
                else:
                    self.logger.info("Processing files matching %s", filename)
                    files = glob.glob(filename)

            for log_file in sorted(files):
                filename_only = log_file.rsplit('/', 1)[-1]
                if not os.path.isdir(log_file):
                    existing = SquidLog.objects.filter(filename=filename_only)
                    if existing:
                        self.logger.debug("Already processed %s  - skipping", log_file)
                        continue

                    squid_log = SquidLog(filename=filename_only, impressiontype="banner")
                    squid_log.timestamp = squid_log.filename2timestamp()

                    if squid_log.timestamp > datetime(2012, 10, 1):
                        self.recent = True

                    results = self.process_file(log_file)

                    if not self.debug:
                        squid_log.save()

                    self.matched += results["squid"]["match"]
                    self.nomatched += results["squid"]["nomatch"]

                    self.logger.info("DONE - %s", log_file)
                    self.logger.info(
                        "\tSQUID: %d OKAY / %d FAILED with %d IGNORED and ...",
                        int(results["squid"]["match"]),
                        int(results["squid"]["nomatch"]),
                        int(results["squid"]["ignored"])
                    )
                    for code in results['squid']['codes']:
                        self.logger.info(
                            "\t\tIGNORED CACHE RESPONSE CODE %d: %d",
                            int(code),
                            results['squid']['codes'][code]
                        )
                    self.logger.info(
                        "\tIMPRESSIONS: %d MATCHED / %d NOMATCH with %d IGNORED / %d ERROR",
                        results["impression"]["match"],
                        results["impression"]["nomatch"],
                        results["impression"]["ignored"],
                        results["impression"]["error"]
                    )
                    for reason in results['impression']['ignore_because']:
                        self.logger.info(
                            "\t\tIGNORED IMPRESSION BECAUSE %s: %d",
                            reason,
                            results['impression']['ignore_because'][reason]
                        )

            endtime = datetime.now()
            self.logger.info("DONE")
            self.logger.info("Total squid matched: %d", self.matched)
            self.logger.info("Total squid not matched: %d", self.nomatched)
            self.logger.info(
                "Finished in %d.%d seconds",
                (endtime - starttime).seconds, (endtime - starttime).microseconds
            )

        except Exception:
            self.logger.exception("Error processing files")

    def process_file(self, filename=None):
        if filename is None:
            self.logger.error("Error loading banner impressions - No file specified")
            return

        if not os.path.exists(filename):
            self.logger.error("Error loading banner impressions - File %s does not exist", filename)
            return

        self.logger.error("Processing %s", filename)

        results = {
            "squid": {
                "match": 0,
                "nomatch": 0,
                "ignored": 0,
                "codes": {
                    302: 0,
                    404: 0,
                }
            },
            "impression": {
                "match": 0,
                "nomatch": 0,
                "ignored": 0,
                "error": 0,
                "ignore_because": {

                    # Coordinate with window.insertBanner() and window.hideBanner() in CentralNotice
                    # modules/ext.centralNotice.bannerController/bannerController.js,
                    # https://www.mediawiki.org/wiki/Extension:CentralNotice/Special:RecordImpression,
                    # and reasons set up in this method.
                    # Note also that "hidecookie" and "hideempty" come from the values "cookie" and
                    # "empty" respectively.
                    "file": 0,
                    "client": 0,
                    "hidecookie": 0,
                    "hideempty": 0,
                    "preload": 0,
                    "alterImpressionData": 0,
                    "close": 0,
                    "donate": 0,
                    "other": 0
                }
            }
        }

        sample_rate = 1

        counts = dict()

        filename_only = filename.rsplit('/', 1)[-1]
        if sampled.match(filename_only):
            sample_rate = int(sampled.match(filename_only).group("samplerate"))

        file = gzip.open(filename, 'rb')
        try:
            i = 0

            for log_line in file:

                i += 1
                try:
                    match = squidline.match(log_line.decode("latin_1"))
                    if not match:
                        # TODO: do we want to write the failed lines to another
                        # file for reprocessing?
                        results["squid"]["nomatch"] += 1
                        if self.verbose:
                            if results["squid"]["nomatch"] < 100:
                                self.logger.info("*** NO MATCH FOR BANNER IMPRESSION ***")
                                self.logger.info("--- File: %s | Line: %d ---", filename, i + 1)
                                self.logger.info(log_line[:500])
                                if len(log_line) > 500:
                                    self.logger.info("...TRUNCATED...")
                                self.logger.info("*** END OF NO MATCH ***")
                    else:
                        results["squid"]["match"] += 1

                        # Go ahead and ignore SSL termination logs since they are missing GET params
                        # and are followed by a proper squid log for the request
                        if match.group("squid")[:3] == "ssl":
                            results["squid"]["ignored"] += 1
                            continue

                        # Ignore everything but status 200
                        squidstatus = int(match.group("squidstatus")[-3:])
                        if squidstatus not in (0, 200, 204, 206, 304):
                            results["squid"]["ignored"] += 1
                            if squidstatus not in results['squid']['codes']:
                                results['squid']['codes'][squidstatus] = 0
                            results['squid']['codes'][squidstatus] += 1
                            continue

                        if self.recent:
                            # yeah, ignore this too
                            if "Special:BannerRandom" in match.group("url"):
                                results["impression"]["ignored"] += 1
                                results["impression"]["ignore_because"]["file"] += 1
                                continue

                            # And ignore all of our testing UA's
                            for user_agent in ignore_uas:
                                if user_agent.match(match.group("useragent")):
                                    results["impression"]["ignored"] += 1
                                    results["impression"]["ignore_because"]["client"] += 1
                                    continue
                            if phantomJS.search(match.group("useragent")):
                                results["impression"]["ignored"] += 1
                                results["impression"]["ignore_because"]["client"] += 1
                                continue

                        timestamp = dateparse(match.group("timestamp"))
                        url = urllib.parse.urlparse(match.group("url"))
                        query_string = urllib.parse.parse_qs(url.query, keep_blank_values=True)

                        country_string = query_string["country"][0] if "country" in query_string else "XX"
                        country = lookup_country(country_string, self.logger)
                        country_id = country.id if country else None

                        language_string = "en"
                        if "uselang" in query_string:
                            language_string = query_string["uselang"][0]
                        elif "userlang" in query_string:
                            language_string = query_string["userlang"][0]
                        elif "language" in query_string:
                            language_string = query_string["language"][0]

                        language = None
                        if self.top:
                            if language_string in self.detail_languages:
                                language = lookup_language(language_string, self.logger)
                            else:
                                language = lookup_language("other", self.logger)
                        else:
                            language = lookup_language(language_string, self.logger)

                        banner = ""
                        if "banner" in query_string:
                            banner = query_string["banner"][0].replace("%", "%%")

                        campaign = ""
                        if "campaign" in query_string:
                            campaign = query_string["campaign"][0].replace("%", "%%")

                        project = None
                        if "db" in query_string:
                            project = lookup_project(query_string["db"][0], self.logger)
                            if self.top and not query_string["db"][-4:] == "wiki":
                                project = lookup_project("other_project", self.logger)
                        else:
                            project = lookup_project("", self.logger)

                        if "result" in query_string and query_string["result"][0] == "hide":
                            results["impression"]["ignored"] += 1

                            if "reason" in query_string:

                                # Switch "cookie" to "hidecookie" and "empty" to "hideempty"
                                # for consistency with legacy reasons in the database
                                reason = query_string["reason"][0]
                                if reason == "cookie":
                                    reason = "hidecookie"
                                if reason == "empty":
                                    reason = "hideempty"

                                if reason in results["impression"]["ignore_because"]:
                                    results["impression"]["ignore_because"][reason] += 1
                                else:
                                    results["impression"]["ignore_because"]["other"] += 1

                                continue

                                # ^^^ fixed (?) this which was mis-indented,
                                # rendering the next block unreachable

                            results["impression"]["error"] += 1
                            if self.verbose:
                                self.logger.exception(
                                    "** INVALID BANNER IMPRESSION - NOT ENOUGH DATA TO RECORD **"
                                )
                                self.logger.error(
                                    "********************\n%s\n********************",
                                    log_line.strip()
                                )
                            continue

                        # not using the models here saves a lot of wall time
                        try:
                            key = tuple((
                                roundtime(timestamp, 1, False).strftime("%Y-%m-%d %H:%M:%S"),
                                banner,
                                campaign,
                                project.id,
                                language.id,
                                country_id
                            ))
                            if key in counts:
                                counts[key] += sample_rate
                            else:
                                counts[key] = sample_rate

                            results["impression"]["match"] += 1
                        except Exception:
                            results["impression"]["error"] += 1
                            self.logger.exception(
                                "** UNHANDLED EXCEPTION WHILE PROCESSING BANNER IMPRESSION **"
                            )
                            self.logger.error(
                                "********************\n%s\n********************",
                                log_line.strip()
                            )

                except Exception:
                    results["impression"]["error"] += 1
                    self.logger.exception(
                        "** UNHANDLED EXCEPTION WHILE PROCESSING BANNER IMPRESSION **"
                    )
                    self.logger.error(
                        "********************\n%s\n********************",
                        log_line.strip()
                    )

            try:
                if not self.debug:
                    self.write(counts)

            except Exception:
                self.logger.exception(
                    "** UNHANDLED EXCEPTION WHILE PROCESSING LANDING PAGE IMPRESSION **"
                )
                self.logger.error("********************")

        except IOError:
            pass
        finally:
            file.close()

        reset_queries()

        return results

    def write(self, impressions):
        insert_sql = """INSERT INTO bannerimpressions (timestamp, banner, campaign,
            project_id, language_id, country_id, count)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY update count=count+(%s)"""

        if not impressions:
            return

        transaction.set_autocommit(False)
        cursor = connections['default'].cursor()

        try:
            for key, count in impressions.items():
                try:
                    cursor.execute(insert_sql, list(key) + [count, count])
                except MySQLdb._exceptions.Warning:
                    pass  # We don't care about the message
                transaction.commit('default')

        except Exception as exception:
            transaction.rollback("default")
            self.logger.exception("UNHANDLED EXCEPTION: %s", str(exception))
            self.logger.exception(sys.exc_info()[0])
            if self.debug:
                if len(impressions) == 1:
                    self.logger.info(impressions)

                for r in self.debug_info:
                    self.logger.info("\t%s", r)
        finally:
            reset_queries()
            transaction.set_autocommit(True)
