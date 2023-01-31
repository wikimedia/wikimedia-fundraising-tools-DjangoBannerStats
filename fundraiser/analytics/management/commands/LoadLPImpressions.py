"""Parses the specified squid log file and stores the impression in the database."""
import gc
from datetime import datetime, timedelta
import glob
import gzip
import logging
import os
from urllib.parse import unquote
import urllib.parse
import MySQLdb
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections, transaction
import django.db.utils
from dateutil.parser import parse as dateparse
from fundraiser.analytics.functions \
    import lookup_country, lookup_language, lookup_project, lookup_squidhost
from fundraiser.analytics.models import LandingPageImpression, SquidLog
from fundraiser.analytics.regex import ignore_uas, landingpages, squidline


class Command(BaseCommand):

    logger = logging.getLogger("fundraiser.analytics.load_lps")

    help = "Parses the specified squid log file and stores the impression in the database."

    impression_sql = """REPLACE INTO `landingpageimpression_raw%s` (timestamp, squid_id,
         squid_sequence, utm_source, utm_campaign, utm_key, utm_medium, landingpage,
         project_id, language_id, country_id) VALUES (%s)"""

    # Using ON DUPLICATE KEY UPDATE with a deliberate no-op rather than INSERT
    # IGNORE, as the latter raises warnings
    unique_sql = """INSERT INTO `donatewiki_unique` (timestamp, utm_source, utm_campaign,
        contact_id, link_id) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE link_id=link_id"""

    pending_impressions = []
    pending_uniques = []

    debug_info = []
    debug_count = 0

    counts = {
        "countries": {},
        "languages": {},
    }

    def add_arguments(self, parser):
        parser.add_argument(
            '-f',
            '--file',
            dest='filename',
            default=None,
            help='Specify the input file(s)')
        parser.add_argument(
            '--verbose',
            dest='verbose',
            action='store_true',
            default=False,
            help='Provides more verbose output.')
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
        parser.add_argument(
            '--alt',
            dest='alt',
            action='store_true',
            default=False,
            help="Save to alternate tables.  Allows for reprocessing and then a table rename."
            "NOTE: This requires the associated SquidLog records to be removed.")

    def handle(self, *args, **options):
        try:
            starttime = datetime.now()
            filename = options.get('filename')
            self.debug = options.get('debug')
            self.verbose = options.get('verbose')
            self.recent = options.get('recent')
            self.alt = options.get('alt')

            self.matched = 0
            self.nomatched = 0
            self.ignored = 0

            if self.alt:
                self.impression_sql = self.impression_sql % ("_alt", "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")
            else:
                self.impression_sql = self.impression_sql % ("", "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")

            files = []
            if self.recent:
                self.logger.info("Processing recent files")
                time_now = datetime.now()
                time_minus1hr = time_now - timedelta(hours=1)

                now = "landingpages.tsv[.-]%s*" % time_now.strftime("%Y%m%d-%H")
                pasthour = "landingpages.tsv[.-]%s*" % time_minus1hr.strftime("%Y%m%d-%H")
                now_glob = os.path.join(settings.UDP_LOG_PATH, time_now.strftime("%Y"), now)
                pasthour_glob = os.path.join(settings.UDP_LOG_PATH, time_minus1hr.strftime("%Y"), pasthour)

                self.logger.info("Checking for files matching '%s' or '%s'", now_glob, pasthour_glob)

                files.extend(glob.glob(now_glob))
                files.extend(glob.glob(pasthour_glob))
            else:
                if os.path.isdir(filename):
                    self.logger.info("Processing directory")
                    filename = filename.rstrip('/')
                    files = glob.glob("%s/landingpages*.gz" % filename)
                else:
                    self.logger.info("Processing files matching %s", filename)
                    files = glob.glob(filename)

            self.logger.info("Examining %s files", len(files))

            for f in files:
                _, filename_only = f.rsplit('/', 1)
                if not os.path.isdir(f):
                    existing = SquidLog.objects.filter(filename=filename_only, impressiontype="landingpage")
                    if existing:
                        self.logger.debug("Already processed %s  - skipping", f)
                        continue

                    results = self.process_file(f)

                    sq = SquidLog(filename=filename_only, impressiontype="landingpage")
                    sq.timestamp = sq.filename2timestamp()
                    if not self.debug:
                        sq.save()

                    self.matched += results["squid"]["match"]
                    self.nomatched += results["squid"]["nomatch"]

                    self.logger.info("DONE - %s", f)
                    self.logger.info(
                        "\tSQUID: %d OKAY / %d FAILED with %d IGNORED and %d 404s",
                        results["squid"]["match"],
                        results["squid"]["nomatch"],
                        results["squid"]["ignored"],
                        results["squid"]["404"]
                    )
                    self.logger.info(
                        "\tIMPRESSIONS: %d MATCHED / %d NOMATCH with %d IGNORED / %d ERROR",
                        results["impression"]["match"],
                        results["impression"]["nomatch"],
                        results["impression"]["ignored"],
                        results["impression"]["error"],
                    )

            endtime = datetime.now()
            self.logger.info("DONE")
            self.logger.info("Total squid matched: %d", self.matched)
            self.logger.info("Total squid not matched: %d", self.nomatched)
            self.logger.info("Finished in %d.%d seconds", (endtime - starttime).seconds, (endtime - starttime).microseconds)
        except Exception:
            self.logger.exception("Error processing files")

    def process_file(self, filename=None):
        if filename is None:
            self.logger.error("Error loading landing page impressions - No file specified")
            return

        if not os.path.exists(filename):
            self.logger.error("Error loading landing page impressions - File %s does not exist", filename)
            return

        self.logger.info("Processing %s", filename)

        results = {
            "squid": {
                "match": 0,
                "nomatch": 0,
                "ignored": 0,
                "404": 0,
            },
            "impression": {
                "match": 0,
                "nomatch": 0,
                "ignored": 0,
                "error": 0
            }
        }

        batch_size = 1500

        file = gzip.open(filename, 'rb')
        try:
            i = 0

            for line in file:
                i += 1
                try:

                    m = None
                    try:
                        m = squidline.match(line.decode("latin_1"))
                    except Exception as decode_error:
                        self.logger.info(decode_error)
                        self.logger.info(line)

                    if not m:
                        # TODO: do we want to write the failed lines to another file for reprocessing?
                        results["squid"]["nomatch"] += 1
                        if self.verbose:
                            if results["squid"]["nomatch"] < 100:
                                self.logger.info("*** NO MATCH FOR LANDING PAGE IMPRESSION ***")
                                self.logger.info("--- File: %s | Line: %d ---", filename, i + 1)
                                self.logger.info(line[:500].decode("utf-8"))
                                if len(line) > 500:
                                    self.logger.info("...TRUNCATED...")
                                self.logger.info("*** END OF NO MATCH ***")
                    else:
                        results["squid"]["match"] += 1

                        # Go ahead and ignore SSL termination logs since they are missing GET params
                        # and are followed by a proper squid log for the request
                        if m.group("squid")[:3] == "ssl":
                            results["squid"]["ignored"] += 1
                            continue

                        # And ignore all of our testing UA's
                        for ua in ignore_uas:
                            if ua.match(m.group("useragent")):
                                results["impression"]["ignored"] += 1
                                continue

                        record = False

                        url_uni = unquote(m.group("url"))
                        while unquote(url_uni) != url_uni:
                            url_uni = unquote(url_uni)

                        url_uni = url_uni.encode('utf-8')

                        # check the landing page patterns
                        for r in landingpages:
                            record = r.match(url_uni.decode('utf-8'))
                            if record:
                                results["impression"]["match"] += 1
                                break

                        if not record:
                            results["impression"]["nomatch"] += 1
                            continue

                        # go ahead and parse the URL
                        url = urllib.parse.urlparse(url_uni.decode("utf-8"))
                        qsi = urllib.parse.parse_qs(url.query, keep_blank_values=True)
                        qs = {}
                        # convert parameter names to lowercase
                        for p in qsi:
                            qs[p.lower()] = qsi[p]

                        # grab the tracking information that should be common to any LP
                        utm_source = ""
                        utm_campaign = ""
                        utm_medium = ""
                        utm_key = ""
                        contact_id = ""
                        link_id = ""

                        if "utm_source" in qs:
                            utm_source = qs["utm_source"][0].replace("%", "%%")
                        if "utm_campaign" in qs:
                            utm_campaign = qs["utm_campaign"][0].replace("%", "%%")
                        if "utm_medium" in qs:
                            utm_medium = qs["utm_medium"][0].replace("%", "%%")
                        if "utm_key" in qs:
                            utm_key = qs["utm_key"][0].replace("%", "%%")
                        if "contact_id" in qs:
                            contact_id = qs["contact_id"][0].replace("%", "%%")
                        if "link_id" in qs:
                            link_id = qs["link_id"][0].replace("%", "%%")

                        landingpage = ""
                        project = None

                        self.debug_info = []

                        if "country" in qs:
                            country = lookup_country(qs["country"][0], self.logger)
                        else:
                            country = lookup_country("XX", self.logger)

                        if "uselang" in qs:
                            language = lookup_language(qs["uselang"][0], self.logger)
                        elif "userlang" in qs:
                            language = lookup_language(qs["userlang"][0], self.logger)
                        elif "language" in qs:
                            language = lookup_language(qs["language"][0], self.logger)
                        else:
                            language = lookup_language("en", self.logger)

                        if "landingpage" in record.groupdict():
                            # TODO: this should reflect the source project not the LP wiki
                            project = lookup_project("foundationwiki", self.logger)
                            landingpage = record.group("landingpage").rsplit('/', 2)[0]

                        else:
                            # TODO: this should reflect the source project not the LP wiki
                            project = lookup_project("donatewiki", self.logger)

                            flp_vars = {
                                "template": qs["template"][0] if "template" in qs else "default",
                                "appeal": qs["appeal"][0] if "appeal" in qs else "default",
                                "appeal-template": qs["appeal-template"][0] if "appeal-template" in qs else "default",
                                "form-template": qs["form-template"][0] if "form-template" in qs else "default",
                                "form-countryspecific": qs["form-countryspecific"][0] if "form-countryspecific" in qs else "default",
                            }

                            # go ahead and remove the cruft from the lp variables
                            for k, v in flp_vars.items():
                                _, _, after = v.rpartition('-')
                                if after:
                                    flp_vars[k] = after

                            landingpage = '~'.join([
                                flp_vars["template"],
                                flp_vars["appeal-template"],
                                flp_vars["appeal"],
                                flp_vars["form-template"],
                                flp_vars["form-countryspecific"]
                            ])

                        if landingpage == "" or language is None or country is None or project is None:
                            # something odd does not quite match in this request
                            results["impression"]["error"] += 1
                            self.logger.info("*** NOT ALL VARIABLES CAPTURED FOR LANDING PAGE IMPRESSION ***")
                            self.logger.info("--- File: %s | Line: %d ---", filename, i + 1)
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

                        # truncate to db max lengths
                        utm_campaign = utm_campaign[:255]
                        utm_medium = utm_medium[:255]
                        utm_source = utm_source[:255]
                        utm_key = utm_key[:128]
                        contact_id = contact_id[:31]
                        link_id = link_id[:127]

                        squid = lookup_squidhost(hostname=m.group("squid"), logger=self.logger)

                        # Can't do int(m.group("sequence")) because its > 2^32
                        # and I don't want to run an alter
                        seq = 0

                        timestamp = dateparse(m.group("timestamp"))

                        # not using the models here saves a lot of wall time
                        self.pending_impressions.append([
                            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                            squid.id,
                            seq,
                            utm_source,
                            utm_campaign,
                            utm_key,
                            utm_medium,
                            landingpage,
                            project.id,
                            language.id,
                            country.id
                        ])

                        if url.hostname == 'donate.wikimedia.org' and contact_id != '':

                            self.pending_uniques.append([
                                str(timestamp.strftime("%Y-%m-%d %H:%M:%S")),
                                utm_source,
                                utm_campaign,
                                contact_id,
                                link_id,
                            ])

                        # write the models in batch
                        if len(self.pending_impressions) % batch_size == 0:
                            try:
                                if not self.debug:
                                    self.write(self.impression_sql, self.pending_impressions)
                            except Exception:
                                self.logger.exception("Error writing impressions to the database")
                            finally:
                                self.pending_impressions = []

                        if len(self.pending_uniques) % batch_size == 0:
                            try:
                                if not self.debug:
                                    self.write(self.unique_sql, self.pending_uniques)
                            except Exception:
                                self.logger.exception(
                                    "Error writing donatewiki uniques to the database"
                                )
                            finally:
                                self.pending_uniques = []

                except Exception:
                    results["impression"]["error"] += 1
                    self.logger.exception(
                        "** UNHANDLED EXCEPTION #1 WHILE PROCESSING LANDING PAGE IMPRESSION **"
                    )
                    self.logger.error("********************\n%s\n********************", line)

            try:
                # write out any remaining records
                if not self.debug:
                    self.write(self.impression_sql, self.pending_impressions)
                    self.write(self.unique_sql, self.pending_uniques)
                self.pending_impressions = []
                self.pending_uniques = []

            except Exception:
                self.logger.exception(
                    "** UNHANDLED EXCEPTION #2 WHILE PROCESSING LANDING PAGE IMPRESSION **"
                )
                self.logger.error("********************")

        except IOError:
            pass
        finally:
            file.close()

        gc.collect()

        return results

    def write(self, base_sql, impressions):
        """
        Commits a batch of transactions. Attempts a single query per model by splitting the
        tuples of each banner impression and grouping by model.  If that fails, the function
        falls back to a single transaction per banner impression
        """
        i_len = len(impressions)

        if not i_len:
            return

        transaction.set_autocommit(False)
        cursor = connections['default'].cursor()

        try:
            # attempt to create all in batches
            cursor.executemany(base_sql, impressions)
            transaction.commit('default')
        except (django.db.utils.IntegrityError, django.db.utils.OperationalError, MySQLdb._exceptions.OperationalError) as my_error:
            # some impression was not happy
            self.logger.info("ERROR %s", my_error)
            transaction.rollback('default')

            if i_len == 1:
                return

            for i in impressions:
                self.write(base_sql, [i])

        except Exception as exception:
            transaction.rollback()

            self.logger.exception("UNHANDLED EXCEPTION %s", exception)

            if self.debug:
                self.logger.info(self.impression_sql, ', '.join(impressions))

                for r in self.debug_info:
                    self.logger.info("\t%s", r)

            if i_len == 1:
                return

            for i in impressions:
                self.write(base_sql, [i])

        finally:
            transaction.set_autocommit(True)
