from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from datetime import datetime
from optparse import make_option
import os
import urlparse
import re
from django.db.utils import IntegrityError

from analytics.cache import cache
from analytics.functions import *
from analytics.models import *
from analytics.regex import *

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

            if os.path.isdir(filename):
                print "Processing directory"
                for f in os.listdir(filename):

                    subfile = os.path.join(filename,f)

                    if not os.path.isdir(subfile):
                        m, n = self.process_file(subfile)
                        self.matched += m
                        self.nomatched += n

#                        if n == 0:
#                            os.renames(subfile, os.path.join(filename, 'processed', f))

                        if n == -1:
                            break

                        print "DONE - %d OKAY / %d FAILED - %s" % (m, n, subfile)
            else:
                m, n = self.process_file(filename)

                self.matched += m
                self.nomatched += n

#                if n == 0:
#                    os.renames(subfile, os.path.join(filename, 'processed', f))

                print "DONE - %d OKAY / %d FAILED - %s" % (m, n, filename)

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
            print "Error loading banner impressions - No file specified"
            return

        if not os.path.exists(filename):
            print "Error loading banner impressions - File %s does not exist" % filename
            return

        print "Processing %s" % filename

        matched = 0
        nomatched = 0

        batch_size = 10
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
                        if notmatched < 100:
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

                    batch_models.append((
                        SquidRecord(squid=squid, sequence=seq, timestamp=timestamp),
                        BannerImpression(timestamp=timestamp, banner=banner,campaign=campaign,
                             project=project, language=language, country=country))
                    )

                    if len(batch_models) > 0 and len(batch_models) % batch_size == 0:
                        self.write(batch_models)



        return matched, nomatched

    @transaction.commit_manually
    def write(self, list):
        squids = []
        banners = []

        if len(list) > 1:
            for l in list:
                s, b = l
                squids.append(s)
                banners.append(b)
            try:
                SquidRecord.objects.batch_create(squids)
            except IntegrityError:
                transaction.rollback()

        elif len(list) == 1:
            try:
                for m in models[0]:
                    m.save()
                transaction.commit()
            except IntegrityError:
                transaction.rollback()
        elif len(list) == 0:
            return



        pass

    def process_line(self, values):
        "Processes a matched squid line and inserts records into the database"


