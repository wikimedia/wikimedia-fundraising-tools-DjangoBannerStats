from django.core.management.base import BaseCommand
from django.db import connections, transaction

from datetime import datetime
import logging
import MySQLdb
import _mysql_exceptions
from optparse import make_option

from fundraiser.analytics.functions import *
from fundraiser.analytics.models import *
from fundraiser.analytics.regex import *
from fundraiser.settings import UDP_LOG_PATH

class Command(BaseCommand):

    logger = logging.getLogger("fundraiser.analytics.load_banners")

    option_list = BaseCommand.option_list + (
        make_option('', '--verbose',
            dest='verbose',
            action='store_true',
            default=False,
            help='Provides more verbose output.'),
        make_option('', '--debug',
            dest='debug',
            action='store_true',
            default=False,
            help='Do not save. Parse only.'),
        make_option('', '--newest',
            dest='newest',
            action='store_true',
            default=False,
            help='Do not save. Parse only.'),
        make_option('', '--top',
            dest='top',
            action='store_true',
            default=False,
            help='Only separate out top languages and projects'),
        make_option('', '--batch',
            dest='batch',
            type='int',
            default=1000,
            help='Batch size to be used for query operations.'),
        make_option('', '--rounds',
            dest='rounds',
            type='int',
            default=1,
            help='Number of rounds of the batch size to be run.'),
        )

    help = ''

    select_sql = "SELECT id, timestamp, banner, campaign, project_id, language_id, country_id, sample_rate FROM bannerimpression_raw WHERE processed = 0 ORDER BY id %s LIMIT %d"

    insert_sql = "INSERT INTO bannerimpressions (timestamp, banner, campaign, project_id, language_id, country_id, count) VALUES (%s) ON DUPLICATE KEY update count=count+%d"

    update_sql = "UPDATE bannerimpression_raw SET processed = 1 WHERE id IN (%s)"

    detail_languages = [
        "en", "fr", "it", "ja", "nl", "es", "ru", "hi",
        "de", "pt", "sv", "no", "he", "da", "zh", "fi",
        "pl", "cs", "ar", "el", "ko", "tr"
    ]


    def handle(self, *args, **options):
        starttime = datetime.now()
        self.debug = options.get('debug')
        self.verbose = options.get('verbose')
        self.top = options.get('top')
        self.newest = options.get('newest')
        batch = options.get('batch')
        rounds = options.get('rounds')

        for r in range(rounds):
            self.run(batch)

        endtime = datetime.now()

        print "Aggregated %d rounds of %d each in %d.%d seconds" % (rounds, batch, (endtime - starttime).seconds, (endtime - starttime).microseconds)


    @transaction.commit_manually
    def run(self, batchSize=1000):
        if not isinstance(batchSize, int):
            raise TypeError("Invalid batch size %s" % batchSize)

        cursor = connections['default'].cursor()

        try:
            if self.newest:
                num = cursor.execute(self.select_sql %  ("DESC", batchSize))
            else:
                num = cursor.execute(self.select_sql %  ("ASC", batchSize))

            if num == 0:
                transaction.commit('default')
                return None

            counts = dict()
            ids = []

            for i in cursor:

                proj_id = i[4]
                lang_id = i[5]

                if self.top:
                    proj = get_project(i[4])
                    if not proj.project[-4:] == "wiki":
                        proj_id = lookup_project("other_project").id

                    lang = get_language(i[5])
                    if not lang.language in self.detail_languages:
                        lang_id = lookup_language("other").id


                k = "'%s', '%s', '%s', %d, %d, %d" % (
                    roundtime(i[1], 1, False).strftime("%Y-%m-%d %H:%M:%S"),
                    i[2].replace("%", "%%"),
                    i[3].replace("%", "%%"),
                    proj_id,
                    lang_id,
                    i[6],
                )
                if k in counts:
                    counts[k] += i[7]
                else:
                    counts[k] = i[7]
                ids.append(i[0])

            for k, c in counts.iteritems():
                cursor.execute(self.insert_sql % (
                    "%s, %d" % (k, c), c
                    ))

            try:
                cursor.execute(self.update_sql % ', '.join(map(str, ids)))
                transaction.commit('default')
            except (MySQLdb.Warning, _mysql_exceptions.Warning) as e:
                self.logger.warning("MySQL Warning: %s" % e.message)

            return num

        except Exception as e:
            transaction.rollback('default')
            raise e