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
import urlparse

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

    select_sql = "SELECT id, timestamp, banner, campaign, project_id, language_id, country_id FROM bannerimpression_raw WHERE processed = 0 LIMIT %d"

    insert_sql = "INSERT INTO bannerimpressions (timestamp, banner, campaign, project_id, language_id, country_id, count) VALUES (%s) ON DUPLICATE KEY update count=count+%d"

    update_sql = "UPDATE bannerimpression_raw SET processed = 1 WHERE id IN (%s)"


    def handle(self, *args, **options):
        starttime = datetime.now()
        self.debug = options.get('debug')
        self.verbose = options.get('verbose')
        batch = options.get('batch')
        rounds = options.get('rounds')

        for r in range(rounds):
            self.run(batch)

        endtime = datetime.now()


    @transaction.commit_manually
    def run(self, batchSize=1000):
        if not isinstance(batchSize, int):
            raise TypeError("Invalid batch size %s" % batchSize)

        cursor = connections['default'].cursor()

#        try:
        impressions = cursor.execute(self.select_sql % batchSize)

        counts = dict()

        for i in impressions:
            k = "'%s', '%s', '%s', %d, %d, %d" % (
                datetime.strptime(i['timestamp'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:00"),
                i['banner'],
                i['campaign'],
                i['project_id'],
                i['language_id'],
                i['country_id'],
            )
            if k in counts:
                counts[k] += 1
            else:
                counts[k] = 1

        for k, c in counts.iteritems():
            cursor.execute(self.insert_sql % (
                "(%s, %d)" % (k, c), c
            ))

        to_update = ""
        for i in impressions:
            to_update = "%s, %d" % (to_update, i['id'])

        cursor.execute(self.update_sql % to_update)

#            transaction.commit('default')
#
#        except Exception as e:
#            transaction.rollback('default')
#            raise e