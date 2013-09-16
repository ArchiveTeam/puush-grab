'''Searches twitter for the upper puush id name and loads items into tracker

Requires Tweepy which can be installed using `pip install tweepy`

Usage python item_queue.py PATH_OF_CONFIG_FILE PATH_OF_MIN_ID_FILE.

The config file is an INI-like config file::

    [twitter]
    consumer_token: TODO
    consumer_secret: TODO
    access_token: TODO
    access_token_secret: TODO

The min id file contains an integer of the minimum ID to use.

There are currently some hard coded values which may need to be adjusted to
your needs.

Example crontab entry::

    @daily cd /home/tracker/puush-grab/ && /usr/bin/env python item_queue.py item_queue.conf item_queue_min_id
'''
from __future__ import print_function

import argparse
import logging
import logging.handlers
import tweepy
import ConfigParser
import re

from decentralized_puush_grab import base62_decode, ALPHABET_PUUSH
import tempfile
import subprocess
import os
import sys
import shutil

_logger = logging.getLogger(__name__)

# logging.basicConfig(level=logging.DEBUG)


class Queuer(object):
    def __init__(self, tracker_id='puush'):
        super(Queuer, self).__init__()

        self.tracker_id = tracker_id

        _logger.info('Start up')

        # stupid twitter api v1.1 requires authentication just to do a public
        # search. why do you have to be so difficult. see @fake_api
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('config')
        arg_parser.add_argument('min_id_path')
        self.args = args = arg_parser.parse_args()

        config = ConfigParser.ConfigParser()
        config.read(args.config)

        self.min_item_id = self.get_min_item_id()
        self.max_item_id = None

        consumer_token = config.get('twitter', 'consumer_token')
        consumer_secret = config.get('twitter', 'consumer_secret')
        access_token = config.get('twitter', 'access_token')
        access_token_secret = config.get('twitter', 'access_token_secret')

        _logger.debug('Set authentication tokens')

        auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        self.api = tweepy.API(auth)

        if self.check_fail_sentinel_file():
            raise Exception('Failure sentinel file exists!')

        self.search_for_ids()

        if self.max_item_id:
            self.process_max_item_id()

    def get_min_item_id(self):
        with open(self.args.min_id_path, 'r') as f:
            return int(f.read()) + 1

    def search_for_ids(self):
        _logger.debug('Attempt search')

        api = self.api

        results = api.search('puu.sh', result_type='recent')
        id_names = []

        _logger.debug('Got %d items', len(results))

        for result in results:
            for url_obj in result.entities['urls']:
                url = url_obj['expanded_url']

                match = re.search(r'puu.sh/([a-zA-Z0-9]{5})', url)

                if match:
                    id_names.append(match.group(1))

        id_ints = [base62_decode(s, ALPHABET_PUUSH) for s in id_names]
        id_ints = list(sorted(id_ints))

        _logger.debug('Got %d item ids', len(id_ints))

        if not id_ints:
            _logger.info('No IDs found. Quitting.')
            return

        self.max_item_id = id_ints[-1]

    def process_max_item_id(self):
        if self.max_item_id <= self.min_item_id:
            _logger.info('No new IDs found. Quitting')
            return

        _logger.info('Adding %d to %d', self.min_item_id, self.max_item_id)

        with tempfile.NamedTemporaryFile() as item_list_file:
            _logger.debug('Generating item list')

            proc = subprocess.Popen(['/usr/bin/env', 'python',
                'item_name_gen.py',
                '--range', '13',
                str(self.min_item_id), str(self.max_item_id)],
                stdout=item_list_file)

            proc.communicate()

            _logger.debug('item_name_gen returned %d', proc.returncode)

            if proc.returncode != 0:
                raise Exception('item_name_gen failed. return code {}'.format(
                    proc.returncode))

            self.save_new_min_id()

            _logger.debug('Queuing item list')

            # TODO: make these values configurable
            with open(item_list_file.name, 'rb') as f:
                proc = subprocess.Popen(['/home/tracker/.rvm/bin/ruby',
                    'enqueue.rb', self.tracker_id],
                    stdin=f,
                )
                proc.communicate()

            if proc.returncode != 0:
                self.save_fail_sentinel_file()
                raise Exception('enqueue failed. return code {}'.format(
                    proc.returncode))

    def save_new_min_id(self):
        old_path = '%s-old' % self.args.min_id_path
        new_path = '%s-new' % self.args.min_id_path

        shutil.copy2(self.args.min_id_path, old_path)

        with open(new_path, 'wb') as f:
            f.write(str(self.max_item_id).encode())

        _logger.debug('Save new min ID.')
        os.rename(new_path, self.args.min_id_path)

    def save_fail_sentinel_file(self):
        path = '%s-fail' % self.args.min_id_path

        with open(path, 'wb'):
            pass

    def check_fail_sentinel_file(self):
        path = '%s-fail' % self.args.min_id_path
        return os.path.exists(path)

if __name__ == '__main__':
    log_handler = logging.handlers.RotatingFileHandler('item_queue.log',
        maxBytes=1048576,
        backupCount=10)
    
    formatter = logging.Formatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s')
    log_handler.setFormatter(formatter)
    
    _logger.setLevel(logging.DEBUG)
    _logger.addHandler(log_handler)
    

    lock_file = '/tmp/item_queue_lock'

    if os.path.exists(lock_file):
        _logger.error('Lock file found. Quitting.')
        sys.exit(1)

    try:
        try:
            _logger.debug('Create lock file')
            with open(lock_file, 'wb'):
                pass
            Queuer()
        finally:
            _logger.debug('Remove lock file')
            os.remove(lock_file)
    except Exception as e:
        _logger.exception('Queuer error!')
        raise e

    _logger.info('Done.')

