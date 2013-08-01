#!/usr/bin/env python
from __future__ import print_function
import argparse
import os.path
import subprocess
import time
import os
import random
import shutil
import logging


_logger = logging.getLogger(__name__)

VERSION = 20130731.1
USER_AGENT = 'ArchiveTeam DPG/{}'.format(VERSION)

# Be careful! Some implementations have the ordering of upper and lower case
# differently
ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'


# http://stackoverflow.com/a/1119769/1524507
def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def base62_decode(string, alphabet=ALPHABET):
    """Decode a Base X encoded string into the number

    Arguments:
    - `string`: The encoded string
    - `alphabet`: The alphabet to use for encoding
    """
    base = len(alphabet)
    strlen = len(string)
    num = 0

    idx = 0
    for char in string:
        power = (strlen - (idx + 1))
        num += alphabet.index(char) * (base ** power)
        idx += 1

    return num


class Grabber(object):
    def __init__(self, min_delay):
        self._max_int = base62_decode('40000')
        self._min_delay = min_delay
        self._seconds_throttle = 1.0
        self._start_time = time.time()
        self._data_dir = os.path.abspath('data')
        self._report_dir = os.path.abspath('report')
        self._wget_dir = os.path.abspath('wget-temp-{}'.format(os.getpid()))
        self._next_time = 0
        self._running = True

        self._run()

    def _run(self):
        _logger.debug('Running with delay at {} seconds'.format(
            self._min_delay))

        while self._running:
            self._do_job()

            while time.time() < self._next_time:
                if os.path.exists('STOP') \
                and os.path.getmtime('STOP') > self._start_time:
                    self._running = False
                    break

                time.sleep(2)

        _logger.debug('Stopping')

    def _do_job(self):
        item_num = random.randint(0, self._max_int)
        item_name = base62_encode(item_num)

        _logger.info('Starting fetch for item {} ({})'.format(item_name,
            item_num))

        if not os.path.exists(self._wget_dir):
            _logger.debug('Creating dir in {}'.format(self._wget_dir))
            os.makedirs(self._wget_dir)

        return_code = self._run_wget(item_name)

        _logger.debug('wget return code {}'.format(return_code))

        if not os.path.exists(self._data_dir):
            os.makedirs(self._data_dir)

        if not os.path.exists(self._report_dir):
            os.makedirs(self._report_dir)

        self._save_report(item_name)

        if return_code not in [0]:
            _logger.info('Failed')
            self._throttle(True)
        else:
            self._move_files(item_name)
            _logger.info('OK')
            self._throttle(False)

        shutil.rmtree(self._wget_dir)

    def _run_wget(self, item_name):
        env = os.environ.copy()
        if 'PATH' not in env:
            env['PATH'] = ''

        env['PATH'] += ':.:../:'
        self._warc_name = 'puush-{}-{}'.format(item_name, int(time.time()))

        _logger.debug('Running wget with warc name {}'.format(self._warc_name))

        command_args = ['wget-lua',
            "-U", USER_AGENT,
            "-nv",
            "-o", '{}/wget.log'.format(self._wget_dir),
            "--lua-script", "puush.lua",
            "--no-check-certificate",
            "--output-document", '{}/wget.tmp'.format(self._wget_dir),
            "--truncate-output",
            "-e", "robots=off",
            "--rotate-dns",
            "--timeout", "60",
            "--tries", "20",
            "--waitretry", "5",
            "--warc-file", "{}/{}".format(self._wget_dir, self._warc_name),
            "--warc-header", "operator: Archive Team",
            "--warc-header",
                "decentralized-puush-dld-script-version: {}".format(VERSION),
            "http://puu.sh/{}".format(item_name)
        ]

        return subprocess.call(command_args, env=env)

    def _move_files(self, item_name):
        _logger.debug('Move files')

        source = "{}/{}.warc.gz".format(self._wget_dir, self._warc_name)
        dest = "{}/{}.warc.gz".format(self._data_dir, self._warc_name)

        if os.path.exists(source):
            os.rename(source, dest)
        else:
            _logger.debug('Warc does not exist')

    def _save_report(self, item_name):
        _logger.debug('Saving report')

        source = "{}/wget.log".format(self._wget_dir)
        dest = "{}/wget-{}-{}.log".format(self._report_dir, int(time.time()),
            item_name)

        if os.path.exists(source):
            os.rename(source, dest)
        else:
            _logger.debug('Log does not exist')

    def _throttle(self, is_bad):
        if is_bad:
            self._seconds_throttle *= 2.0
            self._seconds_throttle = min(3600.0, self._seconds_throttle)
        else:
            self._seconds_throttle = 1.0

        delay_time = self._min_delay + self._seconds_throttle
        delay_time *= random.uniform(0.8, 1.2)

        _logger.info('Next download in {:.1f} seconds'.format(delay_time))

        self._next_time = time.time() + delay_time


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(u'--delay', type=int, default=10,
        help=u'Minimum time in seconds between requests')
    args = arg_parser.parse_args()
    Grabber(args.delay)
