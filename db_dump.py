#!/usr/bin/env python
'''Export data out of the redis database'''
from __future__ import print_function

import argparse
import json
import redis

from decentralized_puush_grab import (base62_decode, base62_encode, ALPHABET,
    ALPHABET_PUUSH)


def main():
    arg_parser = argparse.ArgumentParser()
    sub_parsers = arg_parser.add_subparsers(title='Command',
        dest='command')

    arg_parser.add_argument('--host', help='Hostname of database server',
        default='localhost')
    arg_parser.add_argument('--port', help='Port number of server',
        default=6379, type=int)
    arg_parser.add_argument('--db', help='Database number',
        default=0, type=int)

    done_arg_parser = sub_parsers.add_parser('done',
        help='Dump out done items')
    done_arg_parser.add_argument('project', help='Name of the project')
    done_arg_parser.set_defaults(func=done_command)

    log_arg_parser = sub_parsers.add_parser('log',
        help='Dump out log with privacy')
    log_arg_parser.add_argument('--scrub-username', action='store_true',
        help='Scrub out the usernames as well')
    log_arg_parser.add_argument('project', help='Name of the project')
    log_arg_parser.set_defaults(func=log_command)

    archived_log_arg_parser = sub_parsers.add_parser('archivedlog',
        help='Dump out archived log with privacy')
    archived_log_arg_parser.add_argument('--scrub-username',
        action='store_true',
        help='Scrub out the usernames as well')
    archived_log_arg_parser.add_argument('log_file',
        help='Path of log file')
    archived_log_arg_parser.set_defaults(func=archived_log_command)

    args = arg_parser.parse_args()
    args.func(args)


def get_redis_connection(args):
    return redis.StrictRedis(host=args.host, port=args.port, db=args.db)


def get_expanded_item_name(item_name):
    if ',' in item_name:
        alphabet = ALPHABET
        start_item, end_item = item_name.split(',', 1)
    elif ':' in item_name:
        alphabet = ALPHABET_PUUSH
        start_item, end_item = item_name.split(':', 1)
    else:
        start_item = item_name
        end_item = item_name
        alphabet = ALPHABET_PUUSH

    start_num = base62_decode(start_item, alphabet)
    end_num = base62_decode(end_item, alphabet)

    for num in xrange(start_num, end_num + 1):
        yield base62_encode(num, alphabet)


def done_command(args):
    r = get_redis_connection(args)

    for item in r.smembers('%s:done' % args.project):
        for expanded_item_name in get_expanded_item_name(item):
            print(expanded_item_name)


def log_command(args):
    r = get_redis_connection(args)
    fetch_size = 10000
    i = 0

    while True:
        l = r.lrange('%s:log' % args.project, i, i + fetch_size - 1)

        if not l:
            break

        for item in l:
            doc = json.loads(item)
            doc['ip'] = '<scrubbed>'
            id_doc = json.loads(doc['id'])
            doc['id'] = id_doc

            if args.scrub_username:
                doc['by'] = '<scrubbed>'

            print(json.dumps(doc))

        i += fetch_size


def archived_log_command(args):
    with open(args.log_file, 'rt') as f:
        for item in f:
            doc = json.loads(item)
            doc['ip'] = '<scrubbed>'
            id_doc = json.loads(doc['id'])
            doc['id'] = id_doc

            if args.scrub_username:
                doc['by'] = '<scrubbed>'

            print(json.dumps(doc))


if __name__ == '__main__':
    main()
