#!/usr/bin/env python
'''Generates item names to be input into tracker'''
from __future__ import print_function
import argparse
from decentralized_puush_grab import base62_decode, base62_encode


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('start_int', type=int,
        help='The starting base 10 integer')
    arg_parser.add_argument('end_int', type=int,
        help='The ending base 10 integer')
    arg_parser.add_argument('--exclusion-file',
        help='A path to a file containing lines of base 10 integers to '
        'exclude from the print out')
    arg_parser.add_argument('--exclusion-file-62',
        help='A path to a file containing lines of base 62 integers to '
        'exclude from the print out')

    args = arg_parser.parse_args()

    exclusion_set = set()

    if args.exclusion_file:
        with open(args.exclusion_file, 'rt') as f:
            for line in f:
                exclusion_set.add(int(line.strip()))

    if args.exclusion_file_62:
        with open(args.exclusion_file_62, 'rt') as f:
            for line in f:
                exclusion_set.add(base62_decode(line.strip()))

    for i in xrange(args.start_int, args.end_int + 1):
        if i not in exclusion_set:
            print(base62_encode(i))


if __name__ == '__main__':
    main()
