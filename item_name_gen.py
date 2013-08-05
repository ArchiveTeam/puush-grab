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
    arg_parser.add_argument('--range', type=int,
        help='Generate a list using the range notation of the given size',
        default=1)

    args = arg_parser.parse_args()

    if args.range and not (1 <= args.range <= 100):
        raise Exception("Range should be positive and not too large");

    exclusion_set = set()

    if args.exclusion_file:
        with open(args.exclusion_file, 'rt') as f:
            for line in f:
                exclusion_set.add(int(line.strip()))

    if args.exclusion_file_62:
        with open(args.exclusion_file_62, 'rt') as f:
            for line in f:
                exclusion_set.add(base62_decode(line.strip()))

    range_start = None
    range_size = None
    for i in xrange(args.start_int, args.end_int + 1):
        if range_start is None:
            range_start = i
            range_size = 1
    
        if i in exclusion_set or i == args.end_int or range_size >= args.range:
            if range_size == 1:
                 print(base62_encode(i))
            else:
                print('{},{}'.format(base62_encode(range_start),
                    base62_encode(range_start + range_size - 1)))

            range_start = None
            rage_size = None
        else:
            range_size += 1


if __name__ == '__main__':
    main()
