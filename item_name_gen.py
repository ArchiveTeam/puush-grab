#!/usr/bin/env python
'''Generates item names to be input into tracker'''
from __future__ import print_function

import argparse

from decentralized_puush_grab import (base62_decode, base62_encode, ALPHABET,
    ALPHABET_PUUSH)


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
    arg_parser.add_argument('--legacy-alphabet', action='store_true',
        help='Use an alternate alphabet (not Puush alphabet)')

    args = arg_parser.parse_args()

    if args.legacy_alphabet:
        alphabet = ALPHABET
        separator = ','
    else:
        alphabet = ALPHABET_PUUSH
        separator = ':'

    if args.range and not (1 <= args.range <= 100):
        raise Exception("Range should be positive and not too large")

    exclusion_set = set()

    if args.exclusion_file:
        with open(args.exclusion_file, 'rt') as f:
            for line in f:
                exclusion_set.add(int(line.strip()))

    if args.exclusion_file_62:
        with open(args.exclusion_file_62, 'rt') as f:
            for line in f:
                exclusion_set.add(base62_decode(line.strip(), alphabet))

    l = []
    for i in xrange(args.start_int, args.end_int + 1):
        if i not in exclusion_set:
            l.append(i)

        if i in exclusion_set or i == args.end_int or len(l) >= args.range:
            if len(l) == 1:
                print(base62_encode(l[0], alphabet))
            elif l:
                print('{}{}{}'.format(
                    base62_encode(l[0], alphabet),
                    separator,
                    base62_encode(l[-1], alphabet)
                ))
            l = []


if __name__ == '__main__':
    main()
