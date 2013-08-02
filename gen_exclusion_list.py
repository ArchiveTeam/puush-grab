#!/usr/bin/env python
'''Generates a list of item names based from directory of warc files'''
from __future__ import print_function
import argparse
import glob
import os.path


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('directory', help='The path of the directory',
        nargs='+')

    args = arg_parser.parse_args()

    for dir_path in args.directory:
        for file_path in glob.glob(dir_path + '*.*'):
            item_name = os.path.basename(file_path).split('-')[1]
            print(item_name)


if __name__ == '__main__':
    main()
