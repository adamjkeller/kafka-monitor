#!/usr/bin/env python

import argparse


class Args(object):

    @staticmethod
    def get_args():
        parser = argparse.ArgumentParser()
        parser.add_argument('-t',
                            '--topic',
                            action='store',
                            dest='topic',
                            help='Topic Name',
                            type=str,
                            required=True)
        parser.add_argument('-e',
                            '--exhibitor',
                            action='store',
                            dest='exhibitor',
                            help='Exhibitor endpoint used to get broker information from rest API',
                            type=str,
                            required=True)
        parser.add_argument('-z',
                            '--zookeeper',
                            action='store',
                            dest='zookeeper',
                            help='Zookeeper endpoint (can be IP or hostname)',
                            type=str,
                            required=True)
        return parser.parse_args()


if __name__ == '__main__':
    args = Args()
    print args.get_args()
