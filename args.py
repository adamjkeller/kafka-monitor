#!/usr/bin/env python

import argparse


class Args(object):

    @staticmethod
    def get_args():
        parser = argparse.ArgumentParser()
        parser.add_argument('-s',
                            '--stackname',
                            action='store',
                            dest='stackname',
                            help='Stack name',
                            type=str,
                            required=True)
        parser.add_argument('-d',
                            '--domain',
                            action='store',
                            dest='domain',
                            help='AWS Region',
                            type=str,
                            required=True)
        parser.add_argument('-t',
                            '--topic',
                            action='store',
                            dest='topic',
                            help='Topic Name',
                            type=str,
                            required=True)
        return parser.parse_args()


if __name__ == '__main__':
    args = Args()
    print args.get_args()
