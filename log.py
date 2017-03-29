#!/usr/bin/env python

import logging
from sys import stdout


class SetLogging(object):

    @staticmethod
    def setup_logging():
        logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s',
                            level=logging.INFO,
                            stream=stdout)


if __name__ == '__main__':
    SetLogging.setup_logging()
