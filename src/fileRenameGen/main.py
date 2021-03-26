#!/bin/env python
''' Called by run.py '''
import sys
import os
import logging

import functions as fun

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('move_files')

if __name__ == '__main__':
    print('Error should be imported by run.py')

def main(argv):
    print(f"main {argv}")
    for f in fun.getFiles(argv['src']):
        if os.path.isfile(f):
            fun.renameFile(f, argv['dest'], argv=argv)
        # print(f"{f=}")