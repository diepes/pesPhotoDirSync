#!/bin/env python
import sys
import os
import argparse
import getpass
import logging
import main

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('move_files')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-d', '--dest', dest='dest', type=str, help='Destination dir',
                        default=os.path.expanduser("~/Pictures/Photos"),
    )
    parser.add_argument('-s', '--src', dest='src', type=str, help='Source dir',
        default=os.path.expanduser("~/Pictures/Photos/CameraUploads/Paula-SamsungS7edge-CrackedScreen/WhatsApp Images/Sent"),
    )
    parser.add_argument('-m', '--multidir', dest='multidir', action='store_true', help='pick first if multi dir match')
    parser.add_argument('-o', '--option', dest='option', type=str, help='Example of str arg')

    parser.add_argument('fileglob', metavar='file', type=str, help='Files to rename and move')

    args = parser.parse_args()

    # Never ask for a password in command-line. Manually ask for it here
    #password = getpass.getpass()
    argv=vars(args)
    logger.debug(f'parsed args, calling main with argv={argv}')
    main.main(argv)
