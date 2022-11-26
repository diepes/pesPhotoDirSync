'''
    (c) Pieter Smit 2020 GPL3
    - Scan two dirs (Photos) , generate file hash for each
    - Detect corrupt files, and duplicates
    - Provide cleanup and dir sync two way to preserve Photos

    20200718 Currently only one dir , use hashdeep hash info to find duplicates
        - run hashdeep
'''
import asyncio
import concurrent.futures  # ThreadPoolExecutor for hash
import hashlib
import os
#import threading  # run PySimpleGui in own thread, comms through queue
import queue
import re

#from . import photoGui
from .. import classFiles, globals


async def readHashFromFile(files: classFiles.classFiles,
                           filename,
                           c: dict, #counters
                           ) -> dict:
    '''
    read hashdeep csv and entries to files.
    '''
    import csv
    global exitFlag
    if not os.path.exists(filename):
        print(f"Debug: readHashFromFile missing csv file {filename}")
        exitFlag = True
        exit(1)
    c['hashdeepNoFile'] = 0
    with open(filename) as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for counter,row in enumerate( csvreader ):
            if row[0][0] in [ "%", "#"]:
                ## print(', '.join(row))
                if "Invoked from:" in row[0]:
                    basepath = row[0].split(":")[1].strip()
                    print(f"Debug: basepath from hashdeep: {basepath}")
            else:
                # Got file entry and checksum row. (Not header)
                assert len(row[1]) == 32, f"Error md5 length wrong ? line={counter}"
                assert len(row[2]) == 64, f"Error sha256 length wrong ? line={counter}"
                # key=row[1]+"-"+row[2]
                try:
                    files.add(
                        fileInfo=
                            classFiles.classFile(
                            pathbase=basepath,
                            path=row[3],
                            size=int(row[0]),
                            hashmd5=row[1],
                            hashsha256=row[2],
                            source=f"hashdeep@{filename}@line#{counter+1}",
                        )
                    )
                except FileNotFoundError:
                   c['hashdeepNoFile'] += 1
    # No return, the c(counters) should be updated


if __name__ == '__main__':
    print(f"ERR {__name__} is only a module of import") 
    print("The END.  __main__")
