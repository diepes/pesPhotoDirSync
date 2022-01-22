#!/usr/bin/env python3.8
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
#import EXIF
##import pyexiv2
# from PIL import Image, ImageTk
# import io
import sys
import time
from dataclasses import dataclass

#from . import photoGui
from .. import classFiles, globals


async def readHashFromFile(files: classFiles.classFiles,
                           filename,
                           ) -> dict:
    import csv
    global exitFlag
    if not os.path.exists(filename):
        print(f"Debug: readHashFromFile missing csv file {filename}")
        exitFlag = True
        exit(1)
    c = dict() #Counters
    c['clean']=0
    c['del']=0
    c['ZeroSize']=0
    c['stillDup']=0
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
    print(f"FIND Duplicates: {files.getNumDuplicateFiles()} ")
    # Set list of Prefered dirs to remove if there are duplicates.
    dirsRemove=[ "/.stversions/", "/backupRsyncDel-", "temp/CameraUploads/", "/CameraUploads/", "/20190000-Info/",
        "/20111030-Quinn-Hedgehog/",
        "/201310050-SydneyHarbour/", "/2013-PaulaNtshonalanga/", "/20110729-BillsBest-Sony/", "/temp/",
        "/20161027-Natalie+Evan@CA/", "/20150300-DogsHomeAlone/", "/200880113-Paula-SunFlowerPuzzle/",
        "/Screenshots/", "/200903-Study/", "/Note3-DCIM/", "/20180600-Station/",
        "NoMatch.jpg", "/00000000-Recover-", "/20140203-KarooNasionalePark/" , ".mp4",
        "/Paula-201810-Phone/", "/Pieter-202001-OnePlus6/" , "/20121211-SDR-PowerFailure./" , "/UNSORTED/" , "20070600-Liam-0-5",
        "/Photos/2014/20140914-SDR-WashPoolCover/", "/Photos/2014/20140300-Paula/", "/Photos/2014/20140400-Dogs/", "/CameraUploads/",
        ]
    # add to dirsRemove prefered e.g /2021/01/ -> /2021/20210101-NewYear/
    for y in range(2003,2020):
        for m in range(1, 13):
            dirsRemove.append(f"/{int(y)}/{int(m):0>2d}/")
            # print(f"/{int(y)}/{int(m):0>2d}/")
    falldel = []
    for key,fl in files.items_hash():  # loop through file hash's  v=list(fileObj)
        if len( fl ) > 1:
            if files.getHashFileSize(key) == 0:
                c['ZeroSize'] =+ len(fl)
                falldel.extend( fl )
                continue
            #  Duplicate hash
            fdel = []
            fkeep = []
            matchHistory = []
            for f in [x.path for x in fl]:
                #Check if filename matches list of undesired paths
                for match in dirsRemove:
                    if match in f:
                        fdel.append(f)
                        c['del'] += 1
                        matchHistory.append(match)
                        break
                else:
                    #check for dup file in same dir as file we are keeping
                    if len(fkeep) == 1:
                        if os.path.dirname(f) == os.path.dirname(fkeep[0]):
                            fdel.append(f)
                            c['del'] += 1
                            print(f" Del {f} in same path as matching {fkeep[0]}")
                        else:
                            fkeep.append(f)
                    else:
                        fkeep.append(f)
            if len(fkeep) == 0:  # Keep one.
                fkeep.append(fdel.pop(0))
                c['del'] -= 1
                # print(f" Keeping one copy, all were to be removed fkeep={fkeep}   fdel={', '.join(fdel)}  matchHistory={', '.join(matchHistory)}")
            assert (len(fkeep)+len(fdel)) == len( fl ) ,  "BUG lost a file ??"
            assert (len(fkeep) > 0) , f"Deleting all files ? {fdel=}"
            if len(fkeep) == 1:
                c['clean'] +=1
                falldel.extend(fdel)
            else:
                print(f" Duplicate: fkeep={', '.join(fkeep)}")
                print(f"            fdel={', '.join(fdel)}")
                c['stillDup'] =+1

            #  Done filtering multiple files for single hash
            #print(f"         fkeep={', '.join(fkeep)}  <<<<  fdel={', '.join(fdel)}")

    print(f"done. {c['del']=} {c['clean']=} dup cleaned, found {c['ZeroSize']=} with zero size")
    eq = "=" if ( len(falldel) == (c['del'] + c['ZeroSize']) ) else "!="
    print(f" falldel:{len(falldel)} {eq} {c['del']} + {c['ZeroSize']} = {c['del'] + c['ZeroSize']}")

    print(f" basepath={basepath} , {c['stillDup']=}")
    c['fdel']=0
    c['fdel_missing']=0
    for f in falldel:
        fname = f"{basepath}/{f}"
        if os.path.isfile(fname):
            os.remove(fname)
            c['fdel'] += 1
        else:
            print(f"Error - delfile missing {fname=} {type(f)=}")
            c['fdel_missing'] += 1
    print(f"Deleted {c['fdel']}, missing {c['fdel_missing']} done. ")
    return c


if __name__ == '__main__':
    print(f"ERR {__name__} is only a module of import") 
    print("The END.  __main__")
