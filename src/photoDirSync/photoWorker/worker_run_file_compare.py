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


async def run_file_compare( files: classFiles.classFiles, dir: str, qLog, qInfo) -> None:
    print(f"Start ... run_file_compare: dir={dir}")
    t = time.time()
    qLog[0].put((qLog[1],f"run_file_compare: start..."))
    tstart = time.time()
    _dir_scan(files=files, dir_name=dir,
              whitelist=["gif", "GIF", "jpg" , "JPG", "jpeg", "png", "PNG", "mp4" ],
              qLog=qLog , max=100000,
              )
    t_dir_scan = time.time() - tstart
    fLen_dir_scan = len(files)
    qLog[0].put((qLog[1],f"run_file_compare: found {len(files)} in {dir} in {time.time()-t:.2f}s  {len(files)/(time.time()-t):.2f}"))
    print(f"run_file_compare: found {len(files)} files in {dir} in {time.time()-t:.2f}s")
    # print(next(iter(files)))
    # print(f"t_dir_list={t_dir_list} fLen_dir_list={fLen_dir_list}     t_dir_scan={t_dir_scan} fLen_dir_scan={fLen_dir_scan}")
    await asyncio.sleep(2)
    print(f"run_file_compare:Test hash")
    qLog[0].put((qLog[1],f"run_file_compare:Test hash"))
    hashCount = 0
    hashErr = 0
    thash = time.time()
    # We can use a with statement to ensure threads are cleaned up promptly
    # max_workers=5 ( Defaut CPU*4 )
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        #  use zip to limit number of files for testing
        future_to_hash = {executor.submit(classFiles.hashMd5Sha256,os.path.join(f[1][0].pathbase,f[1][0].path)): f for _, f in zip(range(5000), files.items_file())}
        for future in concurrent.futures.as_completed(future_to_hash):
            if globals.exitFlag:
                print(f'run_file_compare break on globals.exitFlag')
                for ft in future_to_hash:
                    ft.cancel()
                print(f'run_file_compare break on globals.exitFlag canceled all')
                break
            f = future_to_hash[future]
            try:
                hashInfo = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (f, exc))
                hashErr += 1
                qInfo[0].put((qInfo[1],f"{hashCount=} {hashErr=}"))
            else:
                # print('%r page is %d bytes' % (f, len(files)))
                hashCount += 1
                if qLog[0] and qLog[0].qsize() < 2:
                    qInfo[0].put((qInfo[1],f"{hashCount=} {hashErr=}"))
                    qLog[0].put( (qLog[1],f"..{hashCount=} time={time.time()-thash:.2f}s {hashInfo=}") )

    qInfo[0].put((qInfo[1],f"{hashCount=} {hashErr=}"))
    print("The End. run_file_compare()")


def _dir_scan(dir_name,
              whitelist,
              files: classFiles.classFiles,
              qLog=(None,""),
              max: int = globals.maxFiles,
              ):
    ''' fast, returns os.DirEntry (20200719 double the speed of _dir_list)
        https://docs.python.org/3/library/os.html#os.DirEntry
    '''
    def scantree(path):
        """Recursively yield DirEntry objects for given directory."""
        for entry in os.scandir(path):
            if entry.is_dir(follow_symlinks=False):
                yield from scantree(entry.path)
            else:
                yield entry
    #
    count=0
    t=time.time()
    if qLog[0]:
        qLog[0].put((qLog[1],f"{count=} dir_scan dir_name={dir_name}"))
    #with os.scandir(dir_name) as it:
    for entry in scantree(dir_name):
            if entry.is_file() and os.path.splitext(entry.name)[1][1:] in whitelist:
                files.add(classFiles.classFile(pathbase=dir_name, path=entry.path, size=entry.stat(follow_symlinks=False).st_size))
                count += 1
                if qLog[0] and qLog[0].qsize() < 2:
                    qLog[0].put((qLog[1],[ f"_dir_scan: {count=}", ]))
            else:
                if qLog[0] and qLog[0].qsize() < 2:
                    qLog[0].put((qLog[1],f"dir_scan dir skip count={count} @{count/(time.time()-t):.3f}s t={t}   ext={os.path.splitext(entry.name)[1]} f={os.path.join(dir_name, entry.name)}"))
            if globals.exitFlag:
                break
            if count > max:
                print("Debug call files.save")
                files.save(pathbase=dir_name, fileHashName="/tmp/save-pesDirSyncAB.txt")
                break
    if qLog[0]: qLog[0].put((qLog[1],f"dir_scan exit count={count} dir_name={dir_name}"))
    return files  # classFiles


     