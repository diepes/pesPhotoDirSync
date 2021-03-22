#!/usr/bin/env python3.8
'''
    (c) Pieter Smit 2020 GPL3
    - Scan two dirs (Photos) , generate file hash for each
    - Detect corrupt files, and duplicates
    - Provide cleanup and dir sync two way to preserve Photos

    20200718 Currently only one dir , use hashdeep hash info to find duplicates
        - run hashdeep
'''
#from . import photoGui
from . import globals

from dataclasses import dataclass
#import EXIF
##import pyexiv2
# from PIL import Image, ImageTk
# import io
import sys
import os
import re
import time
import asyncio
import threading  # run PySimpleGui in own thread, comms through queue
import queue
import hashlib
import concurrent.futures  # ThreadPoolExecutor for hash



@dataclass
class classFile:
    pathbase: str
    path: str
    size: int
    hashmd5: str = ""
    hashsha256: str = ""
    flagCalculatedHash: bool = False

class classFiles:
    ''' Class to keep track of files and hashes '''
    def __init__(self):
        from typing import Dict, List
        self.dictHash : Dict[str, List[classFile] ] = dict()
        self.dictFile : Dict[str, List[classFile] ] = dict()

    def __len__(self) -> int:
        return len(self.dictFile)

    def __getitem__(self, key) -> classFile:
        return self.dictFile.get(key, [])

    def __iter__(self):
        return iter(self.dictFile)

    def items_file(self):
        return self.dictFile.items()
    def items_hash(self):
        return self.dictHash.items()
    def item_file(self, key) -> classFile:
        return self.dictFile[key]
    def item_hash(self, key) -> classFile:
        return self.dictHash[key]

    def add(self, file: classFile):
        assert file.path != "." , "can't use . as path"
        if file.hashmd5 == "" or file.hashsha256 == "":
            size = file.size
            file.getSize, file.getMd5, file.getSha256, getFname = hashMd5Sha256(os.path.join(file.pathbase,file.path))
            file.flagCalculatedHash = True
            assert size == file.size, f"Error file size changed for {file.pathbase=} {file.path=} {getFname=}"
        # print(f"{file.path=} {len(self.dictFile)}")
        if file.path in self.dictFile.keys():
            #
            print(f"Debug: add found dup {file.path}")
            if all( [(x != file.pathbase) for x in self.dictFile[file.path]['pathbase']]):
                self.dictFile[file.path].append(file)
            else:
                assert False, f"class.files add Duplicate file name added ? {file.pathbase=} {file.path=} "
        else:
            #print(f"Debug add {path=}")
            self.dictFile[file.path] = [ file ]

    def save(self, pathbase: str, fileHashName: str):
        print(f"Debug save {len(self.dictFile)} docs {pathbase=} to {fileHashName=}")
        with open(fileHashName, "w") as f:
            f.write(f"%%%% size,md5,sha256,filename\n")
            f.write(f"## pathbase = \"{pathbase}\"\n")
            for pathDocs,fileInstances in self.dictFile.items():
                for fileInstance in fileInstances:
                    #print(f"Debug {fileInstance=}  type(fileInstance)={type(fileInstance)}")
                    if fileInstance.pathbase == pathbase:
                        print(f"save - entry {fileInstance.pathbase} / {fileInstance.path} ")
                        f.write(f"{fileInstance.size},{fileInstance.hashmd5},{fileInstance.hashsha256},{fileInstance.path}\n")
                    else:
                        print(f"skip - entry {fileInstance.pathbase} / {fileInstance.path} ")

    def load(self, pathbase: str, fileHashName: str) -> int:
        import csv
        countNew = 0
        with open(fileHashName) as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for counter,row in enumerate( csvreader ):
                if row[0][0] in [ "%", "#"]:
                    print(', '.join(row))
                    if "Invoked from:" in row[0]:
                        basepath = row[0].split(":")[1].strip()
                        print(f" basepath from hashdeep {basepath}")
                else:
                    #  size,md5,sha256,filename
                    assert not row[3] in self.dictfiles, f"Duplicate file {row[3]=} {counter=}"
                    self.dictfile[row[3]] = classFile(  pathbase=pathbase,
                                                        path=row[3],
                                                        size=row[0],
                                                        hashmd5=row[1],
                                                        hashsha256=row[2],
                                                        flagCalculatedHash=False
                                                    )
                    countNew += 1
        return countNew


def _dir_scan(dir_name,
              whitelist,
              files: classFiles,
              qLog=(None,""),
              max: int = max,
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
                files.add(classFile(pathbase=dir_name, path=entry.path, size=entry.stat(follow_symlinks=False).st_size))
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


async def readHashFromFile(files: classFiles,
                           filename,
                           ) -> None:
    import csv
    global exitFlag
    if not os.path.exists(filename):
        print(f"Debug: readHashFromFile missing file {filename}")
        exitFlag = True
        exit(1)
    with open(filename) as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for counter,row in enumerate( csvreader ):
            if row[0][0] in [ "%", "#"]:
                print(', '.join(row))
                if "Invoked from:" in row[0]:
                    basepath = row[0].split(":")[1].strip()
                    print(f" basepath from hashdeep {basepath}")
            else:
                # print(row)
                # print(row[2])
                assert len(row[1]) == len("7f44a72ad9a8928ffad9a37c9dcb46c0") , f"Error md5 length wrong ? line={counter}"
                assert len(row[2]) == len("92126c1ef0ec2183ef72dccdff3b826b0eb41c8546b3b54d46a384b12978dba0") , "Error sha256 length wrong ? line={counter}"
                key=row[1]+"-"+row[2]
                if key in files.dictHash:
                    files.dictHash[key]["files"].append(row[3])
                    # print("adding duplicate file name")
                else:
                    # Create key Hash, dict with file size, and list of file names
                    files.dictHash[key] = { "size": int(row[0]) , "files": [ row[3] , ] }
                # print(counter,files["hash"][key])
    print(f"FIND Duplicates: {len( files['hash'] )} ")
    dirsRemove=[ "/.stversions/", "/backupRsyncDel-", "temp/CameraUploads/", "/CameraUploads/", "/20190000-Info/",
        "/20111030-Quinn-Hedgehog/",
        "/201310050-SydneyHarbour/", "/2013-PaulaNtshonalanga/", "/20110729-BillsBest-Sony/", "/temp/",
        "/20161027-Natalie+Evan@CA/", "/20150300-DogsHomeAlone/", "/200880113-Paula-SunFlowerPuzzle/",
        "/Screenshots/", "/200903-Study/", "/Note3-DCIM/", "/20180600-Station/",
        "NoMatch.jpg", "/00000000-Recover-", "/20140203-KarooNasionalePark/" , ".mp4",
        "/Paula-201810-Phone/", "/Pieter-202001-OnePlus6/" , "/20121211-SDR-PowerFailure./" , "/UNSORTED/" , "20070600-Liam-0-5"
        ]
    for y in range(2003,2020):
        for m in range(1, 13):
            dirsRemove.append(f"/{int(y)}/{int(m):0>2d}/")
            # print(f"/{int(y)}/{int(m):0>2d}/")

    countclean=0
    countdel=0
    countZeroSize=0
    countStillDuplicate=0
    falldel = []
    for key in files["hash"]:
        if len( files["hash"][key]["files"] ) > 1:
            if files["hash"][key]["size"] == 0:
                countZeroSize =+ len(files["hash"][key]["files"])
                falldel.extend(files["hash"][key]["files"])
                continue
            #  Duplicate hash
            fdel = []
            fkeep = []
            matchHistory = []
            for f in files["hash"][key]["files"]:
                #Check if filename matches list of undesired paths
                for match in dirsRemove:
                    if match in f:
                        fdel.append(f)
                        countdel += 1
                        matchHistory.append(match)
                        break
                else:
                    #check for dup file in same dir as file we are keeping
                    if len(fkeep) == 1:
                        if os.path.dirname(f) == os.path.dirname(fkeep[0]):
                            fdel.append(f)
                            countdel += 1
                            print(f" Del {f} in same path as matching {fkeep[0]}")
                        else:
                            fkeep.append(f)
                    else:
                        fkeep.append(f)
            if len(fkeep) == 0:  # Keep one.
                fkeep.append(fdel.pop(0))
                countdel -= 1
                # print(f" Keeping one copy, all were to be removed fkeep={fkeep}   fdel={', '.join(fdel)}  matchHistory={', '.join(matchHistory)}")
            assert (len(fkeep)+len(fdel)) == len(files["hash"][key]["files"]) ,  "BUG lost a file ??"
            assert (len(fkeep) > 0) , f"Deleting all files ? fdel={fdel}"
            if len(fkeep) == 1:
                countclean +=1
                falldel.extend(fdel)
            else:
                print(f" Duplicate: fkeep={', '.join(fkeep)}")
                print(f"            fdel={', '.join(fdel)}")
                countStillDuplicate =+1

            #  Done filtering multiple files for single hash
            #print(f"         fkeep={', '.join(fkeep)}  <<<<  fdel={', '.join(fdel)}")

    print(f"done. del={countdel} countclean={countclean} dup cleaned, found {countZeroSize} with zero size")
    eq = "=" if ( len(falldel) == (countdel + countZeroSize) ) else "!="
    print(f" falldel:{len(falldel)} {eq} {countdel} + {countZeroSize} = {countdel + countZeroSize}")

    print(f" basepath={basepath} , countStillDuplicate={countStillDuplicate}")
    fdel=0
    for f in falldel:
        os.remove(f"{basepath}/{f}")
        fdel += 1
    print(f"Deleted {fdel} done. ")
    return


def hashMd5Sha256(fname: str):
    ''' Calc both hashes from one file stream '''
    # print("+", flush=True, end="")
    os.environ['PYTHONHASHSEED'] = '0'  # disable salt
    hash_md5 = hashlib.md5()
    hash_sha256 = hashlib.sha256()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
            hash_sha256.update(chunk)
    fsize=os.stat(fname).st_size
    # print("-", flush=True, end="")
    return fsize, hash_md5.hexdigest(), hash_sha256.hexdigest() , fname


async def run_file_compare( files: classFiles, dir: str, qLog, qInfo) -> None:
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
    max = 100000
    thash = time.time()
    # We can use a with statement to ensure threads are cleaned up promptly
    # max_workers=5 ( Defaut CPU*4 )
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        #  use zip to limit number of files for testing
        future_to_hash = {executor.submit(hashMd5Sha256,os.path.join(f[1][0].pathbase,f[1][0].path)): f for _, f in zip(range(5000), files.items_file())}
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


async def main_run(qFromGui: queue, qToGui: queue) -> None:
    ''' main worker thread uses q's to talk to gui'''
    print("Its main worker.")
    files = classFiles()
    qToGui.put(("lb1",f"Its main worker. START"))
    dirPrimary = os.path.expanduser("~/Pictures/Photos")
    asyncio.ensure_future( readHashFromFile(files=files,
                                filename=dirPrimary+"/hashdeep/20210321-hashdeep.hash.txt",
                                )
                         )
    asyncio.ensure_future(run_file_compare( dir=dirPrimary , files=files,
                          qLog=(qToGui,"lb2"), qInfo=(qToGui,"TextInfo1")
                          ) )
    while not globals.exitFlag:
        try:
            event,values = qFromGui.get_nowait()    # see if something has been posted to Queue
        except queue.Empty:                     # get_nowait() will get exception when Queue is empty
            event = None                      # nothing in queue so do nothing
            await asyncio.sleep(1)
        if event:
            print(f"photoDirSync.py main: qFromGui {event=} {values=} {globals.exitFlag=}")
            if event == "SaveImg":
                qToGui.put(("lb1",f"Got [SaveImg] event in photodirSync.py"))

    print("The end. main().")


if __name__ == '__main__':
    print("ERR photoMain.py is only a module of import") 
    print("The END.  __main__")
