'''
    (c) Pieter Smit 2021 GPL3
    Split out the files class.
'''
#from . import photoGui
from . import globals

#import EXIF
##import pyexiv2
# from PIL import Image, ImageTk
# import io
#import sys
import os
#import re
import time
#import asyncio
#import queue
import hashlib


from dataclasses import dataclass, field
@dataclass  # creates boilerplate for class to __init__
class classFile:
    ''' Single file and it's hashes'''
    pathbase: str  # base dir for all files in one location
    path: str      # rel path/filename in pathbase dir
    size: int
    hashmd5: str = ""
    hashsha256: str = ""
    flagCalculatedHash: bool = False
    source: str = "" # optional e.g. hashdeep csv
    def get_filename(self):
        return os.path.join(self.pathbase, self.path)

class classFiles:
    ''' Class to keep track of files and hashes '''
    def __init__(self):
        from typing import Dict, List
        # dictHash [md5-sha256] : [ file, ] list
        # { hash: list(classFile)}  all files for specific hash
        self.dictHashFiles : Dict[str, List[classFile]] = dict()
        # { hash: size }  
        self.dictHashSize  : Dict[str, int]  = dict()
        # dictFile { rel-dir/fname: list(classFile) } #Can have more than one file, in different root-dir's
        self.dictFile : Dict[str, List[classFile] ] = dict()
        # counters

    def __len__(self) -> int:
        return len(self.dictFile)

    def __getitem__(self, key) -> classFile:
        return self.dictFile.get(key, [])

    def __iter__(self):
        return iter(self.dictFile)

    def items_file(self):
        return self.dictFile.items()
    def items_hash(self):
        return self.dictHashFiles.items()
    def item_file(self, key) -> classFile:
        return self.dictFile[key]
    def item_hash(self, key) -> classFile:
        return self.dictHashFiles[key]

    def getHashFileSize(self, hash: str) -> int:
        return self.dictHashSize[hash]

    def getNumDuplicateFiles(self) -> int:
        count = 0
        for k,v in self.dictHashFiles.items():
            count += len(v)-1
        return count       

    def add(self, fileInfo: classFile):
        ''' add a new classFile to classFiles, updating the dictFile, and dictHash '''
        assert fileInfo.path != "." , "can't use . as path"
        filename = os.path.join(fileInfo.pathbase,fileInfo.path)
        if not os.path.exists(filename):
            #sprint(f"Debug: classFiles.add({fileInfo.source=}) non-existent file {filename}")
            raise FileNotFoundError(f"classFiles.add missing file {filename=} {fileInfo.source=}")
        if fileInfo.hashmd5 == "" or fileInfo.hashsha256 == "":
            size = fileInfo.size  #Save size passed in before fs lookup.
            fileInfo.size, fileInfo.hashmd5, fileInfo.hashsha256, getFname = hashMd5Sha256(os.path.join(fileInfo.pathbase,fileInfo.path))
            fileInfo.flagCalculatedHash = True
            assert size == fileInfo.size, f"Error file size changed for {fileInfo.pathbase=} {fileInfo.path=} {getFname=}"
        else:
            fileInfo.flagCalculatedHash = False  # We did not calc hash from FS yet.
        # print(f"{fileInfo.path=} {len(self.dictFile)}")
        # fileInfo.path is relative dir/fname to fileInfo.pathbase
        if fileInfo.path in self.dictFile.keys():
            #
            print(f"Debug: add found dup {fileInfo.path}")
            # make sure this is first time we add this fileInfo path (dir/fname) in specific pathbase
            if all( [(x != fileInfo.pathbase) for x in self.dictFile[fileInfo.path]['pathbase']]):
                # all good add file is in different pathbase than other fileInfos found.
                self.dictFile[fileInfo.path].append(fileInfo)
            else:
                assert False, f"class.files add Duplicate file name added ? {fileInfo.pathbase=} {fileInfo.path=} "
        else:
            #print(f"Debug add {path=}")
            self.dictFile[fileInfo.path] = [ fileInfo, ]
        # Also update the hash dict to find by hash.
        # - calc hashkey from md5-sha256
        assert len(fileInfo.hashmd5) == 32, f"Error md5 length wrong ? {fileInfo.path=}"
        assert len(fileInfo.hashsha256) == 64, f"Error sha256 length wrong ? {fileInfo.path=}"
        hashkey=f"{fileInfo.hashmd5}-{fileInfo.hashsha256}"
        if hashkey in self.dictHashSize.keys():
            #print(f"Debug add: {hashkey=} {len(self.dictHash[hashkey])} {self.dictHash[hashkey].keys()}")
            self.dictHashFiles[hashkey].append(fileInfo)
            #assert size == self.dictHashSize[hashkey], f"Existing file not matching size, but same hash ?? {fileInfo.path=}"
        else:
            self.dictHashFiles[hashkey] = [ fileInfo, ]
            self.dictHashSize[hashkey]  = fileInfo.size


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
                    self.add( classFile(pathbase=pathbase,
                                        path=row[3],
                                        size=row[0],
                                        hashmd5=row[1],
                                        hashsha256=row[2],
                                        flagCalculatedHash=False
                                       )
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




if __name__ == '__main__':
    print("ERR photoMain.py is only a module of import") 
    print("The END.  __main__")
