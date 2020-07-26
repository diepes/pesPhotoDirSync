#!/usr/bin/env python3.8
'''
    (c) Pieter Smit 2020 GPL3
    - Scan two dirs (Photos) , generate file hash for each
    - Detect corrupt files, and duplicates
    - Provide cleanup and dir sync two way to preserve Photos

    20200718 Currently only one dir , use hashdeep hash info to find duplicates
        - run hashdeep
'''
import PySimpleGUI as sg
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
import PySimpleGUI as sg
import queue
import hashlib
import concurrent.futures  # ThreadPoolExecutor for hash
debug = 0
global_exit=False
#if debug > 0 no rename.
list = {}
extag = ""

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
        return self.dictFile[key]

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
        print(f"{file.path=} {len(self.dictFile)}")
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


def gui_run(qin,qout):
    global global_exit
    try:
        window = gui_setup(x=1080,y=1920,scale=0.3)
        logs=[]
        while True:
            event, values = window.read(timeout=100)  # wait for up to 100 ms for a GUI event
            if event != "__TIMEOUT__":
                print("gui_run:",event, values)
            if event == sg.WIN_CLOSED or event == 'Exit' or global_exit == True:
                global_exit=True
                qout.put( ("gui_state","Gui Quitting."))
                break
            elif event == 'Show':
                # Update the "output" text element to be the value of "input" element
                window['-OUTPUT-'].update(values['-IN-'])
                # In older code you'll find it written using FindElement or Element
                # window.FindElement('-OUTPUT-').Update(values['-IN-'])
                # A shortened version of this update can be written without the ".Update"
                # window['-OUTPUT-'](values['-IN-'])
            elif event != "__TIMEOUT__":
                qout.put( (event,values) )
            # --------------- Read next message coming in from threads ---------------
            try:
                qevent,qvalue = qin.get_nowait()    # see if something has been posted to Queue
            except queue.Empty:                     # get_nowait() will get exception when Queue is empty
                qevent = None                      # nothing in queue so do nothing
            if qevent == "log":
                # print(f"gui got log qvalue={qvalue}")
                logs.insert(0, qvalue)
                window.FindElement('logbox').Update(values=logs[:20])
        # 4 - the close
    finally:
        window.close()

def gui_setup(x,y,scale) -> sg.Window:
    #image_elem = sg.Image(data=get_img_data(filename, first=True) , enable_events=True, tooltip="pieter")
    #Samsung S7 1080x1920
    global click_max
    image_elem = sg.Graph(
            canvas_size=( x*scale, y*scale),
            graph_bottom_left=(0, y),
            graph_top_right=(x, 0),
            key="graph",
            background_color="red",
            enable_events=True,
            change_submits=True, # mouse click events
            drag_submits=False, # mouse drag events (lot of repeat events)
            tooltip="graph",
            right_click_menu= [
                                    [ "Ignored", [ "Print", "Save"]],
                                    [ "&bb"   , ["SendClick", "3rdEntry"]],
                                    [ "c&c"   , ["c1", "c2" ]],
                                ],
            )
    coord_display_elem = sg.Text("latest coordinates", key='TextImg', size=(48, 4))
    # define layout, show and read the form
    col = [
            [image_elem],
            [coord_display_elem],
        ]
    menu_buttons = [ "Login" , "Home", "LaunchG", "SaveImg", "RlMatch" ]
    click_max=4
    click_button_src = [ f for x in range(0,click_max) for f in (sg.Button(f"Click{x+1}",size=(6,1)),sg.Text(f"Coord{x+1}", key=f"TextClick{x+1}", size=(12,1)) )]
    # using list comprehension + zip()
    # to perform custom list split
    split_list = [8,] #list(range(0,click_max,8)) ##Split into rows 4+4
    print(f"{split_list=}")
    click_buttons = [click_button_src[i : j] for i, j in zip([0] + split_list, split_list + [None])]
    col_files = [
        [sg.Listbox(values=[], change_submits=True, size=(110, 20), key='listbox')],
        [sg.Button(b, size=(6,1)) for b in menu_buttons],
        [sg.Button('Auto-on', size=(8,1), button_color=('white', 'green'), key='_auto_'), sg.Text(f"auto on", key="TextAuto", size=(80,1)) ],
            click_button_src[0:8],click_button_src[8:],
        #[sg.Button('Login', size=(6, 1)), sg.Button('Home', size=(6,1)), sg.Button('LaunchG', size=(6,1)), sg.Button('Prev', size=(8, 2))],
        [sg.Text('Text TextInfo1', key='TextInfo1', size=(35, 1))],
        [sg.Listbox(values=["Log msg's from q ..."], change_submits=True, size=(110, 20), key='logbox')]
            ]

    layout = [[ sg.Column(col), sg.Column(col_files),]]
    window = sg.Window('Photo syncer', layout, return_keyboard_events=True,
                location=(0, 0), use_default_focus=False)
    return window
    #from https://pysimplegui.trinket.io/demo-programs#/demo-programs/design-pattern-2-persistent-window-with-updates


def _dir_list( dir_name, whitelist, qLog=None):
    global global_exit
    outputList = []
    count=0
    t=time.time()
    if qLog: qLog.put(("log",f"dir_list dir_name={dir_name}"))
    for root, dirs, files in os.walk(dir_name , topdown=True):
        #if qLog: qLog.put(("log",f"dir_list count={count} @{count/(time.time()-t):.3f}s dir {root}"))
        for f in files:
            count += 1
            if os.path.splitext(f)[1][1:] in whitelist:
                outputList.append(os.path.join(root, f))
            else:
                if qLog and qLog.qsize() < 2: qLog.put_nowait(("log",f"dir_list dir skip qsize={qLog.qsize()} count={count}/{(time.time()-t):.3f}s @{count/(time.time()-t):.3f}/s  t={t}   ext={os.path.splitext(f)[1]} f={os.path.join(root, f)}"))
        if global_exit:
            break
    return outputList

def _dir_scan( dir_name, whitelist, qLog=None) -> classFiles:
    ''' fast, returns os.DirEntry (20200719 double the speed of _dir_list)
    https://docs.python.org/3/library/os.html#os.DirEntry
    '''
    global global_exit
    f = classFiles()
    def scantree(path):
        """Recursively yield DirEntry objects for given directory."""
        for entry in os.scandir(path):
            if entry.is_dir(follow_symlinks=False):
                yield from scantree(entry.path)  # see below for Python 2.x
            else:
                yield entry
    #
    count=0
    t=time.time()
    if qLog: qLog.put(("log",f"dir_scan dir_name={dir_name}"))
    #with os.scandir(dir_name) as it:
    for entry in scantree(dir_name):
            if entry.is_file() and os.path.splitext(entry.name)[1][1:] in whitelist:
                f.add(classFile(pathbase=dir_name, path=entry.path, size=entry.stat(follow_symlinks=False).st_size))
                count += 1
            else:
                if qLog and qLog.qsize() < 2: qLog.put(("log",f"dir_scan dir skip count={count} @{count/(time.time()-t):.3f}s t={t}   ext={os.path.splitext(entry.name)[1]} f={os.path.join(dir_name, entry.name)}"))
                # filepath = entry.path # absolute path
                # filesize = entry.stat().st_size
            if global_exit:
                break
            if count > 50:
                print("Debug call f.save")
                f.save(pathbase=dir_name, fileHashName="/tmp/save-pesDirSyncAB.txt")
                break
    if qLog: qLog.put(("log",f"dir_scan exit count={count} dir_name={dir_name}"))
    return f  # classFiles

async def readHashDeep( filename="./hashdeep/20200719-hashdeep.hash.txt", data=dict(), root="" ):
    import csv
    if not "hash" in data: data["hash"] = dict()
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
                if key in data['hash']:
                    data["hash"][key]["files"].append(row[3])
                    # print("adding duplicate file name")
                else:
                    # Create key Hash, dict with file size, and list of file names
                    data["hash"][key] = { "size": int(row[0]) , "files": [ row[3] , ] }
                # print(counter,data["hash"][key])
    print(f"FIND Duplicates: {len( data['hash'] )} ")
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
    for key in data["hash"]:
        if len( data["hash"][key]["files"] ) > 1:
            if data["hash"][key]["size"] == 0:
                countZeroSize =+ len(data["hash"][key]["files"])
                falldel.extend(data["hash"][key]["files"])
                continue
            #  Duplicate hash
            fdel = []
            fkeep = []
            matchHistory = []
            for f in data["hash"][key]["files"]:
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
            assert (len(fkeep)+len(fdel)) == len(data["hash"][key]["files"]) ,  "BUG lost a file ??"
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
    print("+", flush=True, end="")
    os.environ['PYTHONHASHSEED'] = '0'  # disable salt
    hash_md5 = hashlib.md5()
    hash_sha256 = hashlib.sha256()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
            hash_sha256.update(chunk)
    fsize=os.stat(fname).st_size
    print("-", flush=True, end="")
    return fsize, hash_md5.hexdigest(), hash_sha256.hexdigest() , fname

async def run_file_compare(qOut,dir, qLog=None):
    global global_exit
    print(f"Start ... run_file_compare( dir={dir})")
    t = time.time()
    qOut.put(("log",f"run_file_compare: start..."))
    # tstart=time.time()
    # files=_dir_list( dir, ["gif", "GIF", "jpg" , "JPG", "jpeg", "png", "PNG", "mp4" ] , qLog=qOut);
    # t_dir_list=time.time() - tstart
    # fLen_dir_list=len(files)
    tstart = time.time()
    files =_dir_scan( dir, ["gif", "GIF", "jpg" , "JPG", "jpeg", "png", "PNG", "mp4" ] , qLog=qOut)
    t_dir_scan = time.time() - tstart
    fLen_dir_scan = len(files)
    qOut.put(("log",f"run_file_compare: found {len(files)} in {dir} in {time.time()-t:.2f}s  {len(files)/(time.time()-t):.2f}"))
    print(f"run_file_compare: found {len(files)} files in {dir} in {time.time()-t:.2f}s")
    print(next(iter(files)))
    # print(f"t_dir_list={t_dir_list} fLen_dir_list={fLen_dir_list}     t_dir_scan={t_dir_scan} fLen_dir_scan={fLen_dir_scan}")
    await asyncio.sleep(2)
    print(f"Test hash") ; qOut.put(("log",f"Test hash"))
    hashCount = 0
    max = 10000
    thash = time.time()
    # We can use a with statement to ensure threads are cleaned up promptly
    # max_workers=5 ( Defaut CPU*4 )
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        #  use zip to limit number of files for testing
        future_to_hash = {executor.submit(hashMd5Sha256,os.path.join(f[1][0].pathbase,f[1][0].path)): f for _, f in zip(range(5000), files.items_file())}
        for future in concurrent.futures.as_completed(future_to_hash):
            if global_exit:
                print(f'run_file_compare break on global_exit')
                for ft in future_to_hash:
                    ft.cancel()
                print(f'run_file_compare break on global_exit canceled all')
                break
            f = future_to_hash[future]
            try:
                hashInfo = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (f, exc))
            else:
                # print('%r page is %d bytes' % (f, len(data)))
                hashCount += 1
                if qLog and qLog.qsize() < 2: qLog.put(("log",f".. hashCount={hashCount} time={time.time()-thash:.2f}s {hashInfo}"))

        # for f in files:
        #     hashInfo = await hashMd5Sha256(f)
        #     hashCount += 1
        #     if qLog and qLog.qsize() < 2: qLog.put(("log",f"    .. hashCount={hashCount} time={time.time()-thash:.2f}s {hashInfo}"))
        # if qLog: qLog.put(("log",f"    .. hashCount={hashCount} time={time.time()-thash:.2f}s"))

    print("The End. run_file_compare()")
    #await readHashDeep()

async def main(qIn,qOut, dirL, dirR):
    global global_exit
    print("Its main.")
    asyncio.ensure_future( run_file_compare(qOut,dirL, qLog=qOut) )
    while not global_exit:
        try:
            event,value = qIn.get_nowait()    # see if something has been posted to Queue
        except queue.Empty:                     # get_nowait() will get exception when Queue is empty
            event = None                      # nothing in queue so do nothing
            await asyncio.sleep(1)
        if event:
            print(f"main worker got event={event} value={value} global_exit={global_exit}")

    print("The end. main.")



if __name__ == '__main__':
    #Start PySimpleGui in own thread
    qGui = queue.Queue()
    qWorker = queue.Queue()
    # 1/2 Start gui
    thread_id_gui = threading.Thread(target=gui_run, args=(qGui,qWorker,), daemon=True)
    thread_id_gui.start()

    # 2/2 Start main worker
    loop = asyncio.get_event_loop()
    loop.set_debug(True)  # disable for production
    loop.run_until_complete( main(qIn=qWorker,qOut=qGui, dirL=".", dirR="b") )
    global_exit=True

    print("The END.  __main__")