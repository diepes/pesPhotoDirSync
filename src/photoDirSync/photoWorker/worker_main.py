'''
    (c) Pieter Smit 2020 GPL3
    - Scan two dirs (Photos) , generate file hash for each
    - Detect corrupt files, and duplicates
    - Provide cleanup and dir sync two way to preserve Photos

    20200718 Currently only one dir , use hashdeep hash info to find duplicates
        - run hashdeep
'''
import asyncio
import os
import queue
import time
from dataclasses import dataclass

from . import worker_readHashFromFile
from . import worker_selectDuplicatePrimary
#from . import photoGui
from .. import classFiles, globals
from . import worker_run_file_compare 

async def readHashdeepIntoFiles(files, filename, qToGui, qLabel="lb1"):
    fn_hashdeep = filename
    t_hashdeep = time.time()
    counters = dict()
    await worker_readHashFromFile.readHashFromFile(
                                files=files,
                                filename=fn_hashdeep,
                                c=counters,
                                #20210321-hashdeep.hash.txt",
                                )
    qToGui.put(qLabel,f"hashdeep: from csv {fn_hashdeep} in {time.time()-t_hashdeep:.1f}s found {files.getNumDuplicateFiles()} duplicates ... ")
    print(f"worker_main: Duplicates in files: {files.getNumDuplicateFiles()} ")

    await worker_selectDuplicatePrimary.select_duplicate_primary(
        files=files,
        c=counters,
    )
    qToGui.put(qLabel,f"hashdeep: {counters=}")

async def worker_run(qFromGui: queue, qToGui: queue, files: classFiles) -> None:
    ''' main worker thread uses q's to talk to gui'''
    print("Its photoWorker.worker_main.worker_run().")
    qToGui.put("lb1",f"Its main worker. START")
    dirPrimary = os.path.expanduser("~/Pictures/Photos")

    await readHashdeepIntoFiles(files=files, filename=dirPrimary+"/hashdeep/20211229-hashdeep.hash.txt", qToGui=qToGui, qLabel="lb1")

    asyncio.ensure_future(worker_run_file_compare.run_file_compare( dir=dirPrimary, files=files,
                                            qLog=(qToGui,"lb2"),
                                            qInfo=(qToGui,"TextInfo1"),
                                          )
                         )
    while not globals.exitFlag:
        try:
            event,values = qFromGui.get_nowait()    # see if something has been posted to Queue
        except queue.Empty:                     # get_nowait() will get exception when Queue is empty
            event = None                      # nothing in queue so do nothing
            await asyncio.sleep(1)
        if event:
            print(f"photoDirSync.py main: qFromGui {event=} {values=} {globals.exitFlag=}")
            if event == "SaveImg":
                qToGui.put("lb1",f"Got [SaveImg] event in photoMain.py form qFromGui ")

    print("The end. main().")



if __name__ == '__main__':
    print(f"ERR {__name__} is only a module of import") 
    print("The END.  __main__")
