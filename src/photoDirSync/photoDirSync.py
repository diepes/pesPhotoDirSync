#!/usr/bin/env python3.8
'''
    (c) Pieter Smit 2020 GPL3
    - Scan two dirs (Photos) , generate file hash for each
    - Detect corrupt files, and duplicates
    - Provide cleanup and dir sync two way to preserve Photos

    20200718 Currently only one dir , use hashdeep hash info to find duplicates
        - run hashdeep
'''
try:
    from . import photoGui
except ImportError as e:
    print(f"Err: fail import photoGui, probably caused by running from inside /src/ dir. {e}")
    exit(1)
except Exception as e:
    #Unknown error , print type.
    print(f"Err:{type(e).__name__} , {e}")
    exit(1)
    

from . import photoMain

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


def run():
    '''Start PySimpleGui in own thread and then asyncio worker main'''
    qGui = queue.Queue()
    qWorker = queue.Queue()
    # 1/2 Start gui
    thread_id_gui = threading.Thread(target=photoGui.gui_run, args=(qGui,qWorker,), daemon=True)
    thread_id_gui.start()

    # 2/2 Start main worker
    loop = asyncio.get_event_loop()
    loop.set_debug(True)  # disable for production
    loop.run_until_complete( photoMain.main_run(qFromGui=qWorker, qToGui=qGui) )
    globals.exitFlag=True

    print("The END. run()")

if __name__ == '__main__':
    run()
    print("The END.  __main__")
