'''
    (c) Pieter Smit 2020 GPL3
    - Scan two dirs (Photos) , generate file hash for each
    - Detect corrupt files, and duplicates
    - Provide cleanup and dir sync two way to preserve Photos

    20200718 Currently only one dir , use hashdeep hash info to find duplicates
        - run hashdeep

    This module:
      1. Setup queue's msgQueue for front ack com's
      2. Start gui (in own thread)
      3. photoWorker
'''
try:
    from . import photoGui
except ImportError as e:
    print(f"Err: fail import photoGui, probably caused by running from inside /src/ dir. {e}")
    exit(1)
except Exception as e:
    #Unknown error , print type.
    print(f"Err[photoDirSync.py]:{type(e).__name__} , {e}")
    exit(1)

from . import photoWorker
from . import globals
from . import classFiles

import time
import asyncio
import threading  # run PySimpleGui in own thread, comms through queue
from . import msgQueue
# For debug add tracemalloc - better async tracebacks.
import tracemalloc
tracemalloc.start()

def start_photoGui_thread(qGui, qWorker):
    # 1/2 Start gui (thread)
    thread_id_gui = threading.Thread(target=photoGui.gui_run, args=(qGui,qWorker,), daemon=True)
    thread_id_gui.start()


def run():
    ''' Start PySimpleGui in own thread and then asyncio worker 
        Work done in photoMain thread, and display updates sent through queue to photoGui
    '''
    qGui = msgQueue.Q(name="qGui")
    qWorker = msgQueue.Q(name="qWorker")
    start_photoGui_thread(qGui=qGui, qWorker=qWorker)

    files = classFiles.classFiles()  # Keeps photo file info, and hash etc.
    #run_main_worker_until_done(qGui, qWorker, files)
    asyncio.run( photoWorker.worker_run(qFromGui=qWorker, qToGui=qGui, files=files) )

    globals.exitFlag=True

    print("The END. run()")

if __name__ == '__main__':
    run()
    print("The END.  __main__")
