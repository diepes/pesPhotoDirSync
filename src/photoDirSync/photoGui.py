#!/usr/bin/env python3.8
'''
    (c) Pieter Smit 2020 GPL3
    - Scan two dirs (Photos) , generate file hash for each
    - Detect corrupt files, and duplicates
    - Provide cleanup and dir sync two way to preserve Photos

    20200718 Currently only one dir , use hashdeep hash info to find duplicates
        - run hashdeep
'''
from . import globals
try:
    import PySimpleGUI as sg
except ImportError as e:
    print(f"Err: importing PySimleGUI, try pip install ??. {e}")
    exit(1)
import sys
import time
import threading  # run PySimpleGui in own thread, comms through queue
import queue
debug = 0

def gui_run(qFromWorker,qToWorker):
    try:
        window = gui_setup(x=1080,y=1920,scale=0.3)
        logsContent=[]
        listboxContent = { "lb1":[], "lb2":[] }
        event, values = window.read(timeout=10)
        window.Element('lb1').Update(["lb1"])
        window.FindElement('lb2').Update(values=["lb2"])
        while True:
            event, values = window.read(timeout=100)  # wait for up to 100 ms for a GUI event
            if event != "__TIMEOUT__":
                print(f"gui_run: {event=} {values=}")
            if event == sg.WIN_CLOSED or event == 'Exit' or globals.exitFlag == True:
                globals.exitFlag=True
                qToWorker.put( ("gui_state","Gui Quitting."))
                break
            elif event == 'Show':
                # Update the "output" text element to be the value of "input" element
                window['-OUTPUT-'].update(values['-IN-'])
                # In older code you'll find it written using FindElement or Element
                # window.FindElement('-OUTPUT-').Update(values['-IN-'])
                # A shortened version of this update can be written without the ".Update"
                # window['-OUTPUT-'](values['-IN-'])
            elif event != "__TIMEOUT__":
                qToWorker.put( (event,values) )
            # --------------- Read next message coming in from threads ---------------
            try:
                qevent,qvalue = qFromWorker.get_nowait()    # see if something has been posted to Queue
            except queue.Empty:                     # get_nowait() will get exception when Queue is empty
                qevent = None                      # nothing in queue so do nothing
            if qevent == None:
                continue
            elif qevent == "log":
                # print(f"gui got log qvalue={qvalue}")
                logsContent.insert(0, qvalue)
                window.FindElement('logbox').Update(values=logsContent[:20])
                del logsContent[50:]
            elif qevent in ["lb1", "lb2"]:
                # lb1 or 2, qvalues = list of text lines
                if not isinstance(qvalue, list): qvalue = listboxContent[qevent] + [qvalue, ]
                window[qevent].update( values=qvalue )
                ## print(f"photoGui.py update {qevent=} with {qvalue=} {listboxContent[qevent]=}... ")
                listboxContent[qevent] = qvalue
            elif qevent in ["TextInfo1"]:
                window[qevent].update( value=qvalue )
            else:
                print(f"gui_run received qFromWorker msg unknown {qevent=}")
        # 4 - the close
    finally:
        window.close()

def gui_setup(x,y,scale) -> sg.Window:
    #image_elem = sg.Image(data=get_img_data(filename, first=True) , enable_events=True, tooltip="pieter")
    global click_max
    menu_buttons = [ "Login" , "Home", "LaunchG", "SaveImg", "RlMatch" ]
    click_max=4
    click_button_src = [ f for x in range(0,click_max) for f in (sg.Button(f"Click{x+1}",size=(6,1)),sg.Text(f"Coord{x+1}", key=f"TextClick{x+1}", size=(12,1)) )]
    # using list comprehension + zip()
    # to perform custom list split
    split_list = [8,] #list(range(0,click_max,8)) ##Split into rows 4+4
    print(f"{split_list=}")
    click_buttons = [click_button_src[i : j] for i, j in zip([0] + split_list, split_list + [None])]
    col_files = [

        [sg.Button(b, size=(6,1)) for b in menu_buttons],
        # [sg.Button('Auto-on', size=(8,1), button_color=('white', 'green'), key='_auto_'), sg.Text(f"auto on", key="TextAuto", size=(80,1)) ],
        #     click_button_src[0:8],click_button_src[8:],
        #[sg.Button('Login', size=(6, 1)), sg.Button('Home', size=(6,1)), sg.Button('LaunchG', size=(6,1)), sg.Button('Prev', size=(8, 2))],
        [sg.Text('Text TextInfo1', key='TextInfo1', size=(35, 1))],
        [sg.Listbox(values=["a"], change_submits=True, size=(120, 10), key='lb1')],
        [sg.Listbox(values=["b"], change_submits=True, size=(120, 10), key='lb2')],
        [sg.Listbox(values=["Log msg's from q ..."], change_submits=True, size=(140, 10), key='logbox')]
            ]

    layout = [[ sg.Column(col_files),]]
    window = sg.Window('Photo syncer', layout, return_keyboard_events=True,
                location=(0, 0), use_default_focus=False)
    return window
    #from https://pysimplegui.trinket.io/demo-programs#/demo-programs/design-pattern-2-persistent-window-with-updates




if __name__ == '__main__':
    '''Module photoGui based on  PySimpleGui'''
    print("This is a module. The END.  __main__")