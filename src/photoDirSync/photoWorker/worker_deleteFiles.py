import os
async def deleteFiles(falldel, c):
    print(f" deleteFiles , {c['stillDup']=} {len(falldel)=}")
    c['fdel']=0
    c['fdel_missing']=0
    for f in falldel:
        #print(f"{f=}")
        fname = f.get_filename()
        if os.path.isfile(fname):
            os.remove(fname)
            c['fdel'] += 1
        else:
            print(f"Error - delfile missing {fname=} {type(f)=}")
            c['fdel_missing'] += 1
    print(f"Deleted {c['fdel']}, missing {c['fdel_missing']} done. ")
    return c