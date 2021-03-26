import os
import glob
import re

def getFiles(path):
    return iter( glob.glob(f"{path}/*") )

def renameFile(src, dstDir, argv):
    fsd,fsn = os.path.split(src)
    print(f"renameFile: {fsd=} {fsn=}")
    # search fsn for yyyymmdd 
    m = re.search('(?P<y>[0-9]{4})(?P<m>[0-9]{2})(?P<d>[0-9]{2})', fsn)
    fdest = f"{dstDir}/{m.group('y')}"
    # assert that Photos/2017 exist
    assert os.path.isdir(fdest), f" file:{fsn} year:{m.group('y')} missing under {dstDir=}"
    # add date   Photos/2017/20171003
    fdest = f"{fdest}/{m.group('y')}{m.group('m')}{m.group('d')}"
    finddir = glob.glob(f"{fdest}*")
    if len(finddir) > 0:
        fdest = f"{finddir[0]}/{fsn}"
        if len(finddir) > 1 and argv['multidir'] == False:
            print("found more than one dir, and --multidir not set, skip")
            return
        print(f'found dirs using firstone {finddir}')
    else:
        fdest = f"{fdest}-rename"
        print("No matching dir found")
    #print(f"renameTo: {fdest=}")
    print(f"mkdir '{fdest}'")
    #os.mkdir(fdest)
    print(f"mv '{src}' '{fdest}/{fsn}'")
    #os.rename(src, f"{fdest}/{fsn}")
