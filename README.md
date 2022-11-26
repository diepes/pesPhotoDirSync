# pesPhotoDirSync
2022-11 v 0.0.3 split out msgQueue.py
2021-12 v 0.0.2 sudo apt-get install python3-tk
2020-07 v 0.0.1  gui broken
## uses python venv
### Created with
 * apt-get install python3-venv
 * python3 -m venv venv

### Activate with 
source venv/bin/activate
./run.py

### Hashdeep file checksum's
 - generate file hashes with ```~/Pictures/Photos/hashdeep/go-do-hashdeep.sh```
   * hashdeep -rle  Photos/ > $(date +"./Photos/hashdeep/%Y%m%d-hashdeep.hash.txt")
     * HASHDEEP file contains comma delimited, "size,md5,sha256,name"

### Highlevel pesPhotoDirSync
 1. Start 2 threads, one for gui, and one for app, communicating through 2xqueue's
 2. Read hashdeep file, and build data structure in obj(classFiles) containing a classFile for each file.
    * classFiles
       * dictFile { fileRelDir/fileName: classFile }
       * dictHashFiles { hash: list(classFile)}
       * dictSizeFiles { hash: size }