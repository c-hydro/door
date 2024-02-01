import tarfile
import os
import bz2

def untar_file(file_name, out_folder, mode = "r:bz2"):
    tar = tarfile.open(file_name, mode)
    tar.extractall(out_folder)
    tar.close()
    os.remove(file_name)

def decompress_bz2(filepath):
    zipfile = bz2.BZ2File(filepath)  # open the file
    data = zipfile.read()  # get the decompressed data
    newfilepath = filepath[:-4]  # assuming the filepath ends with .bz2
    open(newfilepath, 'wb').write(data)
