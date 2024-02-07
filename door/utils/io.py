import tarfile
import os
import shutil
import bz2
import requests

def move_to_root_folder(folder):
    base = folder
    # traverse root directory, and list directories as dirs and files as files
    for root, dirs, files in os.walk(base):
        path = root.split(os.sep)
        for file in files:
            if not os.path.isdir(file):
                # move file from nested folder into the base folder
                shutil.move(os.path.join(root, file), os.path.join(base, file))
    # Delete all empty folders
    for root, dirs, files in os.walk(base, topdown=False):
        for name in dirs:
            os.rmdir(os.path.join(root, name))

def untar_file(file_name, out_folder = None, mode = "r:bz2", move_to_root= False):
    tar = tarfile.open(file_name, mode)
    if out_folder is None:
        out_folder = os.path.dirname(file_name)
    tar.extractall(out_folder)
    tar.close()
    os.remove(file_name)
    if move_to_root:
        move_to_root_folder(out_folder)

def decompress_bz2(filepath):
    zipfile = bz2.BZ2File(filepath)  # open the file
    data = zipfile.read()  # get the decompressed data
    newfilepath = filepath[:-4]  # assuming the filepath ends with .bz2
    open(newfilepath, 'wb').write(data)

def download_http(url, destination):
    r = requests.get(url)
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    with open(destination, 'wb') as f:
        f.write(r.content)
