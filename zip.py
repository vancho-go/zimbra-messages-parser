import os, zipfile
from datetime import datetime
import shutil
import random
now = datetime.now()

def zip(mail, folder):
    now = datetime.now()
    day = now.strftime("%d%m%Y %H-%M-%S")
    os.mkdir(mail+day)
    archive_name = mail+day
    zips = []
    for filename in os.listdir("."):
        if filename.endswith(".zip"):
            name = os.path.splitext(os.path.basename(filename))[0]
            if not os.path.isdir(name):
                try:
                    zip = zipfile.ZipFile(filename)
                    zip.extractall(path=archive_name)
                    zips.append(filename)
                except zipfile.BadZipfile as e:
                    print("BAD ZIP: "+filename)
                    try:
                        os.remove(filename)
                    except OSError as e: # this would be "except OSError, e:" before Python 2.6
                        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
                            raise # re-raise exception if a different error occured ```
    base_path = os.getcwd()
     
    rename_files(archive_name, folder)
    os.chdir(base_path)   
    shutil.make_archive(archive_name, 'zip', archive_name)
    for zip in zips:
        os.remove(zip)
    shutil.rmtree(archive_name)


def rename_files(archive_name, folder):
    base_path = os.getcwd()
    archive_name = archive_name+'\\'+ folder
    for file in os.listdir(archive_name):
        os.chdir(base_path)
        file_way = str(os.getcwd())+'\\'+archive_name+'\\'+file
        with open (file_way, 'r') as file:
            a = file.read()
        start = a.find('Subject: ')
        end = a.find('MIME-Version')
        subject = (a[start+9:end-1])
        os.chdir(os.getcwd()+'\\'+archive_name)
        os.rename(file_way, '[{}][{}][{}].eml'.format(folder, subject, random.getrandbits(10)))
