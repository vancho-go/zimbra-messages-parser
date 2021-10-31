import configparser
import re
import random
import zipfile
import os
import shutil
import time
from datetime import datetime
from urllib.parse import unquote

import sys
import logging
import asyncio
import aiofiles
import aiohttp
from aiohttp import ClientSession

# Setup logger
logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("zimbra_script")
logging.getLogger("chardet.charsetprober").disabled = True
logging.getLogger("chardet.universaldetector").disabled = True

# Setup configs
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')
ip = config['Zimbra_Configs']['ip']
login_in_web = config['Zimbra_Configs']['login_in_web']
login_with_dns = config['Zimbra_Configs']['login_with_dns']
password = config['Zimbra_Configs']['password']
dns = config['Zimbra_Configs']['dns']
mode = config['Zimbra_Configs']['mode']
date_from_which_parse_start = config['Zimbra_Configs']['date_from_which_parse_start']
folders = (config['Zimbra_Configs']['folders']).split(', ')

now = datetime.now().strftime('%d%m%y%H%M%S')

download_path = '/home/user/Downloads/'

_current_download_path = download_path + login_with_dns + '/'
_current_download_path_with_date = _current_download_path + '{}_{}'.format(login_with_dns, now) + '/'



# Setup authentification
auth = aiohttp.BasicAuth(login=login_in_web, password=password)


async def get_json(folder, client): 
    #Make request
    url = 'https://{}/home/{}/{}?fmt=json'.format(ip, login_with_dns, folder)
    async with client.request(method = 'GET', url = url, auth = auth) as answer:
        assert answer.status
        logger.info("Got json response [%s] for URL: %s", answer.status, url)     
        if answer.status == 200: 
            return await answer.json()   
        else:
            return {}

async def get_attachment_by_id(loop, folder, client):
    answer_json = await get_json(folder, client)   
    ids_to_timeandsubject = {}

    # Set time
    pattern = '%d/%m/%Y'
    epoch = int(str(int(time.mktime(time.strptime(date_from_which_parse_start, pattern)))) + '000')
    if answer_json:
        for i in range(len(answer_json['m'])-1,-1,-1):
            id = answer_json['m'][i]['id']
            timestamp = answer_json['m'][i]['d']
            subject = answer_json['m'][i]['su']
            if epoch < timestamp:
                ids_to_timeandsubject[id] = [timestamp, subject]
        logger.info('Messages in folder {} from {}: {}'.format(folder, date_from_which_parse_start,len(ids_to_timeandsubject))) 

        for id in ids_to_timeandsubject.keys():
            try:
                if (id > config['Zimbra_Configs']['last_downloaded_attachment_{}_id'.format(folder)]):
                    for j in range (1,10): # 10 - max amount of checked attachments
                        url = 'https://{}/home/{}/?&id={}&part={}'.format(ip, login_with_dns, id, j)
                        async with client.request(method = 'GET', url = url, auth = auth) as answer:
                            assert answer.status
                            data = await answer.read()
                        if answer.headers.get('content-disposition'):
                            try:
                                filename = re.findall('filename=(.+)', answer.headers.get('content-disposition'))[0]
                                if (filename[1:-1] != 'unknown'):
                                    async with aiofiles.open(_current_download_path_with_date + filename[1:-1], 'wb') as f:
                                        await f.write(data)
                                    logger.info("Got attachment [%s] for id: %s", filename[1:-1], id)
                            except:
                                filename = unquote(answer.headers.get('content-disposition'))
                                filename = filename[filename.find('UTF-8''')+7:]
                                async with aiofiles.open(_current_download_path_with_date + filename, 'wb') as f:
                                        await f.write(data)
                                logger.info("Got attachment [%s] for id: %s", filename, id)
                            finally:
                                config.set('Zimbra_Configs', 'last_downloaded_attachment_{}_id'.format(folder), id)
                                with open('config.ini', 'w') as configfile:
                                    config.write(configfile)
                        else:
                            logger.info("No attachments for id: %s", id)
                            break
                else:
                    logger.info("Attachment for id was already downloaded: %s", id)
            except:
                for j in range (1,10): # 10 - max amount of checked attachments
                    url = 'https://{}/home/{}/?&id={}&part={}'.format(ip, login_with_dns, id, j)
                    async with client.request(method = 'GET', url = url, auth = auth) as answer:
                        assert answer.status
                        data = await answer.read()
                    if answer.headers.get('content-disposition'):
                        try:
                            filename = re.findall('filename=(.+)', answer.headers.get('content-disposition'))[0]
                            if (filename[1:-1] != 'unknown'):
                                async with aiofiles.open(_current_download_path_with_date + filename[1:-1], 'wb') as f:
                                    await f.write(data)
                                logger.info("Got attachment [%s] for id: %s", filename[1:-1], id)
                        except:
                            filename = unquote(answer.headers.get('content-disposition'))
                            filename = filename[filename.find('UTF-8''')+7:]
                            async with aiofiles.open(_current_download_path_with_date + filename, 'wb') as f:
                                    await f.write(data)
                            logger.info("Got attachment [%s] for id: %s", filename, id)
                        finally:
                            config.set('Zimbra_Configs', 'last_downloaded_attachment_{}_id'.format(folder), id)
                            with open('config.ini', 'w') as configfile:
                                config.write(configfile)
                    else:
                        logger.info("No attachments for id: %s", id)
                        break
    else:
        logger.info("Got EMPTY json response for folder: %s", folder)    
    

async def get_eml_by_id(loop, folder, client):
    answer_json = await get_json(folder, client)   
    ids_to_timeandsubject = {}

    # Set time
    pattern = '%d/%m/%Y'
    epoch = int(str(int(time.mktime(time.strptime(date_from_which_parse_start, pattern)))) + '000')

    if answer_json:
        for i in range(len(answer_json['m'])-1,-1,-1):
            id = answer_json['m'][i]['id']
            timestamp = answer_json['m'][i]['d']
            subject = answer_json['m'][i]['su']
            if epoch < timestamp:
                ids_to_timeandsubject[id] = [timestamp, subject]
        logger.info('Messages in folder {} from {}: {}'.format(folder, date_from_which_parse_start,len(ids_to_timeandsubject))) 

        for id, time_subject in ids_to_timeandsubject.items():
            try:
                if (id > config['Zimbra_Configs']['last_downloaded_eml_{}_id'.format(folder)]):
                    subject = time_subject[1]
                    url = 'https://{}/home/{}/{}?fmt=zip&id={}'.format(ip, login_with_dns, folder, id)
                    async with client.request(method = 'GET', url = url, auth = auth) as answer:
                        assert answer.status
                        data = await answer.read()
                    filename = re.findall('filename=(.+)', answer.headers.get('content-disposition'))[0]
                    zip_file_path = _current_download_path_with_date + filename[1:-1]
                    async with aiofiles.open(zip_file_path, 'wb') as f:
                        await f.write(data)
                    logger.info("Got eml for id: %s", id)

                    # Extracting zip with eml inside
                    with zipfile.ZipFile(zip_file_path) as zip_file:
                        for member in zip_file.namelist():
                            filename = os.path.basename(member)
                            # Skip directories
                            if not filename:
                                continue
                            # Copy file (taken from zipfile's extract)
                            source = zip_file.open(member)
                            target = open(os.path.join(_current_download_path_with_date, filename), "wb")
                            with source, target:
                                shutil.copyfileobj(source, target)
                            # Renaming eml file
                            try:
                                new_filename = '[{}][{}][{}].eml'.format(folder, subject, random.getrandbits(10))
                            except:
                                new_filename = '[{}][{}][{}].eml'.format(folder, '', random.getrandbits(10))
                            os.rename(os.path.join(_current_download_path_with_date, filename), _current_download_path_with_date+new_filename)            

                    # Removing zip
                    os.remove(zip_file_path)
                    config.set('Zimbra_Configs', 'last_downloaded_eml_{}_id'.format(folder), id)
                    with open('config.ini', 'w') as configfile:
                        config.write(configfile)
                else:
                    logger.info("EML for id was already downloaded: %s", id)
            except:
                subject = time_subject[1]
                url = 'https://{}/home/{}/{}?fmt=zip&id={}'.format(ip, login_with_dns, folder, id)
                async with client.request(method = 'GET', url = url, auth = auth) as answer:
                    assert answer.status
                    data = await answer.read()
                filename = re.findall('filename=(.+)', answer.headers.get('content-disposition'))[0]
                zip_file_path = _current_download_path_with_date + filename[1:-1]
                async with aiofiles.open(zip_file_path, 'wb') as f:
                    await f.write(data)
                logger.info("Got eml for id: %s", id)

                # Extracting zip with eml inside
                with zipfile.ZipFile(zip_file_path) as zip_file:
                    for member in zip_file.namelist():
                        filename = os.path.basename(member)
                        # Skip directories
                        if not filename:
                            continue
                        # Copy file (taken from zipfile's extract)
                        source = zip_file.open(member)
                        target = open(os.path.join(_current_download_path_with_date, filename), "wb")
                        with source, target:
                            shutil.copyfileobj(source, target)
                        # Renaming eml file
                        try:
                            new_filename = '[{}][{}][{}].eml'.format(folder, subject, random.getrandbits(10))
                        except:
                            new_filename = '[{}][{}][{}].eml'.format(folder, '', random.getrandbits(10))
                        os.rename(os.path.join(_current_download_path_with_date, filename), _current_download_path_with_date+new_filename)            

                # Removing zip
                os.remove(zip_file_path)
                config.set('Zimbra_Configs', 'last_downloaded_eml_{}_id'.format(folder), id)
                with open('config.ini', 'w') as configfile:
                    config.write(configfile)
    else:
        logger.info("Got EMPTY json response for folder: %s", folder)    

async def run(loop, folders):
    if not os.path.exists(_current_download_path):
        os.makedirs(_current_download_path)
    if not os.path.exists(_current_download_path_with_date):
        os.makedirs(_current_download_path_with_date)
    for folder in folders:
        async with aiohttp.ClientSession(loop=loop) as client:
            if mode == 'a':
                await get_attachment_by_id(loop, folder, client)
            elif mode == 'e':
                await get_eml_by_id(loop, folder, client)

    # Save last extract date            
    last_extract_try = datetime.now().strftime('%d/%m/%Y %H-%M-%S')
    config.set('Zimbra_Configs', 'last_extract_try', str(last_extract_try))
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

    # Change start parse date if needed
    dt = datetime.today()
    if (dt.day - 1 in [1,7,14,21,28]):
        date_from_which_parse_start_new = datetime.now().strftime('%d/%m/%Y')
        config.set('Zimbra_Configs', 'date_from_which_parse_start', str(date_from_which_parse_start_new))
        with open('config.ini', 'w') as configfile:
            config.write(configfile)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop, folders))
    shutil.make_archive(_current_download_path_with_date, 'zip', _current_download_path_with_date)
    shutil.rmtree(_current_download_path_with_date)