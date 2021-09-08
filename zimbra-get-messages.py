import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import configparser
import re
import time
import random
from datetime import datetime
from zip import zip

#Delete unnecessary logs
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Setup configs
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')
ip = config['Zimbra_Configs']['ip']
port = config['Zimbra_Configs']['port']
login = config['Zimbra_Configs']['login']
password = config['Zimbra_Configs']['password']
dns = config['Zimbra_Configs']['dns']
mode = config['Zimbra_Configs']['mode']
date_from_which_parse_start = config['Zimbra_Configs']['date_from_which_parse_start']
folders = (config['Zimbra_Configs']['folders']).split(', ')

def get_json(folder): 
    #Make request
    url = 'https://{}:{}/home/{}@{}/{}?fmt=json'.format(ip, port, login, dns, folder)
    answer_json = requests.get(url, auth=('{}'.format(login), '{}'.format(password)), verify=False)
    answer_json = answer_json.json()
    return answer_json

def get_eml_by_id(id, folder):
    url = 'https://{}:{}/home/{}@{}/{}?fmt=zip&id={}'.format(ip, port, login, dns, folder, id)
    r = requests.get(url, allow_redirects=True, auth=(login, password), verify=False)
    filename = re.findall('filename=(.+)', r.headers.get('content-disposition'))[0]
    open(filename[1:-1], 'wb').write(r.content)

def get_attachment_by_id(id):
    for j in range (1,10): # 10 - max amount of checked attachments
        url = 'https://{}:{}/home/{}@{}/?&id={}&part={}'.format(ip, port, login, dns, id, j)
        r = requests.get(url, allow_redirects=True, auth=(login, password), verify=False)
        if (r.headers.get('content-disposition')):
            try:
                filename = re.findall('filename=(.+)', r.headers.get('content-disposition'))[0]
                if (filename[1:-1] != 'unknown'):
                    open(filename[1:-1], 'wb').write(r.content)
            except Exception as e:
                open('unknown_file_and_format_(random-{})'.format(random.getrandbits(10)), 'wb').write(r.content)

def check_connection(folder):
    url = 'https://{}:{}/home/{}@{}/{}?fmt=json'.format(ip, port, login, dns, folder)
    answer_json = requests.get(url, auth=('{}'.format(login), '{}'.format(password)), verify=False)
    print('Status for request with folder - {}): {}'.format(folder ,answer_json.status_code))
    return(answer_json.status_code)

def run(folders):
    for folder in folders:
        status = check_connection(folder)
        if status == 200:
            try:
                # Set time
                pattern = '%d/%m/%Y'
                epoch = int(str(int(time.mktime(time.strptime(date_from_which_parse_start, pattern)))) + '000')

                answer_json = get_json(folder)
                ids_to_time = {}
                for i in range(len(answer_json['m'])):
                    id = answer_json['m'][i]['id']
                    timestamp = answer_json['m'][i]['d']
                    if epoch < timestamp:
                        ids_to_time[id] = timestamp
                print('New messages in folder {}: {}'.format(folder, len(ids_to_time)))
                for id, timestamp in ids_to_time.items():
                    if epoch < timestamp:
                        if mode == 'a':
                            get_attachment_by_id(id)
                        elif mode == 'e':
                            get_eml_by_id(id, folder)
                            #Pause
                            time.sleep(3)
                            print('Downloading message with id: {}'.format(id))
                now = datetime.now()
                config.set('Zimbra_Configs', 'last_successful_extract', str(now))
                with open('config.ini', 'w') as configfile:
                    config.write(configfile)
            except:
                print('No folder on zimbra-server named {} or no new messages'.format(folder))
    for folder in folders:
        status = check_connection(folder)
        if status == 200:
            try:
                if(mode=='e'):
                    zip(login+'@'+dns, folder)
            except:
                continue



        
run(folders)