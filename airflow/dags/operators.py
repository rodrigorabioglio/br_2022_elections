import pandas as pd

from const import execution_dict

from google.cloud import storage

import requests
import shutil

import os
import re


def download_files_and_unzip(path, filename='temp.zip', **kwargs):
    election_round_code = ['051020221321', '311020221535']
    state = execution_dict[kwargs['ds']]
    filename = f'{state}_{filename}'

    if not path.endswith('/'):
            path = path + '/'

    path = path+state

    os.makedirs(path)

    print(state, path, filename)

    if not path.endswith('/'):
            path = path + '/'

    print(path+filename)

    for i, c in enumerate(election_round_code, 1):

        # defining user-agent to bypass website bot gates
        headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'}
        url = f'https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2022/buweb/bweb_{i}t_{state}_{c}.zip'
        print(url)
        r = requests.get(url=url,headers=headers)   

        with open(path + filename, 'wb') as f:
            f.write(r.content)

        shutil.unpack_archive(path + filename, path)

    os.system(f'rm {path}*.pdf {path}{filename}')


def upload_to_gcs_bucket(local_data_dir, bucket_name, **kwargs):

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)

    #create_folder_for_state
    state = execution_dict[kwargs['ds']]

    blob = bucket.blob(f'{state}/')
    blob.upload_from_string('')

    files_state_directory = f'{local_data_dir}/{state}'

    for f in os.listdir(files_state_directory):
        blob = bucket.blob(f'{state}/{f}')
        blob.upload_from_filename(f'{files_state_directory}/{f}')


def delete_files(path, **kwargs):
    if not path.endswith('/'):
        path = path + '/'

    state = execution_dict[kwargs['ds']]

    path = path+state

    os.system(f'rm -rf {path}')
