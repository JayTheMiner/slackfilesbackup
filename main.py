import pandas as pd
import json
import os
import glob
import requests


def find_json_files(base_directory):
    # 모든 하위 디렉토리를 포함하여 .json 파일 경로를 검색
    json_files = glob.glob(os.path.join(
        base_directory, '**', '*.json'), recursive=True)
    return json_files


def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


def get_file_links_from_json(_json):
    for segment in _json:
        if 'url_private_download' in segment:

            id = segment['id']
            filetype = segment['filetype']
            url = segment['url_private_download']

            dict_pair = {
                'id': id,
                'filetype': filetype,
                'url': url,
            }
            list_dfs.append(dict_pair)

        if 'files' in segment:
            for subseg in segment['files']:
                if 'url_private_download' in subseg:
                    id = subseg['id']
                    filetype = subseg['filetype']
                    url = subseg['url_private_download']
                    dict_pair = {
                        'id': id,
                        'filetype': filetype,
                        'url': url,
                    }
                    list_dfs.append(dict_pair)


base_directory = 'data/originaldata'
json_files_dir = find_json_files(base_directory)

# print(json_files[0])


list_dfs = []
for dir in json_files_dir:
    print(dir)
    json_current = read_json_file(dir)
    get_file_links_from_json(json_current)


df_files = pd.DataFrame(list_dfs)


# 파일 저장 경로 설정
save_path = 'data/downloads'
os.makedirs(save_path, exist_ok=True)

# 파일 다운로드 함수


def download_file(row):
    file_url = row['url']
    file_id = row['id']
    file_type = row['filetype']
    file_name = f"{file_id}.{file_type}"
    file_path = os.path.join(save_path, file_name)

    try:
        response = requests.get(file_url, stream=True)
        response.raise_for_status()  # HTTP 에러가 발생하면 예외를 발생시킴

        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Downloaded: {file_name}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {file_name}: {e}")


# 데이터프레임의 각 행에 대해 파일 다운로드
for index, row in df_files.iterrows():
    download_file(row)
