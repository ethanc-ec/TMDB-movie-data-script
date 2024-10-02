import requests
import pandas as pd
from datetime import datetime
import json
import gzip
import io
import collections
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from dotenv import load_dotenv
import os

def flatten(dictionary, parent_key=False, separator='_'):
    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, collections.abc.MutableMapping):
            items.extend(flatten(value, new_key, separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


def process_data(data: dict):
    result = {}  
  
    for key, value in flatten(data).items():
        if isinstance(value, list):
            if len(value) == 0:
                result[key] = "-".join(value)
            elif isinstance(value[0], dict):            
                value_keys = value[0].keys()
                for k in value_keys:
                    result[key + "_" + k] = "-".join([str(i[k]) for i in value])
        
        else:
            result[key] = value
                
    return result

def fetch_data(num: int):
    response = requests.get(url[0] + str(num) + url[1], headers=headers)
    
    if response.status_code == 200:
        return response.json()

    sleep(1)

    return fetch_data(num)
    

if __name__ == "__main__":
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.getenv('TMDB_API_KEY')}"
    }
    cur_time = datetime.today()
    month, day, year = cur_time.month, cur_time.day, cur_time.year

    if len(str(month)) == 1:
        month = "0" + str(month)
    if len(str(day)) == 1:
        day = "0" + str(day)

    all_url = f"http://files.tmdb.org/p/exports/movie_ids_{month}_{day}_{year}.json.gz"

    movies = requests.get(all_url)
    
    data = io.BytesIO(gzip.decompress(movies.content)).read().decode('utf-8').splitlines()
    data = [json.loads(i) for i in data]
    
    df = pd.DataFrame(data)
    df.sort_values(by="id", inplace=True)
    
    url = "https://api.themoviedb.org/3/movie/", "?language=en-US"

    response = requests.get(url[0] + str(df['id'][0]) + url[1], headers=headers).json()

    response['genres'] = '-'.join([i['name'] for i in response.pop("genres")]) 

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda x: fetch_data(x), df['id']), total=df.shape[0])
        
    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(process_data, results), total=df.shape[0])
    
    clean_df = pd.DataFrame(results)
    clean_df.to_csv("movies.csv", index=False)
