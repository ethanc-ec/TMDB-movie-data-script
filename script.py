"""Fetches movie ids from the TMDB API and processes them into a parquet file."""
import collections
import gzip
import io
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from time import sleep

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm


def flatten(dictionary: dict, parent_key: bool = False, separator: str = "_") -> dict:
    """Flatten the keys in a dictionary."""
    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, collections.abc.MutableMapping):
            items.extend(flatten(value, new_key, separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


def process_data(data: dict) -> dict:
    """Flatten the keys and values in each dictionary."""
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

def fetch_data(num: int) -> dict | int:
    """Fetch data from the TMDB API for a given movie id."""
    try:
        response = requests.get(url[0] + str(num) + url[1], headers=headers, timeout=10)

        if response.status_code == 200:
            return response.json()
    except requests.exceptions.RequestException:
        return num

    return num


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    load_dotenv()

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.getenv("TMDB_API_KEY")}"
    }
    cur_time = datetime.today()
    month, day, year = cur_time.month, cur_time.day, cur_time.year

    if len(str(month)) == 1:
        month = "0" + str(month)
    if len(str(day)) == 1:
        day = "0" + str(day)

    all_url = f"http://files.tmdb.org/p/exports/movie_ids_{month}_{day}_{year}.json.gz"

    logger.info("Fetching movie ids")
    movies = requests.get(all_url, timeout=60)

    logger.info("Processing movie ids")
    data = io.BytesIO(gzip.decompress(movies.content)).read().decode("utf-8").splitlines()
    data = [json.loads(i) for i in data]

    movie_ids = pd.DataFrame(data).sort_values(by="id")

    url = "https://api.themoviedb.org/3/movie/", "?language=en-US"

    logger.info("Splitting data")
    chunks = []
    for i in range(0, movie_ids.shape[0], 1000):
        if i + 1000 > movie_ids.shape[0]:
            chunks.append(movie_ids["id"][i:].tolist())
        else:
            chunks.append(movie_ids["id"][i:i+1000].tolist())

    logger.info("Fetching data")
    combined = []
    for idx, val in enumerate(chunks):
        with ThreadPoolExecutor(max_workers=6) as executor:
            results = list(tqdm(
                executor.map(lambda x: fetch_data(x), val),
                total=len(val),
                desc=f"Chunk {idx} of {len(chunks)}",
            ))

            combined.extend(results)

    errors = [i for i in combined if isinstance(i, int)]
    combined = [i for i in combined if not isinstance(i, int)]
    fixed = []
    count = 0

    logger.info(f"Total errors: {len(errors)}")
    logger.info("Processing error nodes")

    while len(errors) > 0 and count < 10:
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(tqdm(
                executor.map(lambda x: fetch_data(x), errors), 
                total=len(errors),
                desc="Retrying",
            ))

        errors = [i for i in results if isinstance(i, int)]
        combined.extend([i for i in results if not isinstance(i, int)])
        count += 1

        sleep(5)

    logger.info(f"Error nodes: {errors}")
    with Path.open("errors.txt", "w") as f:
        f.write(str(errors))

    logger.info("Error nodes processed")
    logger.info("Processing data")

    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(tqdm(
            executor.map(process_data, combined),
            total=len(combined),
            desc="Processing data",
            ))

    logger.info("Data processed")
    logger.info("Saving data")

    clean_df = pd.DataFrame(results)
    clean_df.to_parquet("movies.parquet", index=False)

    logger.info("Data saved")
