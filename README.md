# TMDB Data Fetching Script

## Setup

1. Install `uv` or the packages in `pyproject.toml`
2. Add a `.env` file with the following content:

    ```env
    TMDB_API_KEY=your_api_key
    ```

3. Do `uv run script.py` to run the script
4. Wait for 12+ hours for the script to finish

## Notes

- Due to the API limit of roughly 40 requests per second, the script will take a long time to finish
- There are more than 1.3 million movies in the database, so it is recommended to run this script on a server or secondary machine
