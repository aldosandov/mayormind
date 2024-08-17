import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from loguru import logger


def download_pdf(url, filename, folder="data/raw/"):
    try:
        response = requests.get(url)
        response.raise_for_status()
        os.makedirs(folder, exist_ok=True)
        filepath = os.path.join(folder, filename)
        with open(filepath, "wb") as f:
            f.write(response.content)
        logger.info(f"Downloaded: {filename}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {filename}: {e}")


def download_pdfs_from_csv(csv_file, max_workers=os.cpu_count()):
    df = pd.read_csv(csv_file)
    downloads = 0
    logger.warning('Downloading files from "ojp.puebla.gob.mx" ...')
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for _, row in df.iterrows():
            if row["download"]:
                url = row["url"]
                filename = row["filename"]
                folder = f'data/raw/{row["doc_type"]}'
                futures.append(executor.submit(download_pdf, url, filename, folder))

        for future in as_completed(futures):
            future.result()
            downloads += 1

    logger.success(f"{downloads} files has been downloaded successfully.")


if __name__ == "__main__":
    download_pdfs_from_csv("data/catalog.csv")
