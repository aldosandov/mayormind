import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger

host = "https://ojp.puebla.gob.mx"
catalog = {
    "leyes": 19,
    "codigos": 8,
    "reglamentos_ley": 242,
    "reglamentos_dependencias": 243,
    "reglamentos_entidades": 244,
    "planes_programas": 240,
    "constitucion": 9,
}


def get_titles_and_urls(host, relative_url, urls=None, filenames=None):
    if urls is None:
        urls = []
    if filenames is None:
        filenames = []

    full_url = requests.compat.urljoin(host, relative_url)

    html = requests.get(full_url).text
    soup = BeautifulSoup(html, "html.parser")

    current_urls = [a["href"] for a in soup.find_all("a", class_="button-download")]
    current_filenames = [
        a["title"] for a in soup.find_all("a", class_="button-download")
    ]

    urls.extend(current_urls)
    filenames.extend(current_filenames)

    next_link = soup.find("a", title="Siguiente")
    if next_link:
        next_relative_url = next_link["href"]

        return get_titles_and_urls(host, next_relative_url, urls, filenames)
    else:
        df = pd.DataFrame.from_dict({"filename": filenames, "url": urls})
        df["url"] = host + df["url"]

        return df


def create_catalog():
    dfs = []
    logger.warning("Starting catalog creation...")
    for key, value in catalog.items():
        url = f"/legislaciondelestado?catid={value}&start=0"
        df = get_titles_and_urls(host, url)
        df["doc_type"] = key
        dfs.append(df)
        logger.info(f'{len(df)} files were found in the "{key}" catalog.')

    final_df = pd.concat(dfs)
    final_df["download"] = ""
    final_df["title"] = (
        final_df["filename"]
        .str.replace(r"_\d+|\.\w+$", "", regex=True)
        .str.replace("_", " ")
    )

    return final_df


if __name__ == "__main__":
    df = create_catalog()
    path = "data/catalog.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    logger.success(f"Catalog saved at {path}")
