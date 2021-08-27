# test_python_de

import pandas as pd
import json

from typing import Iterable
from genpipes import declare, compose
from datetime import datetime

# Using 'genpipes', a library that allow the creation of data pipelines
# (1st use).

@declare.generator()
@declare.datasource(inputs=["./datas/drugs.csv"])
def datas_drugs(path: str) -> pd.DataFrame:
    """
    Datasource (1st data input): Open and return the csv file given.
    By default use the drugs.csv file in the pipeline.

    Args:
    - path: str. The path of the csv file.

    Returns:
    - pd.DataFrame of the csv file.
    """
    df = pd.read_csv(path)
    return df

@declare.datasource(inputs=["./datas/pubmed.csv"])
def datas_pubmed(path: str) -> pd.DataFrame:
    """
    Datasource: Open and return the csv file given.
    By default use the pubmed.csv file in the pipeline.

    Args:
    - path: str. The path of the csv file.
    
    Returns:
    - pd.DataFrame of the csv file.
    """
    df = pd.read_csv(path)
    return df

@declare.datasource(inputs=["./datas/clinical_trials.csv"])
def datas_clinical(path: str) -> pd.DataFrame:
    """
    Datasource: Open and return the csv file given.
    By default use the clinical_trials.csv file in the pipeline.

    Args:
    - path: str. The path of the csv file.
    
    Returns:
    - pd.DataFrame of the csv file.
    """
    df = pd.read_csv(path)
    return df

def find_in_column(df: pd.DataFrame, column_name: str, drug_name: str) \
    -> pd.Series:
    """
    In the DataFrame, return the indexes of the row that contains the
    drug_name in the column 'column_name'

    Args:
    - df: pd.DataFrame. The DataFrame you want to search in.
    - column_name: str. In which column of the DataFrame to search.
    - drug_name: str. The drug_name you want to find occurences of.

    Returns:
    - pd.Series of the row indexes.
    """
    res = df[column_name].str.lower().str.find(drug_name.lower(), 0)
    res = res[res >= 0]
    return res

def format_date(str_date: str) -> str:
    """
    Take a date and return it in the format dd/mm/yyyy
    (if the input format is known).

    Args:
    - str_date: str. The input date.

    Returns:
    - str of the new formated date.
    """
    if " " in str_date:
        return datetime.strptime(str_date, "%d %B %Y").strftime("%d/%m/%Y")
    elif "-" in str_date:
        return datetime.strptime(str_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    return str_date

def add_mention(result_json: dict, found_idxs: pd.Series, df: pd.DataFrame, \
    column_name: str, pub_type: str, drug_name: str):
    """
    Add a mention of a drug (journal name, date, publication type) to the
    dict that will be save as json.

    Args:
    - result_json: dict. The dict that store the results.
    - found_idxs: pd.Series. Indexes of the rows of the DataFrame that contains
    the drug name.
    - df: pd.DataFrame. The DataFrame that contains the data to extract.
    - column_name: str. In which column of the DataFrame the search was done.
    - pub_type: str. The type of publication (ie: pubmed, scientific, ...).
    - drug_name: str. The name of the drug.
    """
    if found_idxs.empty:
        return
    if not result_json.get(drug_name):
        result_json[drug_name] = []
    for idx in found_idxs.index:
        result_json[drug_name].append({
            "journal": df["journal"][idx],
            "date": format_date(df["date"][idx]),
            pub_type: df[column_name][idx]
        })

def save_json(result_json: dict, filename: str):
    """
    Save the dict given as json file.

    Args:
    - result_json: dict. The dict to save as a json file.
    - filename: str: The name for the json file.
    """
    with open(filename, 'w') as outfile:
        json.dump(result_json, outfile, indent=4)

@declare.processor()
def seek_references(stream: Iterable[pd.DataFrame]):
    """
    Process the data to find references of a drug in other csv files.

    Args:
    - stream: Iterable[pd.DataFrame]. Take the data return by the generator.

    Yield:
    - dict that represent the links between drugs and their mentions
    in publications.
    """
    df_pubmed = datas_pubmed()
    df_clinical = datas_clinical()
    result_json = {}
    for df in stream:
        for drug in df["drug"]:
            pubmed_found_idxs = find_in_column(df_pubmed, "title", drug)
            clinical_found_idxs = \
                find_in_column(df_clinical, "scientific_title", drug)
            add_mention(result_json, pubmed_found_idxs, df_pubmed, \
                "title", "pubmed", drug)
            add_mention(result_json, clinical_found_idxs, df_clinical, \
                "scientific_title", "scientific", drug)
        yield result_json

@declare.processor()
def save_as_json(stream: Iterable[dict], filename: str):
    """
    Save the process data in a json file.

    Args:
    - stream: Iterable[dict]. The links between drugs and their mentions.
    - filename: str. The name for the json file. 

    Yield:
    - dict that represent the links between drugs and their mentions
    in publications.
    """
    for j in stream:
        save_json(j, filename)
        yield j

pipeline = compose.Pipeline(steps=[
    ("Fetching datas from csv file", datas_drugs, {}),
    ("Find the references of the drug in the other files", seek_references, {}),
    ("Save the json given", save_as_json, {"filename": "output.json"})
])

if __name__ == "__main__":
    output = pipeline.run()