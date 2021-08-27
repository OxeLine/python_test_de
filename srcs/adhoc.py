import json
import re

def load_json(filename: str) -> dict:
    """
    Load a json file.

    Args:
    - filename: str. The path of the json file to load.

    Returns:
    - dict. The json loaded.
    """
    with open(filename) as json_file:
        data = json.load(json_file)
    return data

def clean_string(s: str) -> str:
    """
    Clean a string from unwanted characters.

    Args:
    - s: str. The string to clean.

    Returns:
    - str. The string cleaned.
    """
    s = re.sub("\\\\...", "", s)
    return s

def find_most_mention_journal(data: dict):
    """
    Find, in a dict that represent the output of the data pipeline, the journal
    that mention the most differents drugs.

    Args:
    - data: dict. The dict that represent the json of the output.
    """
    result = {}
    for drug in data:
        for mention in data[drug]:
            journal = clean_string(mention["journal"])
            if not result.get(journal):
                result[journal] = []
            if not drug in result[journal]:
                result[journal].append(drug)
    keys = [x for x in result]
    nb_mentions = [len(x) for x in result.values()]
    sorted_names = [x for x, _ in reversed(sorted(zip(keys, nb_mentions), \
        key=lambda pair: pair[1]))]
    sorted_nb = [x for _, x in reversed(sorted(zip(keys, nb_mentions), \
        key=lambda pair: pair[1]))]

    for i, name in enumerate(sorted_names):
        print(F"'{name}' mentioned {sorted_nb[i]} drugs.")

if __name__ == "__main__":
    data = load_json("output.json")
    find_most_mention_journal(data)