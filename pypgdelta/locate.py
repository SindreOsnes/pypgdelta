import json
import os
from os import DirEntry
from typing import List, Dict


def find_json_files(root_dir) -> List[DirEntry]:
    """Function for locating json files in a directory

    :param str root_dir: The directory to search

    :return: List of directories
    :rtype: List[DirEntry]
    """
    files = []
    for elem in os.scandir(root_dir):
        if elem.is_dir():
            sub_files = find_json_files(elem.path)
            files.extend(sub_files)
        else:
            if elem.name.endswith('.json'):
                files.append(elem)
    return files


def find_and_read_json(root_dir) -> List[Dict]:
    """Function for locating and readinf json files in a directory

    :param str root_dir: The directory to search

    :return: List of directories
    :rtype: List[DirEntry]
    """

    files = find_json_files(root_dir)
    statements = []
    for file in files:
        with open(file, 'r') as f:
            statements.append(json.load(f))

    return statements
