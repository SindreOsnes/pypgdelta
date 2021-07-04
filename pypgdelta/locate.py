import os
from os import DirEntry
from typing import List


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
