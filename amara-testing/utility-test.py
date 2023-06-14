from __future__ import annotations

"""
Testing of classes and functions (general functionality) of non-data-extraction utility within
the amara module. This includes the submodules:
    - core (without core.extraction)
    - datasets
    - visuals
"""


import os
import unittest

from importlib import reload
import sys; sys.path.append(os.path.abspath('../../amara-dev'))
import amara; reload(amara)

import pandas as pd
import numpy as np

from amara.core.wrappers import DirectoryWrapper

from amara.core.googleapi import SheetConnection


RAW_DATA_FOLDER: os.PathLike = '../../Raw Data'
SAMPLE_FILES: dict[str, os.PathLike] = {}
for folder in os.listdir(RAW_DATA_FOLDER):
    # grab files from folder
    files = DirectoryWrapper(os.path.join(RAW_DATA_FOLDER, folder)).files
    if len(files) == 0:
        continue

    # get a sample file
    SAMPLE_FILES[folder] = files[0]


class TestUtility(unittest.TestCase):

    def googleapi_test(self) -> None:
        # creation
        conn = SheetConnection(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'],
                               spreadsheet_id='')
    

if __name__ == '__main__':
    unittest.main()