"""
This module provides functionality to interact with the google api from the user's 
Google Account.
"""


from __future__ import annotations

import os.path

import pandas as pd
import numpy as np

import httplib2
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import discovery
from googleapiclient.errors import HttpError


class SheetConnection:
    """
    Handles reading and writing between Google Sheets API and pandas DataFrame
    objects, requires Google Account login and API access.

    Attributes
    ----------
    `data` : `pd.DataFrame`
        Data as pandas DataFrame from the connected google sheet.

    Methods
    -------
    :func:`get_token`
        Accesses and validates the Google API token provided by json filepath.
    :func:`save_dataframe`
        Saves the passed pandas DataFrame object to the connected google sheet.

    See Also
    --------
    :module:`amara.core.googleapi`: Handles Google API Connections.
    """

    range_letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

    def __init__(self, scopes: list[str], spreadsheet_id: str, data_range: str) -> None:
        """
        Instantiates an instance of `amara.core.googleapi.SheetConnection, connecting
        to the Google Sheet and its data specified by the `spreadsheet_id` and `data_range`.

        Parameters
        ----------
        `scopes` : `list[str]`
            Access scopes provided by Google API.
        `spreadsheet_id` : `str`
            Id of the spreadsheet to be accessed, found in the Google Sheet's share link.
        `data_range`: `str`
            Range of spreadsheet data to be accessed in Excel terms, e.g.: `A:E`

        Examples
        --------
        >>> conn = SheetConnection(scopes=['...', '...'], spreadsheet_id='...', data_range='A:BZ')
        """

        self.__scopes = scopes
        self.__spreadsheet_id = spreadsheet_id
        self.__data_range = data_range

        self.__credentials: Credentials = None

    def get_token(self, secrets_file: os.PathLike) -> None:
        """
        Accesses and validates the Google API token provided by json filepath.

        Parameters
        ----------
        `secrets_file`: `os.PathLike | str`
            Path to the `credentials.json` file provided by the Google API granting access to 
            your Google Account.

        Examples
        --------
        >>> conn.get_token('path/to/credentials.json')
        """

        # get folder of secrets_file
        jsons_path = os.path.dirname(secrets_file)
        creds = None

        # check if token.json exists in the same folder
        token_path = os.path.join(jsons_path, 'token.json')
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, self.__scopes)

        # if no token file or invalid creds
        if creds is None or not creds.valid:
            # if creds exist, are expired and has a refresh token
            if creds is not None and creds.expired and creds.refresh_token:
                # refresh and continue
                creds.refresh(Request())

            # if no creds
            else:
                # get creds
                flow = InstalledAppFlow.from_client_secrets_file(secrets_file, self.__scopes)
                creds = flow.run_local_server(port=0)

            # save creds for next valid run
            with open(token_path, 'w') as token:
                token.write(creds.to_json())

        # store credentials to object instance
        self.__credentials = creds

    @property
    def data(self) -> pd.DataFrame:
        """
        Data in Google Sheet accessed as a pandas DataFrame object, uses the first row as the 
        dataframe columns
        """
        # create API Resource Service
        service = discovery.build('sheets', 'v4', credentials=self.__credentials)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=self.__spreadsheet_id, range=self.__data_range).execute()
        values = np.array(result.get('values', []))

        # convert 2D list to DataFrame
        return pd.DataFrame(values[1:], columns=values[0])

    def save_dataframe(self, data: pd.DataFrame, chunk_size: int = 5000) -> None:
        """
        Saves the passed pandas DataFrame object to the connected google sheet. Clears
        the sheet before writing begins

        Parameters
        ----------
        `data` : `pd.DataFrame`
            Pandas DataFrame object to be saved to the sheet
        `chunk_size` : `int`, `default=5000`
            Chunk size to use to write to the Google Sheet. Too big chunk sizes may cause the
            API Connection to timeout.

        Examples
        --------
        >>> conn.save_dataframe(data, chunk_size=1000)
        """
        # create API Resource Service
        service = discovery.build('sheets', 'v4', credentials=self.__credentials)

        # prepare new data
        body = {'values': [data.columns.tolist()] + data.values.tolist()}

        # get range
        range_ = 'A:BZ'

        # get and clear sheet
        sheet = service.spreadsheets()
        sheet.values().clear(spreadsheetId=self.__spreadsheet_id, range=range_, body={}).execute()

        # write column names
        sheet.values().append(spreadsheetId=self.__spreadsheet_id, range=range_, valueInputOption='RAW', body={'values': [data.columns.tolist()]}).execute()

        # write to sheet in chunks
        df_chunk_ids = list(range(len(data)))[::chunk_size] + [None]
        for i, start_id in enumerate(df_chunk_ids[:-1]):
            # get chunk
            end_id = df_chunk_ids[i + 1]
            df_chunk = data[start_id:end_id]

            # build body
            body = {'values': df_chunk.values.tolist()}
            sheet.values().append(spreadsheetId=self.__spreadsheet_id, range=range_, valueInputOption='RAW', body=body).execute()