"""
This module mostly provides convenience and utility (hence it's name) for other
functions/classes, used to introduce just a bit more modularity.
"""


from __future__ import annotations

import os
from typing import Any
from configparser import ConfigParser


class ConfigFile:
    """
    This class provides a more streamlined connection betweem a config file (.cfg) and
    code, shortening and streamlining workflows.

    Examples
    --------
    Config file format:
    >>> [Section-Name]
    >>> field_1 = data_1
    >>> field_2 = data_2
    """

    def __init__(self, filepath: os.PathLike) -> None:
        """
        Instantiates an instance of `ConfigFile`.

        Parameters
        ----------
        `filepath` : `os.PathLike`
            Filepath to the config file.
        """

        self.__filepath = filepath

        # read config file and store contents
        self.__configparser = ConfigParser()
        self.__configparser.read(self.__filepath)

    @property
    def sections(self) -> list[str]:
        """
        Sections within the config file.
        """

        return list(self.__configparser.keys())[1:]
    
    def get(self, section_name: str) -> dict[str, str]:
        """
        Returns the config data in the `section_name` as a dictionary of string values.

        Parameters
        ----------
        `section_name` : `str`
            Name of the section in the config file to be accessed.

        Returns
        -------
        `dict[str, str]`
            Dictionary of config data.
        """

        return self.__configparser.items(section_name)
    
    @property
    def all(self) -> dict[str, str]:
        """
        Returns all config data in the config file regardless of section.
        """

        for section in self.sections:
            print(self.get(section))
