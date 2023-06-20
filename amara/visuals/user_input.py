"""
This module provides functionality for user inputs, such as Multiple Questions
(MCQ), yes/no questions, etc. Handles input validation and provides custom error
messages.
"""


from __future__ import annotations

from abc import ABC, abstractmethod


class _IUserInput(ABC):
    def __init__(self, prompt: str) -> None:
        self._prompt = prompt

    @abstractmethod
    def prompt(self) -> int | str | bool:
        pass

class OptionsList(_IUserInput):
    def __init__(self, prompt: str, options: list[str], indent: int = 0) -> None:
        super().__init__(prompt)
        self.__options = options
        self.__indent = '\t' * indent

    def prompt(self) -> int:
        # loop for bad input
        while True:
            # format prompt
            print(f'{self.__indent}{self._prompt}')
            for i, option in enumerate(self.__options):
                print(f'[{i + 1}] {self.__indent}\t{option}')
            choice = input(f'{self.__indent}>>> ')

            # input validation
            try:
                choice = int(choice)
                if choice < 1 or choice > len(self.__options):
                    raise Exception
                break
                
            except Exception:
                print(f'{self.__indent}Expecting a whole number between 1 and {len(self.__options)}, got "{choice}" instead.')

        return choice
    
class YesNoPrompt(_IUserInput):
    def __init__(self, prompt: str, indent: int = 0) -> None:
        super().__init__(prompt)
        self.__indent = '\t' * indent

        # mend prompt
        if not self._prompt.rstrip().endswith('(y/n):'):
            self._prompt += '(y/n): '

    def prompt(self) -> bool:
        # loop for bad input
        while True:
            # format prompt
            choice = input(f'{self.__indent}{self._prompt}').lower()

            # input validation
            if choice in ('y', 'n'):
                return choice == 'y'

            print(f'{self.__indent}Expecting "y" for yes or "n" for no, got "{choice}" instead.')


