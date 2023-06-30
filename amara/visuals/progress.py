"""
This module provides functionality for the creation and customization of progress bars
to give end users an indication of the script's progress.
"""


from __future__ import annotations

import os
import math
import inspect
from typing import Literal


class SingleProgressBar:
    """
    Creates a progress bar on a single line in the command prompt. There should be no other printing or 
    logging within the script if this object is used

    Methods
    -------
    :func:`update`
        Increments the internal step counter of the `SingleProgressBar` object by 1.
    """

    def __init__(self, steps: int | Literal['auto'] = 'auto', bar_length: int = 150, characters: tuple[str, str] = ('░', '▒')) -> None:
        """
        Instantiates an instance of `amara.visuals.progress.SingleProgressBar`.

        Parameters
        ----------
        `steps` : `int | Literal['auto']`, `default='auto'`
            Sets the number of steps 
        `bar_length` : `int`, `default=150`
            The length of the bar printed in the command prompt
        `characters` : `tuple[str, str]`, `default=('░', '▒')`
            The characters used to indicate the current progress. The right-side character 
            should indicate completed and vice versa.

        Examples
        --------
        >>> progress = SingleProgressBar(steps=100, bar_length=200, characters=(' ', ']'))

        Notes
        -----
        If `steps` is set to auto, ensure that no `SingleProgressBar.update` calls are in a loop
        as the class will scan the calling script for `SingleProgressBar.update` calls to derive
        the `steps` number.
        """

        self.__steps = steps
        self.__current_step = 0

        self.__bar_length = bar_length
        self.__characters = characters

        # if init is auto, read file and generate steps count
        if steps == 'auto':
            # get class name to find the var name in file
            class_name = type(self).__name__

            # get caller filepath
            frame = inspect.stack()[1]
            path = os.path.abspath(frame[0].f_code.co_filename)

            var_name = None
            steps = 0

            # open and read file
            with open(path, 'r') as file:
                # iterate over lines
                for line in file.readlines():
                    # find var name
                    if var_name is None:
                        if f'{class_name}(' in line:
                            var_name = line.split('=')[0].rstrip()

                    # check each line for [variable_name].update()
                    if f'{var_name}.update(' in line and not line.replace(' ', '').startswith('#'):
                        steps += 1

            # set auto generated step count
            self.__steps = steps

        self.__bar_progress = [math.ceil((i / self.__steps) * self.__bar_length) for i in range(self.__steps)][1:] + [self.__bar_length]
        print(f'\r{"░" * self.__bar_length}  0.00%', end='')

    def update(self) -> None:
        """
        Increments the internal step counter of the `SingleProgressBar` object by 1. 
        Prints the next updated progress bar with the new progress visual and 
        percentage.
        """

        # if past update point, throw warning
        if self.__current_step >= self.__steps:
            raise Exception(f'Step update exceeded steps count.')

        percent_progress = 100 * (self.__current_step + 1) / self.__steps
        done_section = f"{self.__characters[1]}" * self.__bar_progress[self.__current_step]
        tbd_section = f"{self.__characters[0]}" * (self.__bar_length - self.__bar_progress[self.__current_step])

        print(f'\r{done_section}{tbd_section} {percent_progress:.2f}%', end='')
        self.__current_step += 1

        if self.__current_step == self.__steps:
            print()


class MultipleProgressBar:
    def __init__(self,  names: list[str], steps: list[int], bar_length: int = 100, characters: tuple[str, str] = ('▓', '▒')) -> None:
        self._names = names
        self._steps = steps

        self._bar_length = bar_length
        self._characters = characters
        
        self._current_steps = [0] * len(self._names)

        self._current_dones = [''] * len(self._names)
        self._current_undones = [self._characters[1] * self._bar_length] * len(self._names)

        # formatting for names
        self._name_spacing = len(max(self._names, key=len))

        print('\n' * (len(self._names) + 1))
        self._generate_bars()

    @property
    def all_done(self) -> bool:
        return all([self._current_steps[i] == steps for i, steps in enumerate(self._steps)])

    def _generate_bars(self) -> None:
        # prep individual bar lines
        # os.system('cls')
        
        print(f'\033[{len(self._names) + 3}A')
        print(f'┌┬{"─" * (self._name_spacing + 2)}┬{"─" * (self._bar_length + 2)}┬{"─" * 8}┬┐')
        
        # iterate over names
        for i, name in enumerate(self._names):
            # get done and undone sections
            done_chars = round(self._current_steps[i] / self._steps[i] * self._bar_length) * self._characters[0]
            undone_chars = (self._bar_length - len(done_chars)) * self._characters[1]

            print(f'││ {name: >{self._name_spacing}} │ {done_chars}{undone_chars} │ {100 * self._current_steps[i] / self._steps[i]:#.4g}% │')

        print(f'└┴{"─" * (self._name_spacing + 2)}┴{"─" * (self._bar_length + 2)}┴{"─" * 8}┴┘')

    def update(self, index: int) -> None:
        if self._current_steps[index] >= self._steps[index]:
            raise Exception(f'Current step ({self._current_steps[index]}) exceeded max steps ({self._steps[index]}) for bar {self._names[index]}.')
        self._current_steps[index] += 1

        self._generate_bars()
        # if self.all_done: 
        #     sys.exit()

    def update_all(self) -> None:
        for i, _ in enumerate(self._steps):
            if self._current_steps[i] >= self._steps[i]:
                raise Exception(f'Current step ({self._current_steps[i]}) exceeded max steps ({self._steps[i]}) for bar {self._names[i]}.')
            self._current_steps[i] += 1

        self._generate_bars()   
        # if self.all_done:  
        #     sys.exit()
    