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

        self.__exists = True

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

class ThreadSafeProgressBar:
    """
    Create a thread-safe Progress Base visual in the console, with normal progress bar
    behaviour in parallel processing environments.

    Methods
    -------
    :func:`update`
        Increments the internal step counter of the `SingleProgressBar` object by 1.
    """

    def __init__(self, steps: int | Literal['auto'] = 'auto', bar_length: int = 150, characters: tuple[str, str] = ('░', '▒')) -> None:
        """
        Instantiates an instance of `amara.visuals.progress.ThreadSafeProgressBar`.

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
        >>> def update_loop(tracker):
        ...     tracker.update()
        
        >>> Parallel()(delayed(update_loop)(tracker) for _ in range(100))

        Notes
        -----
        If `steps` is set to auto, ensure that no `ThreadSafeProgressBar.update` calls are in a loop
        as the class will scan the calling script for `ThreadSafeProgressBar.update` calls to derive
        the `steps` number.
        """
        
        self.__steps = steps
        self.__current_step = 0

        self.__characters = characters
        self.__bar_length = bar_length

        self.__tbd_section = [f'{self.__characters[0]}'] * bar_length
        self.__done_section = []

    def update(self):
        """
        Increments the internal step counter of the `SingleProgressBar` object by 1. 
        Prints the next updated progress bar with the new progress visual and 
        percentage.
        """

        # get section length of each update
        update_length = math.ceil(self.__bar_length / self.__steps)

        self.__done_section += [self.__characters[1] * update_length]
        self.__tbd_section = self.__tbd_section[:-update_length]

        print(f'\r{"".join(self.__done_section)}{"".join(self.__tbd_section)}', end='')

        self.__current_step += 1