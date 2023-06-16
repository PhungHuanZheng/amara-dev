"""
This file serves as a template and example on the usage of the various data
extraction and processing functions and classes provided by the module. This
template contains a full showcase of such functionality, and also acts as a 
convenient source to copy paste (Ctrl C Ctrl V) from.
"""


# Imports
from __future__ import annotations

import os
from joblib.parallel import Parallel, delayed

"""
Add amara directory to system path and import. Optionally if you're using the 
Visual Studio Code editor, consider adding this:
{
    "python.analysis.extraPaths": [
        "./amara-dev"
    ]
}
to your .vscode/settings.json folder in your working directory to enable syntax
highlighting 
"""
from importlib import reload
import sys; sys.path.append('../amara-dev')
import amara; reload(amara)

# import data extraction functions
from amara.core.extraction import *

# import specific dataset processing functions
from amara.datasets.Events import WeightedOccupancyCalendar, single_date_event_map
from amara.datasets.Info_HMS_Raw_Arrivals import mend_arrival_departure_dates
from amara.datasets.Master_Calendar import MasterCalendar


def main():
    """Agilysis Files"""


if __name__ == '__main__':
    main()