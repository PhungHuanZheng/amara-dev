"""
This module provides smaller groupings and thresholds used for binning and 
categorizing data in columns. Also used as work-arounds when mapping values
in a column to another value.
"""


import math

room_types = {
    'Deluxe': ['DLXT', 'DLXK'],
    'Executive': ['EXET', 'EXEK'],
    'Premium Executive': ['TROK', 'PEXK'],
    'Club': ['CLBT', 'CLBK'],
    'Suite': ['DLXSUK', 'EXESUK'],
    '1 Bedroom Apartment': ['APT1'],
    '2 Bedroom Apartment': ['APT2']
}

nights_bins = { 
    'Dayuse': (0, 0),
    '1 day': (1, 1), 
    '2 days': (2, 2), 
    '3 - 6 days': (3, 6), 
    '7 - 14 days': (7, 14), 
    '> 14 days': (15, math.inf) 
}

booking_window_bins = {
    'Early Check-In': (-math.inf, -1),
    'Same day': (0, 0),
    '1 day': (1, 1),
    '2 days': (2, 2),
    '3 - 6 days': (3, 7),
    '7 - 13 days': (7, 13),
    '14 - 29 days': (14, 29),
    '> 29 days': (30, math.inf)
}

breakfast_groups = {
    14 + 20 + 18: ['MUSETA'],
    12 + 14.94 + 18: ['MUSETB'],
    14: ['Internal ABF', 'Internal ABF - Bulk Buy'],
    9: ['Club BF'],
    8 + 14 + 12: ['SHN / PCA'],
    2 + 3.5 + 3: ['McDonald PCA'],
    13: ['High Tea'],
    17.5: ['Dayuse - 3 course Set Lunch'],
    39: ['6BEERS']
}

taxes = {
    1.177: (-math.inf, 2022),
    1.188: (2023, 2023),
    1.199: (2024, math.inf)
}