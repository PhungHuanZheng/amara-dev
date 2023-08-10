"""
This module provides functionality to extract convert Amara formatted Excel reports to 
raw data compatible with the pandas DataFrame object.
"""


from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from itertools import chain
import calendar
from typing import Any

import pandas as pd


def Agilysis_extract_raw_data(data: pd.DataFrame) -> list[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Separates the Agilysis Report provided by FnB into 3 datasets -- Revenue, Department
    and Settlement.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted Agilysis Report as pandas DataFrame

    Returns
    -------
    `pd.DataFrame`
        Revenue section of the Agilysis Report
    `pd.DataFrame`
        Settlement section of the Agilysis Report
    `pd.DataFrame`
        Department section of the Agilysis Report
    
    Examples
    --------
    >>> revenue_df, settlement_df, department_df = Agilysis_extract_raw_data(Agilysis_report)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """
    
    # storage for the 3 dataframes
    agilysis_dfs: list[pd.DataFrame, pd.DataFrame, pd.DataFrame] = []

    # find and extract date of report
    for cell in data.columns:
        try:
            report_date = datetime.strptime(cell, '%d %B %Y')
            break

        except ValueError:
            pass

    """Daily Food and Beverage Report"""
    # extract food and beverage portion (first table), grab the rows between 'Avg Check' and 'Settlement'
    first_col: list = data['Daily Food and Beverage and Other Revenue Report'].values.tolist()
    fnb_df_start = first_col.index('Avg Check')
    fnb_df_end = first_col.index('Settlement')
    fnb_df: pd.DataFrame = data.iloc[fnb_df_start:fnb_df_end]

    # adjust column names and trim df
    fnb_df.columns = fnb_df.iloc[0].values
    fnb_df = fnb_df.iloc[1:]

    # extract where row has non-totalled data
    fnb_df = fnb_df.loc[fnb_df['Meal Period'].notna()]
    fnb_df = fnb_df.loc[fnb_df['Avg Check'].notna()]
    fnb_df.reset_index(drop=True, inplace=True)

    agilysis_dfs.append(fnb_df)


    """Extract joint tables"""
    joint_dfs: pd.DataFrame = data.iloc[fnb_df_end:]
    joint_dfs.columns = joint_dfs.iloc[0].values
    joint_dfs = joint_dfs.iloc[1:]
    joint_dfs.index = joint_dfs['Settlement']
    joint_dfs.drop(['Settlement'], axis=1, inplace=True)
    joint_dfs = joint_dfs[joint_dfs.columns[:joint_dfs.columns.tolist().index('Total')]]


    """Daily Departmental and Settlement Revenue Report"""
    agilysis_dfs.append(pd.DataFrame(joint_dfs.sum(axis=1)).T)
    agilysis_dfs.append(pd.DataFrame(joint_dfs.sum(axis=0)).T)

    agilysis_dfs[2] = agilysis_dfs[2][[col for col in agilysis_dfs[2].columns if pd.notna(col)]]


    """Add 'Date' column to all dataframes"""
    for i, dataframe in enumerate(agilysis_dfs):
        columns = dataframe.columns.tolist()
        dataframe['Date'] = report_date.strftime('%d-%m-%Y')
        agilysis_dfs[i] = dataframe[['Date'] + columns]

    return agilysis_dfs

def FnB_Budget_extract_raw_data(data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Separates the FnB Budget Report provided by FnB into 3 datasets -- PROJECTION, FORECAST
    and ACTUAL.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted FnB Budget Report as pandas DataFrame

    Returns
    -------
    `pd.DataFrame`
        PROJECTION section of the FnB Budget Report
    `pd.DataFrame`
        FORECAST section of the FnB Budget Report
    `pd.DataFrame`
        ACTUAL section of the FnB Budget Report

    Examples
    --------
    >>> PROJECTION_df, FORECAST_df, ACTUAL_df = FnB_Budget_extract_raw_data(FnB_Budget_Report)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """

    # constants
    ROW_HEADER_NAMES = ['FOOD REVENUE', 'FOOD COST', 'FOOD COST %', 'BEVERAGE REVENUE', 'BEVERAGE COST', 'BEVERAGE COST %', 'OTHER INCOME', 'TOTAL REVENUE', 'TOTAL COST', 'TOTAL COST %', 'COVERS', 'AVERAGE CHECK']
    KEEP_HEADERS = ['FOOD COST %', 'BEVERAGE COST %', 'TOTAL COST %', 'AVERAGE CHECK']
    AGGREGATE_HEADERS = [rowH for rowH in ROW_HEADER_NAMES if rowH not in KEEP_HEADERS[:-1]]
    DEPARTMENT_NAMES = ['POOL BAR', 'BANQUET', 'CLUB LOUNGE', 'CAFÉ ORIENTAL', 'ELEMENT', 'ELEMENT OTS', 'MINIBAR', 'ROOM SERVICE', 'SILK ROAD', 'TEA ROOM', 'F&B CONSOLIDATION']
    ACTUAL_DEPARTMENT_NAMES = ['POOL BAR', 'BANQUET', 'Amara Club Lounge', 'CAFE ORIENTAL', 'ELEMENT', 'ELEMENT @ TRAS', 'MiniBar', 'ROOM SVC', 'SILK ROAD', 'TEA ROOM', 'F&B CONSOLIDATION']

    # variable storage
    derived_dfs: dict[str, list[pd.DataFrame]] = {}

    # build df
    template_df = { 'Date': [], 'Department': [], 'FOOD REVENUE': [], 'FOOD COST': [],
                   'FOOD COST %': [], 'BEVERAGE REVENUE': [], 'BEVERAGE COST': [], 
                   'BEVERAGE COST %': [], 'OTHER INCOME': [], 'TOTAL REVENUE': [], 
                   'TOTAL COST': [], 'TOTAL COST %': [], 'COVERS': [], 'AVERAGE CHECK': []}

    # extract "Title" and their indexes from df
    title_start_ids: list[tuple[int, str]] = []
    for i, row in data.iterrows():
        # if has only one non null value in row and row is surrounded by empty lines
        if True in row.notna().values and i != data.shape[0] - 1 and pd.isna(data.iloc[i]['Amara Singapore']) and row.notna().value_counts()[True] == 1:
            # track start id and subdf title
            title_start_ids.append((i, [title for title in row if pd.notna(title)][0]))

    # slice subdfs from main df
    subdfs: list[tuple[str, pd.DataFrame]] = []
    for i, start_tup in enumerate(title_start_ids):
        # if variance subdf, ignore
        if 'VARIANCE' in start_tup[1]:
            continue

        # append subdfs by their start ids
        try:
            subdfs.append((start_tup[1], data.iloc[start_tup[0]:title_start_ids[i + 1][0]]))
        except IndexError:
            subdfs.append((start_tup[1], data.iloc[start_tup[0]:]))

    # mend individual subdfs, last word is year, second last is df type
    for title, subdf in subdfs:
        # subdf.drop([idx for idx in subdf['Amara Singapore'] if ])
        subdf = subdf.dropna(subset='Amara Singapore').fillna(0).reset_index(drop=True)

        # extract info from title
        df_year = int(title.split(' ')[-1])
        df_type = title.split(' ')[-2]

        # init template df
        temp_df = deepcopy(template_df)

        # find start indexes of department names and their df slices
        department_start_ids = subdf.index[subdf['Amara Singapore'].apply(lambda row_header: row_header in DEPARTMENT_NAMES)].tolist()
        department_dfs: dict[str, pd.DataFrame] = {}

        for i, start_id in enumerate(department_start_ids[:-1]):
            # append department dfs by their start ids
            try:
                department_dfs[DEPARTMENT_NAMES[i]] = subdf[start_id:department_start_ids[i + 1]].reset_index(drop=True).iloc[1:].T
            except IndexError:
                department_dfs[DEPARTMENT_NAMES[i]] = subdf[start_id:].reset_index(drop=True).iloc[1:].T

            department_dfs[DEPARTMENT_NAMES[i]].columns = department_dfs[DEPARTMENT_NAMES[i]].iloc[0]
            department_dfs[DEPARTMENT_NAMES[i]].reset_index(drop=True, inplace=True)
            department_dfs[DEPARTMENT_NAMES[i]] = department_dfs[DEPARTMENT_NAMES[i]].iloc[1:-1]

        # build df
        year_date_range = pd.date_range(datetime(df_year, 1, 1), datetime(df_year, 12, 31), freq='D').tolist()
        temp_df['Date'] = year_date_range * len(DEPARTMENT_NAMES[:-1])
        temp_df['Department'] = list(chain.from_iterable([[dept_name] * len(year_date_range) for dept_name in DEPARTMENT_NAMES[:-1]]))

        month_days = [calendar.monthrange(df_year, month)[1] for month in range(1, 13)]
        for row_header in ROW_HEADER_NAMES:
            # if no aggregate change to values
            if row_header in KEEP_HEADERS:
                # explode months into days, welcome to hell but it works :>
                exploded_values = list(chain.from_iterable([list(chain.from_iterable([[value] * month_days[i] for i, value in enumerate(department_dfs[dept_name][row_header])])) for dept_name in DEPARTMENT_NAMES[:-1]]))

            elif row_header in AGGREGATE_HEADERS:
                # explode months into days, welcome to hell but it works :>
                exploded_values = list(chain.from_iterable([list(chain.from_iterable([[value / month_days[i]] * month_days[i] for i, value in enumerate(department_dfs[dept_name][row_header])])) for dept_name in DEPARTMENT_NAMES[:-1]]))

            temp_df[row_header] = exploded_values

        # record df in dict
        rename_dict = dict(zip(DEPARTMENT_NAMES, ACTUAL_DEPARTMENT_NAMES))
        if df_type not in derived_dfs:
            derived_dfs[df_type] = [pd.DataFrame(temp_df).sort_values('Date').reset_index(drop=True)]
        else:
            derived_dfs[df_type].append(pd.DataFrame(temp_df).sort_values('Date').reset_index(drop=True))

    # consolidate dfs within their type groups and sort once more
    for df_type, dfs in derived_dfs.items():
        # ignore if only 1 df
        if len(dfs) == 1:
            derived_dfs[df_type] = dfs[0].replace({'Department': rename_dict})
            continue
        
        derived_dfs[df_type] = pd.concat(dfs).sort_values('Date').reset_index(drop=True).replace({'Department': rename_dict})

    return derived_dfs['PROJECTION'], derived_dfs['FORECAST'], derived_dfs['ACTUAL']

def Forecast_MarketSegment_extract_raw_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts raw data from the 'Forecast_MarketSegment' sheet in the Market Segment
    Occupancy Budget Report.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted Market Segment Forecast Report as pandas DataFrame

    Returns
    -------
    `pd.DataFrame`
        Market Segment Forecast Report as raw data

    Examples
    --------
    >>> df = Forecast_MarketSegment_extract_raw_data(Forecast_MarketSegment_df)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """

    # get separator rows
    separators = data.loc[(data['Unnamed: 0'].notna()) & (data['Inventory'].str.startswith('Market Segment'))]
    sep_ids = separators.index.tolist() + [None]

    # consolidate into single dataframe
    Forecast_MarketSegment_df: list[pd.DataFrame] = []

    # iterate over separators and slice pairs
    for i, sep in enumerate(sep_ids[:-1]):
        # extract df slice
        df = data.iloc[sep:sep_ids[i + 1]]
        df = df.T.iloc[:50].T
        df_date: datetime = df['Unnamed: 0'].iloc[0]

        # set column names
        column_names = df.iloc[0].values
        df.drop(['Unnamed: 0'], axis=1)
        
        # trim from 'Inventory' to 'Total', adjust columns
        trim_end = df['Inventory'].tolist().index('Total ')
        df = df.iloc[1:trim_end]
        df.columns = column_names
        df['Date'] = [df_date.strftime('%d-%m-%Y')] * len(df)

        # trim columns
        trim_end = df.columns.tolist().index('WOW FORECAST ADR VARIANCE') 
        df = df[['Date'] + df.columns.tolist()[1:trim_end + 1]].reset_index(drop=True)

        Forecast_MarketSegment_df.append(df)

    # consolidate under common column names, extract needed columns
    Forecast_MarketSegment_df = pd.concat([pd.DataFrame(df) for df in Forecast_MarketSegment_df])
    Forecast_MarketSegment_df: pd.DataFrame = Forecast_MarketSegment_df[['Date', 'Market Segment ', 'FC RNS', 'FC REVENUE', 'FC ADR', 'BUDGET RNS', 'BUDGET REVENUE', 'BUDGET ADR']]
    Forecast_MarketSegment_df.fillna(0, inplace=True)

    # construct template to explode dates
    template_df = {'Date': [], 'Market Segment': [], 'Forecast RNs': [], 'Forecast Revenue': [], 
                   'Forecast ADR': [], 'Budget RNs': [], 'Budget Revenue': [], 'Budget ADR': []}
    exploded_df = deepcopy(template_df)

    # explode dates
    for i, date in enumerate(Forecast_MarketSegment_df['Date']):
        # grab date data
        date = datetime.strptime(date, '%d-%m-%Y')
        date_data = Forecast_MarketSegment_df.iloc[i].tolist()
        month_days = calendar.monthrange(date.year, date.month)[1]

        # explode into month days
        exploded_df['Date'] += pd.date_range(date, date.replace(day=month_days)).tolist()
        exploded_df['Market Segment'] += [date_data[1]] * month_days

        exploded_df['Forecast RNs'] += [date_data[2] / month_days] * month_days
        exploded_df['Forecast Revenue'] += [date_data[3] / month_days] * month_days
        exploded_df['Forecast ADR'] += [date_data[4]] * month_days
        exploded_df['Budget RNs'] += [date_data[5] / month_days] * month_days
        exploded_df['Budget Revenue'] += [date_data[6] / month_days] * month_days
        exploded_df['Budget ADR'] += [date_data[7]] * month_days

    # convert to df, change date to date string
    exploded_df = pd.DataFrame(exploded_df)
    exploded_df['Date'] = exploded_df['Date'].dt.strftime('%d-%m-%Y')

    return exploded_df

def Forecast_Summary_extract_raw_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts raw data from the 'Forecast_Summary' sheet in the Market Segment
    Occupancy Budget Report.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted Forecast Summary Report as pandas DataFrame

    Returns
    -------
    `pd.DataFrame`
        Forecast Summary Report as raw data

    Examples
    --------
    >>> df = Forecast_Summary_extract_raw_data(Forecast_Summary_df)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """
    
    # grab available rooms
    avail_rooms = [value for value in data['Unnamed: 1'] if isinstance(value, int)]
    avail_rooms = [val for i, val in enumerate(avail_rooms) if i % 2 == 0]
            
    # trim data to exclude quarterly tables, grab pure data
    data = data.T.iloc[1:12].T
    data = data.loc[data['Unnamed: 4'].notna()]

    # grab date from every 5th row
    dates = [[date.strftime('%d-%m-%Y')] * 4 for i, date in enumerate(data['Unnamed: 1']) if i % 5 == 0]
    dates = list(chain.from_iterable(dates))

    # adjust columns
    column_names = data.iloc[0].values[1:]
    data = data.T.iloc[1:].T
    
    # extract rows 
    data = data.iloc[[i for i in range(len(data)) if i % 5 != 0]]
    data.columns = column_names

    # add date and metric columns
    data['Date'] = dates
    data['Metric'] = ['Occupancy', 'Room Nights', 'Revenue', 'ADR'] * int(len(dates) / 4)
    data = data[['Date', 'Metric'] + data.columns.tolist()[:-2]]

    # filter out columns needed
    data = data[['Date', 'Metric', 'Budget']]

    # construct new df with 'Metric' as columns
    df_template = {'Date': [], 'RNs': [], 'Revenue': [], 'ADR': [],
                   'Occ': [], 'RevPAR': []}
    new_df = deepcopy(df_template)

    # construct table and convert to df
    for i, date in enumerate(data['Date'].unique()):
        date_df = data.loc[data['Date'] == date]
        date = datetime.strptime(date, '%d-%m-%Y')

        new_df['Date'].append(date)
        new_df['RNs'].append(date_df.iloc[1]['Budget'])
        new_df['Revenue'].append(date_df.iloc[2]['Budget'])
        new_df['ADR'].append(date_df.iloc[3]['Budget'])
        new_df['Occ'].append(date_df.iloc[0]['Budget'])
        new_df['RevPAR'].append(date_df.iloc[2]['Budget'] / (calendar.monthrange(date.year, date.month)[1] * avail_rooms[i]))

    data = pd.DataFrame(new_df)
    # return data

    # explode dates, remove for new extraction script
    exploded_df = deepcopy(df_template)
    for i, date in enumerate(data['Date']):
        date_data = data.loc[data['Date'] == date].iloc[0].tolist()
        month_days = calendar.monthrange(date.year, date.month)[1]
        
        exploded_df['Date'] += pd.date_range(date_data[0], datetime(date_data[0].year, date_data[0].month, month_days)).tolist()
        exploded_df['RNs'] += [date_data[1] / month_days] * month_days
        exploded_df['Revenue'] += [date_data[2] / month_days] * month_days
        exploded_df['ADR'] += [date_data[3]] * month_days
        exploded_df['Occ'] += [date_data[4]] * month_days
        exploded_df['RevPAR'] += [date_data[5]] * month_days
        
    return pd.DataFrame(exploded_df)

def HMS_Flash_Report_extract_raw_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts raw data from the HMS Flash Report.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted HMS Flash Report as pandas DataFrame

    Returns
    -------
    `pd.DataFrame`
        HMS Flash Report as raw data

    Examples
    --------
    >>> df = HMS_Flash_Report_extract_raw_data(HMS_Flash_Report_df)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """

    # extract date of report
    report_date = datetime.strptime(data.at[2, 'Unnamed: 0'], '%d-%m-%Y %A')
    
    # extract data portion for year of report (first 3 columns)
    raw_data = data[data.columns.tolist()[:4]]
    data_start = raw_data['Unnamed: 0'].tolist().index('Rooms Occupied')
    data_end = raw_data['Unnamed: 0'].tolist().index('ADR excluding Complimentary and House Use')
    raw_data = raw_data.iloc[data_start:data_end].T.iloc[:2]

    # cleanup/adjustment
    raw_data.columns = raw_data.iloc[0].values
    raw_data = raw_data.iloc[[1]]
    raw_data['Date'] = report_date

    # clean up columns and index
    raw_data = raw_data[['Date'] + raw_data.columns.tolist()[:-1]]
    raw_data.reset_index(drop=True, inplace=True)

    return raw_data

def OccupancyStatistic_extract_raw_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts raw data from the Occupancy Statistic Report.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted Occupancy Statistic Report as pandas DataFrame

    Returns
    -------
    `pd.DataFrame`
        Occupancy Statistic Report as raw data

    Examples
    --------
    >>> df = OccupancyStatistic_extract_raw_data(Occupancy_Statistic_Report_df)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """

    # trim df to get rows with non-null 'Date' column
    data = data.loc[data['Unnamed: 1'].notna()]

    # trim column
    data.columns = data.iloc[0].values
    data = data.iloc[1:]

    # cleanup/adjustment
    data = data[data.columns.tolist()[1:]]
    data['Date'] = pd.to_datetime(data['Date'], format='%d-%m-%Y %a').dt.strftime('%d-%m-%Y')

    # separate 'Guests' into 'Adults' and 'Children'
    data['Adults'] = data['Guests'].apply(lambda cell: int(cell.split(' / ')[0]))
    data['Children'] = data['Guests'].apply(lambda cell: int(cell.split(' / ')[1]))
    data.drop(['Guests'], axis=1, inplace=True)
    
    return data

def PnL_extract_raw_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts raw data from the PnL Report raw data sheet. Compatible with the Actual and
    Budget files but NOT the Forecast files.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted PnL Report as pandas DataFrame

    Returns
    -------
    `pd.DataFrame`
        PnL Report as raw data

    Examples
    --------
    >>> df = PnL_extract_raw_data(PnL_df)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """

    # columns values to keep the same
    kept_columns = ['Occupancy', 'Average Room Rate', 'Room Yield ']

    # template df to explode dates
    template_df = {col: [] for col in data}

    # recorded every end of month, per year
    for month in data['Month']:
        # explode dates into daily values
        template_df['Month'] += pd.date_range(month.to_pydatetime().replace(day=1), month).tolist()
        month_days = calendar.monthrange(month.year, month.month)[1]

        # grab subdf of month
        month_df = data.loc[data['Month'] == month].drop(['Month'], axis=1)
        # return month_df

        # iterate over df's columns
        for col in month_df:
            # column values to keep
            if col in kept_columns:
                template_df[col] += month_df[col].tolist() * month_days
                continue

            # column values to average
            template_df[col] += (month_df[col] / month_days).tolist() * month_days
    
    return pd.DataFrame(template_df).rename(columns={'Month': 'Date'})

def STR_extract_raw_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts raw data from the STR Report CONSOLIDATED data sheet. Compatible with all
    hotel reports with dynamic column name generation

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted STR Report as pandas DataFrame

    Returns
    -------
    `pd.DataFrame`
        STR Report as raw data

    Examples
    --------
    >>> df = STR_extract_raw_data(STR_df)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """

    # remove null columns
    data.drop(['Unnamed: 2'], axis=1, inplace=True)
    data.dropna(axis=1, how='all', inplace=True)

    # separate data into Occupancy, ADR and RevPAR
    occupancy_id = data['Unnamed: 3'].tolist().index('Occupancy')
    adr_id = data['Unnamed: 3'].tolist().index('ADR')
    revpar_id = data['Unnamed: 3'].tolist().index('RevPAR')

    occupancy_df = data.iloc[occupancy_id:adr_id]
    adr_df = data.iloc[adr_id:revpar_id]
    revpar_df = data.iloc[revpar_id:]
    
    # adjust create datetime filter to only grab non-totals
    months = [calendar.month_abbr[i] for i in range(1, 13)]
    datestr_filter = lambda cell: any([str(cell).startswith(month) for month in months])
    dates = None

    # operations over 3 sets of dfs
    dataframes = [occupancy_df, adr_df, revpar_df]
    for i, df in enumerate(dataframes):
        # extract and spread columns
        column_rows = df.iloc[:3].T.copy(deep=True)
        column_rows.ffill(inplace=True)
        column_rows = column_rows.T
        for col_name in column_rows.columns[-4:]:
            column_rows.at[column_rows.index[-1], col_name] = ' '

        # generate list of column names by collapsing column name frame
        column_names: list[str] = []
        for col in column_rows:
            # construct column name and separate by ' '
            col_name = []
            for str_ in column_rows[col]:
                col_name.append(str_ if pd.notna(str_) else '') 

            # add new name generated
            column_names.append(" ".join(col_name).strip())

        # set new columns
        dataframes[i].columns = column_names
        dataframes[i] = dataframes[i].iloc[3:]

        # purify and grab date column
        if dates is None:
            dates = dataframes[i].loc[dataframes[i]['Date'].apply(datestr_filter)]['Date'].values

        # filter out 'total' rows
        dataframes[i] = dataframes[i].loc[dataframes[i]['Date'].apply(datestr_filter)]
        dataframes[i] = dataframes[i].drop(['Date'], axis=1).reset_index(drop=True)

    # consolidate dfs side by side, add date column
    consolidated_df = pd.concat(dataframes, axis=1)
    consolidated_df['Date'] = dates
    consolidated_df = consolidated_df[['Date'] + consolidated_df.columns.tolist()[:-1]]

    # fix datetime format
    consolidated_df['Date'] = pd.to_datetime(consolidated_df['Date'], format='%b %Y')
    consolidated_df['Date'] = consolidated_df['Date'].dt.strftime('%d-%m-%Y')

    return consolidated_df

def dStarSummary_extract_raw_data(data: pd.DataFrame, *, hotel = None) -> pd.DataFrame:
    """
    Extracts raw data from the 'Summary {n}' sheet in dStar reports. Compatible
    with all hotel reports.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted dStar Summary Report as a Pandas DataFrame

    Returns
    -------
    `pd.DataFrame`
        dStar Summary Report as raw data
    `hotel` : `str`, `default = None`
        Hotel name/code to be shown in the dataset

    Examples
    --------
    >>> df = dStarSummary_extract_raw_data(dStarSummary_df)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """

    # call by value
    data = data.copy(deep=True)

    # get global sheet data
    branch_name = data['Unnamed: 1'].values[4]
    report_date = data['Unnamed: 4'].values[data.loc[data['Unnamed: 1'] == 'Date Selection'].index[0]]
    report_date = datetime.strptime(report_date, '%Y-%m').strftime('%d-%m-%Y')

    # trim data to show section needed
    data = data.iloc[
        data.loc[data['Unnamed: 1'] == 'Market Summary'].index[0] + 1:
        data.loc[data['Unnamed: 2'] == 'Census/Sample - Properties & Rooms'].index[0] - 1
    ][data.columns[1: 10]].reset_index(drop=True)

    # split data into 'Occupancy', 'ADR' and 'RevPAR'
    split_ids = [None] + data.loc[data['Unnamed: 2'].isna()].index.tolist() + [None]
    subdfs = [data.iloc[idx:split_ids[i + 1]].dropna(how='all') for i, idx in enumerate(split_ids[:-1])]

    # dummy df to populate
    ret_df: dict[str, list[Any]] = {'Date': [report_date] * 3, 'Hotel': [branch_name if hotel is not None else hotel] * 3, 'Metric': []}

    # clean up subdfs
    for i, subdf in enumerate(subdfs):
        # fix column names to differentiate
        column_names = ['Set'] + subdf.iloc[1].values.tolist()[1:]
        column_names = [f'{column_names[j - 1]} {name}' if '% Chg' in name else name for j, name in enumerate(column_names)]
        ret_df['Metric'].append(subdfs[i]['Unnamed: 2'].values[0])

        # set column names and trim
        subdfs[i].columns = column_names
        subdfs[i] = subdfs[i].iloc[2:].reset_index(drop=True)

        # populate dummy df
        for j, market in enumerate(subdfs[i]['Set']):
            for col in subdfs[i].columns[1:]:
                # get and set column name
                ret_col_name = f'{market} {col}'
                if ret_col_name not in ret_df:
                    ret_df[ret_col_name] = []

                # append value
                ret_df[f'{market} {col}'].append(subdfs[i][col].values[j])

    return pd.DataFrame(ret_df).reset_index(drop=True)

def dStarMonthly_extract_raw_data(data: pd.DataFrame, *, hotel: str = None) -> pd.DataFrame:
    """
    Extracts raw data from the 'Monthly {n}' sheet in dStar reports. Compatible
    with all hotel reports.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted dStar Monthly Report as a Pandas DataFrame
    `hotel` : `str`, `default = None`
        Hotel name/code to be shown in the dataset

    Returns
    -------
    `pd.DataFrame`
        dStar Monthly Report as raw data

    Examples
    --------
    >>> df = dStarMonthly_extract_raw_data(dStarMonthly_df)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """
    
    # call by value
    data = data.copy(deep=True)

    # get global sheet data
    report_date = data['Unnamed: 4'].values[data.loc[data['Unnamed: 1'] == 'Date Selection'].index[0]]
    report_date = datetime.strptime(report_date, '%Y-%m').strftime('%d-%m-%Y')

    # trim data to show related sections
    data = data.iloc[:data.loc[data['Unnamed: 1'] == 'Date Selection'].index[0]].dropna(how='all', axis=1)
    averages_col_id = data.columns.tolist().index(data.iloc[1][data.iloc[1] == 'Averages'].index[0])
    data.drop(data.columns[averages_col_id:], axis=1, inplace=True)

    # split data into 'Occupancy', 'ADR' and 'RevPAR'
    split_ids = [i for i in range(1, len(data) - 1) if all([
        pd.isna(data['Unnamed: 1'].iloc[i - 1]),
        pd.notna(data['Unnamed: 1'].iloc[i]),
        pd.isna(data['Unnamed: 1'].iloc[i + 1])])
    ] + [None]
    subdfs = [data.iloc[idx:split_ids[i + 1]] for i, idx in enumerate(split_ids[:-1])]

    # dummy df to populate
    ret_df = pd.DataFrame()

    # clean up subdfs
    for i, _ in enumerate(subdfs):
        # clean up and track metric
        subdfs[i] = subdfs[i].dropna(how='all').reset_index(drop=True)
        metric_name = subdfs[i]['Unnamed: 1'].values[0]

        # split into raw value and % change dataframes
        sub_split_ids = subdfs[i].loc[subdfs[i]['Unnamed: 1'].isna()].index.tolist() + [None]
        raw_values, pc_change = [subdfs[i].iloc[idx:sub_split_ids[j + 1]].T.reset_index(drop=True) for j, idx in enumerate(sub_split_ids[:-1])]

        # clean up raw_values extracted
        raw_values.columns = raw_values.iloc[0]
        raw_values = raw_values.iloc[1:].rename(columns={raw_values.columns[1]: 'Date'})
        raw_values.drop(raw_values.columns[0], axis=1, inplace=True)

        # clean up pc_change extracted
        pc_change.columns = pc_change.iloc[0]
        pc_change = pc_change.iloc[1:].rename(columns={pc_change.columns[1]: 'Date'})
        pc_change.drop(pc_change.columns[0], axis=1, inplace=True)
        pc_change.rename(columns=dict(zip(pc_change.columns[1:],
                                          pc_change.columns[1:] + ' % Chg')), inplace=True)

        # merge dfs and add extra info
        for col in pc_change.columns[1:]:
            raw_values[col] = pc_change[col]

        raw_values.insert(1, 'Metric', [metric_name] * len(raw_values))
        if hotel is not None:
            raw_values.insert(1, 'Hotel', [hotel] * len(raw_values))

        ret_df = pd.concat([ret_df, raw_values])

    return pd.DataFrame(ret_df).reset_index(drop=True)

def dStarDaily_extract_raw_data(data: pd.DataFrame, *, hotel: str = None) -> pd.DataFrame:
    """
    Extracts raw data from the 'Daily {n}' sheet in dStar reports. Compatible
    with all hotel reports.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Formatted dStar Daily Report as a Pandas DataFrame
    `hotel` : `str`, `default = None`
        Hotel name/code to be shown in the dataset

    Returns
    -------
    `pd.DataFrame`
        dStar Daily Report as raw data

    Examples
    --------
    >>> df = dStarDaily_extract_raw_data(dStarDaily_df)

    See Also
    --------
    :func:`amara.core.extraction` : data extraction from reports module
    """

    # call by value
    data = data.copy(deep=True)

    # trim data to show related sections
    data = data.iloc[3:data.loc[data['Unnamed: 1'] == 'Date Selection'].index[0]].dropna(how='all', axis=1)
    data.reset_index(drop=True, inplace=True)

    # split data into 'Occupancy', 'ADR' and 'RevPAR'
    split_ids = data.loc[(data['Unnamed: 1'].notna()) & (data['Unnamed: 3'].isna())].index.tolist() + [None]
    subdfs = [data.iloc[idx:split_ids[i + 1]].iloc[1:-1] for i, idx in enumerate(split_ids[:-1])]

    # dummy df to populate
    ret_df = pd.DataFrame()

    for i, _ in enumerate(subdfs):
        subdfs[i] = subdfs[i].reset_index(drop=True)
        metric_name = subdfs[i]['Unnamed: 1'].values[0]

        # split into raw value and % change dataframes
        sub_split_ids = [None] + subdfs[i].loc[subdfs[i]['Unnamed: 1'].isna()].index.tolist() + [None]
        raw_values, pc_change, indexes = [subdfs[i].iloc[idx:sub_split_ids[j + 1]].T.reset_index(drop=True) for j, idx in enumerate(sub_split_ids[:-1])]

        # clean up raw_values extracted
        raw_values.columns = raw_values.iloc[0]
        raw_values = raw_values.iloc[1:].dropna(how='all', axis=1).reset_index(drop=True)
        raw_values.rename(columns={raw_values.columns[0]: 'Date'}, inplace=True)

        # clean up pc_change extracted
        pc_change.columns = pc_change.iloc[0]
        pc_change = pc_change.iloc[1:].dropna(how='all', axis=1).reset_index(drop=True)
        pc_change.rename(columns={pc_change.columns[0]: 'Date'}, inplace=True)
        pc_change.rename(columns=dict(zip(
            pc_change.columns[1:],
            pc_change.columns[1:] + ' % Chg'
        )), inplace=True)

        # clean up indexes extracted
        indexes.columns = indexes.iloc[0]
        indexes = indexes.iloc[1:].dropna(how='all', axis=1).reset_index(drop=True)
        indexes.rename(columns={indexes.columns[0]: 'Date'}, inplace=True)

        # merge dataframes
        raw_values = raw_values.merge(pc_change, 'inner', on='Date')
        raw_values = raw_values.merge(indexes, 'inner', on='Date')
        raw_values.insert(1, 'Metric', [metric_name] * len(raw_values))
        ret_df = pd.concat([ret_df, raw_values])

    # add hotel name if given
    if hotel:
        ret_df.insert(1, 'Hotel', [hotel] * len(ret_df))

    return ret_df.reset_index(drop=True)