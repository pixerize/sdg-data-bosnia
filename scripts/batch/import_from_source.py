import os
import glob
import pandas as pd
import numpy as np
import yaml
import yamlmd

path = 'SDG_Indicators_Global_BIH_oct_2020_EN.xls'
start_cols = [
    'SDG target',
    'SDG indicator',
    'Series',
    'Unit',
]
end_cols = [
    'Comments',
    'Sources',
    'Links',
    'Custodian agency',
    'Link to the global metadata (1) of this indicator:',
    'Link to the global metadata (2) of this indicator:',
]

# Hardcoded some details about the source data, to keep this script simple.
sheet_info = {
    'SDG 1': {
        'goal': 1,
        'disaggregations': ['Location','Age','Reporting Type','Sex'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 2': {
        'goal': 2,
        'disaggregations': ['Reporting Type','Age','Sex','Type of product'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 3': {
        'goal': 3,
        'disaggregations': ['Reporting Type','Age','Sex','Name of non-communicable disease','Type of occupation', 'IHR Capacity'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 4': {
        'goal': 4,
        'disaggregations': ['Reporting Type','Education level','Quantile','Sex','Type of skill','Location'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 5': {
        'goal': 5,
        'disaggregations': ['Reporting Type','Age','Sex'],
        'year_start': 2000,
        'year_end': 2020,
    },
    'SDG 6': {
        'goal': 6,
        'disaggregations': ['Reporting Type','Location'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 7': {
        'goal': 7,
        'disaggregations': ['Reporting Type','Location'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 8': {
        'goal': 8,
        'disaggregations': ['Reporting Type','Activity','Sex','Age','Type of product'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 9': {
        'goal': 9,
        'disaggregations': ['Reporting Type','Mode of transportation'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 10': {
        'goal': 10,
        'disaggregations': ['Reporting Type','Name of international institution','Type of product'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 11': {
        'goal': 11,
        'disaggregations': ['Reporting Type','Location'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 12': {
        'goal': 12,
        'disaggregations': ['Reporting Type','Type of product'],
        'year_start': 2000,
        'year_end': 2020,
    },
    'SDG 13': {
        'goal': 13,
        'disaggregations': ['Reporting Type'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 14': {
        'goal': 14,
        'disaggregations': ['Reporting Type'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 15': {
        'goal': 15,
        'disaggregations': ['Reporting Type','Level/Status'],
        'year_start': 2000,
        'year_end': 2020,
    },
    'SDG 16': {
        'goal': 16,
        'disaggregations': ['Reporting Type','Sex','Age','Parliamentary committees','Name of international institution'],
        'year_start': 2000,
        'year_end': 2019,
    },
    'SDG 17': {
        'goal': 17,
        'disaggregations': ['Reporting Type','Type of speed','Type of product'],
        'year_start': 2000,
        'year_end': 2019,
    },
}

strip_from_values = ['<', 'NaN', 'NA', 'fn', 'C', 'A', 'E', 'G', 'M', 'N', ',']
def clean_data_value(value):
    if value == '-':
        return pd.NA
    for strip_from_value in strip_from_values:
        value = value.replace(strip_from_value, '')
    value = value.strip()
    if value == '':
        return pd.NA
    test = float(value)
    return value


def clean_disaggregation_value(value, column=''):
    conversions = {}
    if column == 'Age':
        conversions = {
            'ALL': 'ALL AGE',
            '<5y': '<5Y',
        }
    if column == 'Sex':
        conversions = {
            'Female': 'FEMALE',
            'Male': 'MALE',
        }
    if value in conversions:
        return conversions[value]
    return value


def clean_metadata_value(column, value):
    return value.strip()


def convert_metadata_column(column):
    conversions = {
        'Comments': 'comments',
        'Sources': 'source_organisation_1',
        'Links': 'source_url_1',
        'Custodian agency': 'un_custodian_agency',
        'Link to the global metadata (1) of this indicator:': 'goal_meta_link',
        'Link to the global metadata (2) of this indicator:': 'goal_meta_link_2',
    }
    return conversions[column]


def get_indicator_id(indicator_name):
    return indicator_name.strip().split(' ')[0]


def get_indicator_name(indicator_name):
    return ' '.join(indicator_name.strip().split(' ')[1:])


def clean_series(series):
    series = series.strip()
    # Weird space character.
    series = series.replace(' ', ' ')
    # Some have line breaks.
    if '\n' in series:
        series = series.split('\n')[-1]
    # Finally return the last word.
    series = series.split(' ')[-1]
    return series


def clean_unit(unit):
    fixes = {}
    if unit in fixes:
        return fixes[unit]
    return unit


data = {}
metadata = {}

for sheet in sheet_info:
    print('Processing sheet: ' + sheet)
    info = sheet_info[sheet]
    year_cols = [str(year) for year in range(info['year_start'], info['year_end'] + 1)]
    columns = start_cols + info['disaggregations'] + year_cols + end_cols
    converters = { year: clean_data_value for year in year_cols }
    converters['Series'] = clean_series
    converters['Unit'] = clean_unit
    df = pd.read_excel(path,
        sheet_name=sheet,
        usecols=columns,
        names=columns,
        skiprows=[0, 1],
        na_values=['-'],
        converters = converters
    )
    for col in info['disaggregations']:
        df[col] = df[col].apply(clean_disaggregation_value, column=col)

    # Fill in the merged cells.
    df['SDG target'] = df['SDG target'].fillna(method='ffill')
    df['SDG indicator'] = df['SDG indicator'].fillna(method='ffill')
    df['Series'] = df['Series'].fillna(method='ffill')
    # Drop rows without data.
    df = df.dropna(subset=year_cols, how='all')
    for index, row in df.iterrows():

        # Convert the data.
        data_df = pd.melt(row.to_frame().transpose(),
            id_vars=start_cols + info['disaggregations'],
            value_vars=year_cols,
            var_name='Year',
            value_name='Value'
        )

        indicator_id = get_indicator_id(row['SDG indicator'])
        if indicator_id not in data:
            data[indicator_id] = data_df
            metadata[indicator_id] = {}
        else:
            data[indicator_id] = pd.concat([data[indicator_id], data_df])

        # Convert the metadata.
        for col in end_cols:
            if pd.isna(row[col]):
                continue
            if col not in metadata[indicator_id]:
                value = clean_metadata_value(col, row[col])
                key = convert_metadata_column(col)
                metadata[indicator_id][key] = value
        # Add a few more metadata values.
        metadata[indicator_id]['sdg_goal'] = str(info['goal'])
        metadata[indicator_id]['reporting_status'] = 'complete'
        metadata[indicator_id]['indicator_number'] = indicator_id
        if 'source_organisation_1' in metadata[indicator_id]:
            metadata[indicator_id]['source_active_1'] = True

for indicator_id in data:
    slug = indicator_id.replace('.', '-')
    data_path = os.path.join('data', 'indicator_' + slug + '.csv')
    df = data[indicator_id]
    df = df.drop(columns=['SDG target', 'SDG indicator', 'Reporting Type'])
    df = df.dropna(subset=['Value'], how='all')
    df = df.rename(columns={'Unit': 'Units'})
    df = df.dropna(axis='columns', how='all')

    # Rearrange the columns.
    cols = df.columns.tolist()
    cols.pop(cols.index('Year'))
    cols.pop(cols.index('Value'))
    cols = ['Year'] + cols + ['Value']
    df = df[cols]

    df.to_csv(data_path, index=False)

    meta_path = os.path.join('meta', slug + '.md')
    meta = yamlmd.read_yamlmd(meta_path)
    for field in metadata[indicator_id]:
        meta[0][field] = metadata[indicator_id][field]
    yamlmd.write_yamlmd(meta, meta_path)
