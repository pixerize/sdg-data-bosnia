import os
import glob
import pandas as pd
import numpy as np
import yaml
import yamlmd

sdmx_compatibility = True

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
        'year_end': 2020,
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
        'year_end': 2020,
    },
    'SDG 17': {
        'goal': 17,
        'disaggregations': ['Reporting Type','Type of speed','Type of product'],
        'year_start': 2000,
        'year_end': 2019,
    },
}

things_to_translate = {}

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


def drop_these_columns():
    # These columns aren't useful for some reason.
    return [
        # This only had 1 value in the source data.
        'Reporting Type',
        # This only had 1 value in the source data.
        'Level/Status',
        # These are in the metadata.
        'SDG target',
        'SDG indicator',
    ]


def get_column_name_changes():
    changes = {
        # These serve specific purposes in Open SDG.
        'Unit': 'UNIT_MEASURE',
        'Series': 'SERIES',
        # These changes are for compatibility with SDMX.
    }
    sdmx_changes = {
        'Sex': 'SEX',
        'Age': 'AGE',
        'Location': 'URBANISATION',
        'Quantile': 'INCOME_WEALTH_QUANTILE',
        'Education level': 'EDUCATION_LEV',
        'Activity': 'ACTIVITY',
        'IHR Capacity': 'COMPOSITE_BREAKDOWN',
        'Mode of transportation': 'COMPOSITE_BREAKDOWN',
        'Name of international institution': 'COMPOSITE_BREAKDOWN',
        'Name of non-communicable disease': 'COMPOSITE_BREAKDOWN',
        'Type of occupation': 'OCCUPATION',
        'Type of product': 'PRODUCT',
        'Type of skill': 'COMPOSITE_BREAKDOWN',
        'Type of speed': 'COMPOSITE_BREAKDOWN',
        'Parliamentary committees': 'COMPOSITE_BREAKDOWN',
        'Reporting Type': 'Reporting Type',
        'Level/Status': 'Level/Status',
    }
    if sdmx_compatibility:
        changes.update(sdmx_changes)

    for key in changes:
        changed = changes[key]
        if changed not in things_to_translate:
            things_to_translate[changed] = {}
        if changed == 'COMPOSITE_BREAKDOWN':
            comp_breakdown_label = key.replace(' ', '_').replace('-', '_').lower()
            things_to_translate[changed][comp_breakdown_label] = key
        else:
            things_to_translate[changed][changed] = changed
    return changes
# run it right away
get_column_name_changes()

def clean_disaggregation_value(value, column=''):
    if pd.isna(value):
        return ''
    if value.strip() == '':
        return ''
    fixed = value
    conversions = {}
    if column == 'Age':
        conversions = {
            'ALL': 'ALL AGE',
            '<5y': '<5Y',
        }
    if sdmx_compatibility:
        if column == 'Location':
            conversions = {
                'ALL AREA': '', # Instead of _T
                'RURAL': 'R',
                'URBAN': 'U',
            }
        if column == 'Age':
            conversions = {
                'ALL': '', # Instead of _T
                'ALL AGE': '', # Instead of _T
                '15-19': 'Y15T19',
                '15-24': 'Y15T24',
                '15-25': 'Y15T25', # custom
                '15-26': 'Y15T26', # custom
                '15-49': 'Y15T49',
                '15+': 'Y_GE15',
                '18+': 'Y_GE18',
                '<18Y': 'Y0T17',
                '<1M': 'M0',
                '<1Y': 'Y0',
                '14-Feb': '14-Feb', # SDMX mapping needed!
                '20-24': 'Y20T24',
                '25+': 'Y_GE25',
                '30-70': 'Y30T70',
                '<5Y': 'Y0T4',
                '<5y': 'Y0T4',
                '46+': 'Y_GE46',
                '2-14': 'Y2T14',
            }
        if column == 'Sex':
            conversions = {
                'FEMALE': 'F',
                'MALE': 'M',
                'BOTHSEX': '', # Instead of _T
            }
        if column == 'Mode of transportation':
            conversions = {
                'RAI': 'MOT_RAI',
                'ROA': 'MOT_ROA',
                'IWW': 'MOT_IWW',
                'SEA': 'MOT_SEA',
            }
        if column == 'Name of international institution':
            conversions = {
                'ECOSOC': 'IO_ECOSOC',
                'IBRD': 'IO_IBRD',
                'IFC': 'IO_IFC',
                'IMF': 'IO_IMF',
                'UNGA': 'IO_UNGA',
                'UNSC': 'IO_UNSC',
            }
        if column == 'Name of non-communicable disease':
            conversions = {
                'CAN': 'NCD_CNCR',
                'CAR': 'NCD_CARDIO',
                'RES': 'NCD_CRESPD',
                'DIA': 'NCD_DIABTS',
            }
        if column == 'IHR Capacity':
            conversions = {
                'IHR01': 'IHR_01',
                'IHR02': 'IHR_02',
                'IHR03': 'IHR_03',
                'IHR04': 'IHR_04',
                'IHR05': 'IHR_05',
                'IHR06': 'IHR_06',
                'IHR07': 'IHR_07',
                'IHR08': 'IHR_08',
                'IHR09': 'IHR_09',
                'IHR10': 'IHR_10',
                'IHR11': 'IHR_11',
                'IHR12': 'IHR_12',
                'IHR13': 'IHR_13',
                'SPAR01': 'SPAR_01',
                'SPAR02': 'SPAR_02',
                'SPAR03': 'SPAR_03',
                'SPAR04': 'SPAR_04',
                'SPAR05': 'SPAR_05',
                'SPAR06': 'SPAR_06',
                'SPAR07': 'SPAR_07',
                'SPAR08': 'SPAR_08',
                'SPAR09': 'SPAR_09',
                'SPAR10': 'SPAR_10',
                'SPAR11': 'SPAR_11',
                'SPAR12': 'SPAR_12',
                'SPAR13': 'SPAR_13',
            }
        if column == 'Quantile':
            conversions = {
                '_T': '', # Instead of _T
            }
        if column == 'Type of occupation':
            conversions = {
                'DENT': 'ISCO08_2261',
                'NURS': 'ISCO08_2221_3221',
                'NURSMID': 'ISCO08_222_322',
                'PHAR': 'ISCO08_2262',
                'PHYS': 'ISCO08_221',
            }
        if column == 'Type of product':
            conversions = {
                'AGR': 'AGG_AGR',
                'ALP': 'ALP', # SDMX mapping needed!
                'ARM': 'AGG_ARMS',
                'BIM': 'BIM', # SDMX mapping needed!
                'CLO': 'AGG_CLTH', # Clothing?
                'COL': 'COL', # SDMX mapping needed!
                'CPR': 'CPR', # SDMX mapping needed!
                'CRO': 'CRO', # SDMX mapping needed!
                'FEO': 'FEO', # SDMX mapping needed!
                'FOF': 'FOF', # SDMX mapping needed!
                'GAS': 'GAS', # SDMX mapping needed!
                'GBO': 'GBO', # SDMX mapping needed!
                'IND': 'AGG_IND',
                'MEO': 'MEO', # SDMX mapping needed!
                'NFO': 'NFO', # SDMX mapping needed!
                'NMA': 'NMA', # SDMX mapping needed!
                'NMC': 'NMC', # SDMX mapping needed!
                'NMM': 'NMM', # SDMX mapping needed!
                'OIL': 'AGG_OIL',
                'PET': 'MF421', # Petroleum?
                'TEX': 'AGG_TXT',
                'WCH': 'WCH', # SDMX mapping needed!
                'WOD': 'MF13', # Wood?
                'MAZ': 'CPC2_1_112', # Maize?
                'RIC': 'CPC2_1_113', # Rice?
                'SOR': 'CPC2_1_114', # Sorghum?
                'WHE': 'CPC2_1_111', # Wheat?
            }
        if column == 'Education level':
            conversions = {
                'LOWSEC': 'ISCED11_2',
                'PRIMAR': 'ISCED11_1',
                'UPPSEC': 'ISCED11_3',
            }
        if column == 'Type of skill':
            conversions = {
                'SKILL MATH': 'SKILL_MATH',
                'SKILL READ': 'SKILL_READ',
                'SOFT': 'SKILL_ICTSFWR',
                'TRAF': 'SKILL_ICTTRFF',
                'CMFL': 'SKILL_ICTCMFL',
                'PCPR': 'PCPR', # SDMX mapping needed!
                'EPRS': 'EPRS', # SDMX mapping needed!
                'EMAIL': 'EMAIL', # SDMX mapping needed!
                'COPA': 'COPA', # SDMX mapping needed!
                'ARSP': 'ARSP', # SDMX mapping needed!
            }
        if column == 'Type of speed':
            conversions = {
                '256KT2MBPS': 'IS_256KT2M',
                '2MT10MBPS': 'IS_2MT10M',
                '10MBPS': 'IS_GE10M',
                'ANYS': '', # Instead of _T
            }
        if column == 'Activity':
            conversions = {
                'ISIC4_A': 'ISIC4_A',
                'NONAGR': 'ISIC4_BTU',
                'TOTAL': '', # Instead of _T
            }
        if column == 'Parliamentary committees':
            conversions = {
                'FOR_AFF': 'PC_FOR_AFF',
                'DEFENCE': 'PC_DEFENCE',
                'FINANCE': 'PC_FINANCE',
                'HUM_RIGH': 'PC_HUM_RIGH',
                'GEN_EQU': 'PC_GEN_EQU',
            }
    if value in conversions:
        fixed = conversions[value]
    fixed_column = get_column_name_changes()[column]
    if fixed_column not in things_to_translate:
        things_to_translate[fixed_column] = {}
    things_to_translate[fixed_column][fixed] = fixed
    return fixed


def clean_metadata_value(column, value):
    if pd.isna(value):
        return ''
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
    if pd.isna(unit) or unit == '':
        return ''
    fixes = {}
    sdmx_fixes = {
        '% (PERCENT)': 'PT',
        '$ (USD)': 'USD',
        'MILLIONS': 'MILLIONS', # Would actually be UNIT_MULT
        'THOUSANDS': 'THOUSANDS', # Would actually be UNIT_MULT
        'INDEX': 'IX',
        'PER 100000 LIVE BIRTHS': 'PER_100000_LIVE_BIRTHS',
        'PER 1000 LIVE BIRTHS': 'PER_1000_LIVE_BIRTHS',
        'PER 1000 UNINFECTED POPULATION': 'PER_1000_UNINFECTED_POP',
        'PER 100000 POPULATION': 'PER_100000_POP',
        'PER 1000  POPULATION': 'PER_1000_POP',
        'LITRES': 'LITRES', # SDMX mapping needed! LITRES_PURE_ALCOHOL?
        'PER 1000 POPULATION': 'PER_1000_POP',
        "'PER 10000 POPULATION": 'PER_10000_POP',
        'RATIO': 'RO',
        'SCORE': 'SCORE',
        'USD/m3': 'USD_PER_M3',
        'KMSQ': 'KM2',
        'M M3 PER ANNUM': 'M_M3_PER_YR',
        'MJPER GDP CON PPP USD': 'MJ_PER_GDP_CON_PPP_USD',
        'W PER CAPITA': 'W_PER_CAPITA',
        'TONNES': 'T', # Metric tons?
        'KG PER CON USD': 'KG_PER_CON_USD',
        'CUR LCU': 'CUR_LCU',
        'PER 100000 EMPLOYEES': 'PER_100000_EMP',
        'CON USD': 'CON_USD',
        '%': 'PT',
        'METONS': 'T', # Metric tons?
        'T KM': 'T_KM',
        'P KM': 'P_KM',
        'TONNES M': 'T', # Metric tons?
        'PER 1000000 POPULATION': 'PER_1000000_POP',
        'mgr/m^3': 'GPERM3', # micrograms per m3?
        'CU USD B': 'CU USD B', # SDMX mapping needed!
        'HA TH': 'HA TH', # SDMX mapping needed! Hectares?
        'CUR LCU M': 'CUR LCU M', # SDMX mapping needed! CUR_LCU?
        'PER 100 POPULATION': 'PER_100_POP',
        'CU USD': 'CU USD', # SDMX mapping needed! USD?
    }
    if sdmx_compatibility:
        fixes.update(sdmx_fixes)
    fixed = unit
    if unit in fixes:
        fixed = fixes[unit]
    if 'UNIT_MEASURE' not in things_to_translate:
        things_to_translate['UNIT_MEASURE'] = {}
    things_to_translate['UNIT_MEASURE'][fixed] = fixed
    return fixed


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
        # Set up dynamic graph titles by series.
        if 'graph_titles' not in metadata[indicator_id]:
            metadata[indicator_id]['graph_titles'] = {}
        metadata[indicator_id]['graph_titles'][row['Series']] = True

for indicator_id in data:
    slug = indicator_id.replace('.', '-')
    data_path = os.path.join('data', 'indicator_' + slug + '.csv')
    df = data[indicator_id]
    for column in drop_these_columns():
        if column in df.columns:
            df = df.drop(columns=[column])
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.dropna(subset=['Value'], how='all')
    df = df.dropna(axis='columns', how='all')
    df = df.rename(columns=get_column_name_changes())
    non_value_columns = df.columns.tolist()
    non_value_columns.pop(non_value_columns.index('Value'))
    df = df.drop_duplicates(subset=non_value_columns)

    # Rearrange the columns.
    cols = df.columns.tolist()
    cols.pop(cols.index('Year'))
    cols.pop(cols.index('Value'))
    cols = ['Year'] + cols + ['Value']
    df = df[cols]

    df.to_csv(data_path, index=False)

    # Fix the special "graph_titles" metadata field we added above.
    graph_titles = []
    for series in metadata[indicator_id]['graph_titles']:
        graph_titles.append({
            'series': 'SERIES.' + series,
            'title': 'SERIES.' + series,
        })
    metadata[indicator_id]['graph_titles'] = graph_titles

    meta_path = os.path.join('meta', slug + '.md')
    meta = yamlmd.read_yamlmd(meta_path)
    for field in metadata[indicator_id]:
        meta[0][field] = metadata[indicator_id][field]
    yamlmd.write_yamlmd(meta, meta_path)

#print(things_to_translate)