import zipfile
import os

import pandas as pd


_F1DB_ZIP = 'f1db_csv.zip'
_F1DB_DIR = 'f1db_csv'  # name auto-created by kaggle
NA_VALUES = ['', '\\N']  # recommended for all f1db csv's


def load_data(filename, folder=_F1DB_DIR, *, convert_types='reg_dtype', standard=True, sort=True, use_id_idx=False):
    r"""Convenience function to load each f1db file.
    
    Ignore default NaN values since they aren't used. Only the
    empty string ('') and `\N`s are used for missing data.
    
    `convert_types` will convert_types the columns' dtypes to something appropriate.
    'reg_dtype' are the usual numpy dtypes. 'ext_type' are the newer Pandas
    extension types, which don't have full functional parity yet. hint: idxmax
    
    `standard=True` will perform some standardisation to the columns
    of each file. Such as creating 'millisecond' columns from time strings.
    
    `sort=True` sorts df rows based on order in `meta.csv`.
    
    `use_id_idx=True` will set the index to the ID columns of the DB's,
    while also keeping the column (default False).
    """
    filename += '.csv' if not filename.endswith('.csv') else ''
    data = pd.read_csv(os.path.join(folder, filename), na_values=NA_VALUES, keep_default_na=False)
    meta = pd.read_csv('meta.csv', dtype={'idx_col': 'Int8', 'sort_order': 'Int8'}, index_col='file')
    
    if filename not in meta.index.values:
        # might be a new file, return without changes
        return data
    meta = meta.loc[filename]
    if convert_types:
        # re-read data with the preferred types
        data = pd.read_csv(os.path.join(folder, filename), na_values=NA_VALUES, keep_default_na=False,
                           **get_type_dict(convert_types, meta))
    if standard:
        data = standard_data_func(filename)(data)
    if sort:
        sort_fields = meta[meta['sort_order'].notna()].sort_values('sort_order')['field'].to_list()
        data.sort_values(sort_fields, ignore_index=True, inplace=True)
    if use_id_idx:
        idx_data = meta[meta['idx_col'].notna()].get('field')
        if any(idx_data):
            data.set_index(idx_data.at[filename], drop=False, inplace=True)
    return data

def get_type_dict(typeset, _meta):
    """create a dict to pass to `read_csv` with the column types specified"""
    if typeset not in ('reg_dtype', 'ext_type'):
        raise ValueError("Pick one of: 'reg_dtype', 'ext_type'")
    cols_types = _meta[['field', typeset]]
    
    dtypes = {}
    parse_dates = []
    for col, type_ in zip(*cols_types.to_dict(orient='list').values()):
        if type_.startswith('datetime'):
            parse_dates.append(col)
        else:
            dtypes[col] = type_
    parse_dates = parse_dates or False
    return {'dtype': dtypes, 'parse_dates': parse_dates, 'infer_datetime_format': True}

def standard_data_func(filename):
    """looks for local functions with the name `stdrd_<filename>`"""
    func_name = 'stdrd_' + ''.join(l if l.isalnum() else '_' for l in filename[:-4])
    return globals().get(func_name, lambda x: x)

def stdrd_drivers(drivers):
    """Convert Driver DoB's to datetime64's"""
    # if convert_types was False, this will still process for standard=True
    drivers['dob'] = ymd_to_dt(drivers['dob'])
    return drivers

def stdrd_pit_stops(stops):
    """Convert Driver DoB's to datetime64's"""
    # if convert_types was False, this will still process for standard=True
    stops['time'] = hms_to_dt(stops['time'])
    return stops

def stdrd_qualifying(qualis):
    """Add Qualifying times in milliseconds"""
    qualis[['q1ms', 'q2ms', 'q3ms']] = qualis.loc[:, 'q1':'q3'].apply(duration_to_ms)
    return qualis

def stdrd_results(results):
    results['fastestLapTime_ms'] = duration_to_ms(results['fastestLapTime'])
    return results

def duration_to_ms(series):
    """Convert a Series of [HH]:MM:SS.sss to milliseconds
    (for mixed H:M:S.sss, M:S.sss and S.sss)
    """
    # from https://stackoverflow.com/a/65483372/1431750
    ts = series.astype('string').str.split(':')  # convert to 'string' extension type
    rows = [i[::-1] if i is not pd.NA else [] for i in ts]
    sec_min_hr = pd.DataFrame.from_records(rows).astype('float')
    cols = len(sec_min_hr.columns)
    assert cols <= 3, "Expected duration to split into 'HH', 'MM' and 'SS.sss' but got %d columns" % cols
    multis = [1, 60, 3600][:cols]
    return sec_min_hr.mul(1000).mul(multis).sum(axis=1, min_count=1).round().astype('UInt32')

def ymd_to_dt(series):
    """Convert YYYY-MM-DD to np.datetime64's"""
    return pd.to_datetime(series, yearfirst=True, format='%Y-%m-%d')

def hms_to_dt(series):
    """Convert HH:MM:SS to np.datetime64's
    (sets date to 1900-01-01 but allows the .dt accessor)
    """
    return pd.to_datetime(series, format='%H:%M:%S')

def extract_f1db(f1db_zip=_F1DB_ZIP):
    if not os.path.exists(_F1DB_DIR):
        os.mkdir(_F1DB_DIR)
    with zipfile.ZipFile(f1db_zip) as dbzip:
        for obj in dbzip.infolist():  # should only contain files
            target = os.path.join(_F1DB_DIR, obj.filename)
            with dbzip.open(obj.filename) as fp_z, open(target, 'wb') as fp_w:
                fp_w.write(fp_z.read())
            print('Extracted:', obj.filename, 'to', target)


if __name__ == '__main__':
    extract_f1db()
