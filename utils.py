import zipfile
import os

import pandas as pd


_F1DB_ZIP = 'f1db_csv.zip'
_F1DB_DIR = 'f1db_csv'  # name auto-created by kaggle
NA_VALUES = ['', '\\N']  # recommended for all f1db csv's


csv_id_cols = {
    'circuits.csv': 'circuitId',
    'constructor_results.csv': 'constructorResultsId',
    'constructor_standings.csv': 'constructorStandingsId',
    'constructors.csv': 'constructorId',
    'driver_standings.csv': 'driverStandingsId',
    'drivers.csv': 'driverId',
    'lap_times.csv': None,
    'pit_stops.csv': None,
    'qualifying.csv': 'qualifyId',
    'races.csv': 'raceId',
    'results.csv': 'resultId',
    'seasons.csv': None,
    'status.csv': 'statusId',
}

def load_data(filename, folder=_F1DB_DIR, *, convert_types=True, standard=True, use_id_idx=False):
    r"""Convenience function to load each f1db file.
    
    Ignore default NaN values since they aren't used. Only the
    empty string ('') and `\N`s are used for missing data.
    
    `convert_types=True` will convert_types the columns' dtypes to something appropriate.
    
    `standard=True` will perform some standardisation to the columns
    of each file. Such as creating 'millisecond' columns from time strings.
    
    `use_id_idx=True` will set the index to the ID columns of the DB's,
    while also keeping the column (default False).
    """
    filename += '.csv' if not filename.endswith('.csv') else ''
    data = pd.read_csv(os.path.join(folder, filename), na_values=NA_VALUES, keep_default_na=False)
    
    if filename not in csv_id_cols:
        # might be a new file, return without changes
        return data
    if convert_types:
        data = data.astype(get_type_dict(filename))
    if standard:
        data = standard_data_func(filename)(data)
    if use_id_idx:
        id_column = csv_id_cols[filename]
        if id_column:
            data.set_index(id_column, drop=False, inplace=True)
    return data

def get_type_dict(filename):
    dtypes_data = pd.read_csv('dtypes.csv')
    cols_types = dtypes_data[dtypes_data['file'] == filename][['field', 'dtype']]
    return dict(zip(*cols_types.to_dict(orient='list').values()))

def standard_data_func(filename):
    """looks for local functions with the name `stdrd_<filename>`"""
    func_name = 'stdrd_' + ''.join(l if l.isalnum() else '_' for l in filename[:-4])
    return globals().get(func_name, lambda x: x)

def stdrd_drivers(drivers):
    """Convert Driver DoB's to datetime64's"""
    # if convert_types was False, this will still process for standard=True
    drivers['dob'] = ymd_to_dt(drivers['dob'])
    return drivers

def stdrd_qualifying(qualis):
    """Add Qualifying times in milliseconds"""
    qualis[['q1ms', 'q2ms', 'q3ms']] = qualis.loc[:, 'q1':'q3'].apply(duration_to_ms)
    return qualis

def duration_to_ms(series):
    """Convert a Series of MM:SS.mmm (qualifying times)
    to milliseconds, like `lap_times.csv`
    """
    t = series.str.split(':', expand=True)
    assert len(t.columns) == 2, "Expected duration to split into MM and SS.sss"
    return ((pd.to_numeric(t[0], errors='coerce') * 60 +
             pd.to_numeric(t[1], errors='coerce')) * 1000).round().astype('UInt32')

def ymd_to_dt(series):
    """Convert YYYY-MM-DD to np.datetime64's"""
    return pd.to_datetime(series, yearfirst=True, format='%Y-%m-%d')#.dt.date

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
