import zipfile

_F1DB_ZIP = 'f1db_csv.zip'

def extract_f1db(f1db_zip=_F1DB_ZIP):
    with zipfile.ZipFile(f1db_zip) as dbzip:
        for obj in dbzip.infolist():  # should only contains files
            with dbzip.open(obj.filename) as fp_z, open(obj.filename, 'wb') as fp_w:
                fp_w.write(fp_z.read())
            print('Extracted:', obj.filename)

if __name__ == '__main__':
    extract_f1db()
