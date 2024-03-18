import sys
import json
import time
import schedule
import pandas as pd
from os import environ, remove
from pathlib import Path
from ftplib import FTP_TLS

def get_ftp() -> FTP_TLS:
    # Get FTP details
    FTPHOST = environ.get('FTPHOST')
    FTPUSER = environ.get('FTPUSER')
    FTPPASS = environ.get('FTPPASS')

    print(f"Connecting to {FTPHOST}...")

    # Return auntheticated FTP connection
    ftp = FTP_TLS(FTPHOST, FTPUSER, FTPPASS)
    ftp.prot_p()
    print(f"Connected to {FTPHOST}.")
    return ftp

def upload_to_ftp(ftp: FTP_TLS, file_source: Path):
    with open(file_source, "rb") as fp:
        ftp.storbinary(f"STOR {file_source.name}", fp)

def read_csv(config: dict) -> pd.DataFrame:
    url = config["URL"]
    params = config["PARAMS"]
    return pd.read_csv(url, **params)

def delete_all_files_in_directory(directory: Path):
    for file in directory.iterdir():
        if file.is_file():
            file.unlink()

def delete_file(file_source: str | Path):
    remove(file_source)

def pipeline():
    # Load source config
    with open("config.json", "rb") as fp:
        config = json.load(fp)

    ftp = get_ftp()
    data_dir = Path("fdata")
    data_dir.mkdir(parents=True, exist_ok=True)
    delete_all_files_in_directory(data_dir)

    for src_name, src_config in config.items():
        # Create data directory if it doesn't exist
        

        file_name = src_name + ".CSV"
        file_path = data_dir / file_name
        
        # Read CSV and save to file path
        read_csv(src_config).to_csv(file_path, index=False)

        # Upload to FTP
        upload_to_ftp(ftp, file_path)
        print(f"Uploaded {file_name} to FTP.")

        # delete_file(file_path)
        # print(f"Deleted {file_name}.")

if __name__ == "__main__":

    param = sys.argv[1]

    if param=="manual":
        pipeline()
    elif param=="schedule":
        schedule.every().day.at("10:51").do(pipeline)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        print("invalid parameter. Please use 'manual' or 'schedule' as parameter.")
        
        
