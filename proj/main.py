import os
import sys
import json
import logging
import pandas as pd
import pymysql
import dropbox

from datetime import datetime
from timeit import default_timer as timer
from modules import data_processing, dropbox_upload
from tqdm import tqdm

def read_secrets(file_path=r'C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\proj\secrets.json'):
    with open(file_path) as s:
        secrets = json.load(s)

    return secrets

def config(id):
    archive_name = f'proj\\archive\\{id}-ids.csv'
    if not os.path.exists(archive_name):
        with open(archive_name, 'w') as f:
            f.write('id\n')

    folder_filename = f'proj\\archive\\{id}-folder-ids.csv'
    if not os.path.exists(folder_filename):
        with open(folder_filename, 'w') as f:
            f.write('id\n')

def main():
    start_time = timer()
    logging.basicConfig(filename=r'C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\proj\logs\app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 
    if (args_count := len(sys.argv)) > 2:
        print(f'One argument expected, got {args_count - 1}')
        raise SystemExit(2)

    elif args_count < 2:
        print('You must specify the chunk size')
        raise SystemExit(2)
    
    chunk_size = int(sys.argv[1])
    begin_date = datetime.now()
    logging.info(f"Process started at {begin_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Process started at {begin_date.strftime('%Y-%m-%d %H:%M:%S')}")

    if not os.path.exists(rf'C:/Users/HP/Desktop/dropbox_pr/dropbox_pr/proj/accounts/loaded_accounts.csv'):
        with open(r'C:/Users/HP/Desktop/dropbox_pr/dropbox_pr/proj/accounts/loaded_accounts.csv', 'w') as f:
            f.write('account_id')

    loaded_accounts = pd.read_csv(rf'C:/Users/HP/Desktop/dropbox_pr/dropbox_pr/proj/accounts/loaded_accounts.csv').squeeze('columns')
    accounts = pd.read_csv(r'C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\proj\accounts\accounts.csv')
    ready_accounts = accounts[accounts['ready_to_share']==1]
    ready_accounts = ready_accounts[~ready_accounts['account_id'].isin(loaded_accounts)]
    secrets = read_secrets()

    if not ready_accounts.empty:
        for _, row in ready_accounts.iterrows():
            account_id = row['account_id']
            config(account_id)
            print(f"Process started for account {account_id}")
            pst = timer()

            logging.info("Connecting to db")
            print("Connecting to db")
            st = timer()
            cnx = pymysql.connect(
                user = secrets["USER"],
                password= secrets["PASSWORD"],
                host = secrets["HOST"],
                database = secrets["DATABASE"],
                charset="utf8"
            )

            print("Connection time is: ", timer()-st)

            logging.info("Fetching data from DB...")
            st = timer()

            vf_df = data_processing.get_virtual_folder_df(cnx, account_id)
            base_df = data_processing.get_base_df(cnx, account_id)

            print("Fetching time: ", timer()-st)

            logging.info("Folder path calculating")
            print("Folder Path Calculating", )
            st = timer()
            vfwp_df = data_processing.get_hierarchy(vf_df, base_df)
            print("Calculation time: ", timer()-st)

            folder_filename = f'proj\\archive\\{account_id}-folder-ids.csv'
            print('Filtering out folders...')
            st = timer()
            folder_ids = pd.read_csv(folder_filename).squeeze('columns')
            vfwp_df = vfwp_df[~vfwp_df['fld_guid'].isin(folder_ids)]
            print("Filter time: ", timer()-st)

            logging.info("Connecting to Dropbox")
            print("Connecting to Dropbox")
            dbx = dropbox.Dropbox(
                oauth2_access_token = secrets["ACCESS_TOKEN"]
            )

            archive_filename = f"proj\\archive\\{account_id}-ids.csv"
            total_rows = len(vfwp_df)
            print(f"Total rows: {total_rows}")

            t = 0
            for i in tqdm(range(0, total_rows, chunk_size), desc = f"Processing account {account_id}"):
                t+=1
                chunk = vfwp_df.iloc[i:i+chunk_size]
                chunk = chunk.copy()
                chunk['full_path'] = chunk.apply(data_processing.replace_long_paths, axis=1)
                print(f'Fetching images from db for chunk number of {t}')

                st = timer()
                fld_id_string = chunk['fld_guid'].apply(lambda x: f"'{x}'").str.cat(sep=',')
                imap_mast_orig_query = data_processing.get_query(r'C:\Users\HP\Desktop\dropbox_pr\dropbox_pr\proj\queries\imap_mast_orig.sql').format(ids=fld_id_string)
                imap_mast_orig_df = pd.read_sql(imap_mast_orig_query, cnx)
                print("Fetch time:", timer()-st)
                
                print(f"Filtering out loaded images for chunk {t} ...")
                st = timer()
                ids = pd.read_csv(archive_filename).squeeze('columns')

                result_df = pd.merge(chunk, imap_mast_orig_df, left_on='fld_guid', right_on='imap_folder_guid', how='left')
                result_df = result_df.fillna(value={'oi_data': b''})
                result_df = result_df[~result_df['imap_img_guid'].isin(ids)]
                print("Filtering time:", timer()-st)

                if len(result_df) == 0:
                    continue

                logging.info("Uploading images to Dropbox...")
                print("Uploading images to Dropbox...")
                st = timer()
                with open(archive_filename, 'a') as f:
                    dropbox_upload.upload_to_dropbox_concurrently(dbx, result_df, f)
                print("Upload time:", timer()-st)

                loaded_folder_ids = chunk['fld_guid'].str.cat(sep='\n')
                with open(folder_filename, 'a') as f:
                    f.write(f"{loaded_folder_ids}\n")

            with open('proj\\accounts\\loaded_accounts.csv', 'a') as f:
                f.write(f'{account_id}\n')
            print(f"Process finished for account {account_id}:", timer()-pst)

            cnx.close()
    
    end_time = timer()
    logging.info(f"Total time taken for full process: {end_time - start_time} seconds")
    print(f"Total time taken for full process: {end_time - start_time} seconds")

    now = datetime.now()
    logging.info(f"Process started at {begin_date.strftime('%Y-%m-%d %H:%M:%S')} and ended at {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Process started at {begin_date.strftime('%Y-%m-%d %H:%M:%S')} and endeded at {now.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
