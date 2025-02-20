import pandas as pd
import os
import sys

sys.setrecursionlimit(10000)

script_directory = os.path.dirname(os.path.abspath(__file__))
query_directory = os.path.join(script_directory, '..', 'queries')

def get_query(query_name):
    with open(query_name) as q:
        return q.read()

def get_base_df(connection, account_id):
    base_query = f"""
    select fld_guid,
        fld_name,
        concat('/', fld_owner, '/', pat_lname, ',', pat_fname, ',', pat_mname, '(', date(pat_birth_date), '),', fld_guid) pat_fullname,
        concat('/', fld_owner, '/', pat_lname, ',', pat_fname, ',', pat_mname, '(', date(pat_birth_date), '),', fld_guid) full_path
    from virtual_folder, patient_info
    where virtual_folder.fld_owner = '{account_id}'
    and virtual_folder.fld_guid = patient_info.pat_fguid

    union all

    select fld_guid,
        fld_name,
        concat('/', fld_owner, '/', fld_name, '(', fld_guid, ')') pat_fullname,
        concat('/', fld_owner, '/', fld_name, '(', fld_guid, ')') full_path
    from virtual_folder
    where fld_parent_guid = 'ph-{account_id}-tbf'
    """
    return pd.read_sql(base_query, connection)

def get_virtual_folder_df(connection, account_id):
    vf_query = f"""
    select fld_guid,
        fld_name,
        fld_owner,
        fld_parent_guid
    from virtual_folder
    where fld_owner = '{account_id}'
    """
    return pd.read_sql(vf_query, connection)

def get_hierarchy(vf_df, df):
    if df.empty:
        return df

    ndf = pd.merge(vf_df, df[['fld_guid', 'pat_fullname', 'full_path']], 
                    how='inner', 
                    left_on='fld_parent_guid', 
                    right_on='fld_guid',
                    left_index=False,
                    right_index=False)
    
    ndf = ndf.assign(
                full_path=ndf['full_path'] + '/' + ndf['fld_name'] + '(' + ndf['fld_guid_x'] + ')'
            ).rename(
                columns={'fld_guid_x': 'fld_guid'}
            )[['fld_guid', 'pat_fullname', 'fld_name', 'full_path']]
    
    return pd.concat([df, get_hierarchy(vf_df, ndf)])

def replace_long_paths(row):
    full_path = row['full_path']
    if len(full_path) > 200:
        return row['pat_fullname'] + '/BULK_EXPORT/' + row['fld_name']
    
    return full_path
