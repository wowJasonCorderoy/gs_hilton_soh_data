from google.cloud import storage
import pandas as pd
import re
import json
import re
from google.cloud import bigquery
from datetime import datetime,timezone  
import os
import hashlib
import numpy as np
    
#### Declare constants
PROJECT_ID = 'gcp-wow-pvc-grnstck-prod'

now_utc = datetime.now(timezone.utc) # timezone aware object, unlike datetime.utcnow().
    
def pretty_print_event(event: dict = None) -> None:
    """
    pretty print event dict.

    Args:
        event (dict, optional): [description]. Defaults to None.
    """
    print("event:")
    print(event)
    return

def pretty_print_context(context=None) -> None:
    """
    pretty print context dict.

    Args:
        event (dict, optional): [description]. Defaults to None.
    """
    print("context:")
    print(context)
    return

def get_file_name(event: dict = None) -> str:
    return event["name"]

def get_bucket_name(event: dict = None) -> str:
    return event["bucket"]

def save_to_bucket_name(bucketname: str) -> str:
    return bucketname + "_output"

def is_correctFileType(fileName: str = None, regex: str = r".*xls[x]?$") -> bool:
    """[summary]

    Args:
        fileName (str, optional): name of file. Defaults to None.
        regex (str, optional): regex expression to evaluate. Defaults to r".*xls[x]?$".

    Returns:
        bool: if True then regex matched fileName else False
    """
    import re

    if re.match(regex, fileName):
        return True
    else:
        return False

def gen_full_bucket_path(bucketName: str = None, fileName: str = None) -> str:
    return "gs://" + bucketName + "/" + fileName

def copy_blob(
    bucket_name, blob_name, destination_bucket_name, destination_blob_name
):
    """Copies a blob from one bucket to another with a new name."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    print(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )
    
def infer_site(filename: str):
    if 'trug' in filename.lower():
        return 'Truganina'
    elif 'hw' in filename.lower():
        return 'Heathwood'
    elif 'bun' in filename.lower():
        return 'Bunbury'
    elif 'offsite storage' in filename.lower(): # e.g. filename 'Offsite Storage 28032022' is actually Heathwood
        return 'heathwood'
    else:
        return 'Other'

def infer_table_id(filename: str):
    if 'offsite' in filename.lower():
        return 'hilton.hilton_offsitestorage'
    elif 'offiste' in filename.lower():
        return 'hilton.hilton_offsitestorage'
    elif "po's" in filename.lower():
        return 'hilton.hilton_offsitestorage'
    else:
        return 'hilton.hilton_inventory'

def load_site_soh(params_save_dest: str):
    headers = ['ACTUALINVENTORYLEVELEND', 'DATE', 'DESCRIPTION', 'PRODUCTID', 'STOCKINGPOINTID', 'MANUFACTUREDDATE']
    dtypes = {'ACTUALINVENTORYLEVELEND': np.float64,
        'DESCRIPTION': 'str', 
        'PRODUCTID': 'str', 
        'STOCKINGPOINTID': 'str',
            }
    parse_dates = ['DATE', 'MANUFACTUREDDATE']
    try:
        df = pd.read_excel(params_save_dest, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:F", sheet_name=['PORK','LAMB','BEEF'])
        df = pd.concat(df)
    except:
        df = pd.read_excel(params_save_dest, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:F", sheet_name='Sheet1')
    return df

def load_offsite_soh(params_save_dest: str):
    headers = ['VENDOR', 'NAME_1', 'WOW_PO_NUMBER', 'GOODS_SUPPLIER', 'GOODS_SUPPLIER_NAME', 'PURCH_DOC', 'PLANT', 'MATERIAL', 'MATERIAL_DESCRIPTION',
                'MATL_GROUP', 'DOC_DATE', 'DELIV_DATE', 'SCHEDULED_QTY', 'OUN1', 'OUN2', 'QTY_DELIVERED']
    dtypes = {'VENDOR': 'str',
                'NAME_1': 'str',
                'WOW_PO_NUMBER': 'str',
                'GOODS_SUPPLIER': 'str',
                'GOODS_SUPPLIER_NAME': 'str',
                'PURCH_DOC': 'str',
                'PLANT': 'str',
                'MATERIAL': 'str',
                'MATERIAL_DESCRIPTION': 'str',
                'MATL_GROUP': 'str',
                'SCHEDULED_QTY': np.float64,
                'OUN1': 'str',
                'OUN2': 'str',
                'QTY_DELIVERED': np.float64,
            }
    parse_dates = ['DOC_DATE','DELIV_DATE']
    try:
        df = pd.read_excel(params_save_dest, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:P", sheet_name=['PORK','LAMB','BEEF'])
        df = pd.concat(df)
    except:
        df = pd.read_excel(params_save_dest, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:P", sheet_name='Sheet1')
    return df

def get_function_2_load_data(filename: str):
    if 'offsite' in filename.lower():
        return load_offsite_soh
    elif 'offiste' in filename.lower():
        return load_offsite_soh
    elif "po's" in filename.lower():
        return load_offsite_soh
    else:
        return load_site_soh

def run(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pretty_print_event(event)
    pretty_print_context(context)

    fileName = get_file_name(event)
    print(f"Processing file: {fileName}.")
    print(fileName)
    
    # if not params file then abort
    if not is_correctFileType(fileName):
        print(f"File {fileName} is not correct file type. ABORTING.")
        return

    bucketName = get_bucket_name(event)
    save_to_bucketname = save_to_bucket_name(bucketName)
    #fileName_full = "gs://" + bucketName + "/" + fileName
    fileName_full = gen_full_bucket_path(bucketName, fileName)
    
    # get params from bucket
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucketName)
        params_blob = bucket.get_blob(fileName)
        params_save_dest = "/tmp/data_file.xlsx"
        params_blob.download_to_filename(params_save_dest)
        print(f"{fileName} saved to {params_save_dest}")
    except:
        print(f"{fileName} does not exist. ABORTING.")
        return
    
    # hash time
    file_contents_md5 = hashlib.md5(open(params_save_dest,'rb').read()).hexdigest()
    
    # read xlsx as pandas dd    
    df = get_function_2_load_data(fileName)(params_save_dest=params_save_dest)
    
    # add an as at column
    df['site'] = infer_site(fileName)
    df['upload_utc_dt'] = now_utc
    df['filename'] = fileName
    df['file_contents_md5'] = file_contents_md5
      
    # save to cloud storage
    saveFileName = now_utc.strftime("%Y%m%d_%H:%M:%S")+"_"+fileName
    saveLocation = "gs://" + save_to_bucketname + "/" + saveFileName
    df.to_csv(saveLocation+'.csv', index=False)
    df.to_pickle(saveLocation+'.pk')

    copy_blob(bucket_name=bucketName, blob_name=fileName, destination_bucket_name=save_to_bucketname, destination_blob_name=saveFileName)

    # save to BQ
    client = bigquery.Client(project=PROJECT_ID)
    table_id = infer_table_id(fileName)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

if __name__ == "__main__":
    run()
