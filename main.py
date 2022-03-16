from google.cloud import storage
import pandas as pd
import re
import json
import re
from google.cloud import bigquery
from datetime import datetime,timezone  
import os
import hashlib
    
#### Declare constants
N_COLUMNS = 7
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

def is_correctFileType(fileName: str = None, regex: str = r".*htm[l]?$") -> bool:
    """[summary]

    Args:
        fileName (str, optional): name of file. Defaults to None.
        regex (str, optional): regex expression to evaluate. Defaults to r".*htm[l]?$".

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
        params_save_dest = "/tmp/data_file.html"
        params_blob.download_to_filename(params_save_dest)
        print(f"{fileName} saved to {params_save_dest}")
    except:
        print(f"{fileName} does not exist. ABORTING.")
        return
    
    # find each block of data
    re_per_block = re.compile(">([^< ]+)<")

    textfile = open(params_save_dest, 'r')
    filetext = textfile.read()
    textfile.close()
    matches = re.findall(re_per_block, filetext)
    
    file_contents_md5 = hashlib.md5(open(fileName,'rb').read()).hexdigest()

    # 1st element is the report heading
    # next are table headings
    # then rest is the data

    # first replace &nbsp; with space
    replace_these = {'&nbsp;': ' ',
                    '&#38;': '&',
                    }
    matches_clean = matches
    for k, v in replace_these.items():
        matches_clean = [x.replace(k, v) for x in matches_clean]
    matches_clean = [re.sub(r'\s{2,}', " ", x).strip() for x in matches_clean]

    # make a list of lists then dataframe
    # sometimes 1st column name is 2nd item in list (in which case report title is first) other times it's 1st.
    if matches_clean[0] != 'Material':
        report_name = matches_clean.pop(0)
    else:
        report_name = 'Stocklist with remainging SLED and days from pack'
    lol = list(zip(*[iter(matches_clean)]*N_COLUMNS))
    # remove duplicate headers
    df_headings = lol.pop(0)
    lol = [x for x in lol if x != df_headings]
    # to dataframe
    df = pd.DataFrame(lol, columns=df_headings)

    # types
    df_schema = {
        'Material': lambda x: x.astype('str'),
        'WoW material no.': lambda x: x.astype('str'),
        'Material description': lambda x: x.astype('str'),
        'Batch': lambda x: x.astype('str'),
        'Unrestricted': lambda x: [int(re.sub(r'[^0-9]', "", str(s))) for s in x],
        'BUn': lambda x: x.astype('str'),
        'Manuf. Dte' : lambda x: pd.to_datetime(x, dayfirst=True),
    }

    for k,v in df_schema.items():
        df[k] = df_schema[k](df[k])

    # add an as at column
    df['upload_utc_dt'] = now_utc
    df['filename'] = fileName
    df['file_contents_md5'] = file_contents_md5
    
    # clean headings for BQ save
    colname_map = {
        'Material': 'material',
        'WoW material no.': 'wow_material_no',
        'Material description': 'material_description',
        'Batch': 'batch',
        'Unrestricted': 'unrestricted',
        'BUn': 'bun',
        'Manuf. Dte' : 'manufacture_date',
    }
    bq_df = df.rename(columns=colname_map)
    
    # save to cloud storage
    saveFileName = now_utc.strftime("%Y%m%d_%H:%M:%S")+"_"+fileName
    saveLocation = "gs://" + save_to_bucketname + "/" + saveFileName
    bq_df.to_csv(saveLocation+'.csv', index=False)
    bq_df.to_pickle(saveLocation+'.pk')

    #clean_saveFileName = re.sub(r'[^0-9a-zA-Z.]','_', saveFileName) #copy_blob has issues with spaces and special chars so replace them
    #copy_blob(bucket_name=bucketName, blob_name=fileName, destination_bucket_name=save_to_bucketname, destination_blob_name=clean_saveFileName)
    copy_blob(bucket_name=bucketName, blob_name=fileName, destination_bucket_name=save_to_bucketname, destination_blob_name=saveFileName)

    # save to BQ
    client = bigquery.Client(project=PROJECT_ID)
    table_id = 'masterdata_view.hilton_soh_daily'

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    job = client.load_table_from_dataframe(bq_df, table_id, job_config=job_config)


if __name__ == "__main__":
    run()
