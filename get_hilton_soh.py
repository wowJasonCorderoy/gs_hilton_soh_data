
#### Declare constants
N_COLUMNS = 7
PROJECT_ID = 'gcp-wow-pvc-grnstck-prod'

project_creds_file_map = {
    'gcp-wow-pvc-grnstck-prod':r"C:\dev\greenstock\optimiser_files\key_prod.json",
    'gcp-wow-pvc-grnstck-dev':r"C:\dev\greenstock\optimiser_files\key_dev.json",
}
CREDS_FILE_LOC = project_creds_file_map.get(PROJECT_ID)

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

def run(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    from google.cloud import storage
    import pandas as pd
    from sklearn.ensemble import IsolationForest
    import re
    import json
    
    
    # # for local dev...
    # if run_local:
    #     event = c.EVENT
    #     context = c.CONTEXT       

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
    report_name = matches_clean.pop(0)
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
        'Unrestricted': lambda x: x.astype('int64'),
        'BUn': lambda x: x.astype('str'),
        'Manuf. Dte' : lambda x: pd.to_datetime(x, dayfirst=True),
    }

    for k,v in df_schema.items():
        df[k] = df_schema[k](df[k])

    # add an as at column
    df['upload_utc_dt'] = now_utc
        
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
    bq_df.to_csv(saveLocation, index=False)

    # save to BQ
    client = bigquery.Client(project=PROJECT_ID)
    table_id = 'sandpit.hilton_soh'

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    job = client.load_table_from_dataframe(bq_df, table_id, job_config=job_config)


if __name__ == "__main__":
    run()