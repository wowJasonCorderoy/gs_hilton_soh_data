import re
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from datetime import datetime,timezone

#### Declare constants
N_COLUMNS = 7
PROJECT_ID = 'gcp-wow-pvc-grnstck-prod'

project_creds_file_map = {
    'gcp-wow-pvc-grnstck-prod':r"C:\dev\greenstock\optimiser_files\key_prod.json",
    'gcp-wow-pvc-grnstck-dev':r"C:\dev\greenstock\optimiser_files\key_dev.json",
}
CREDS_FILE_LOC = project_creds_file_map.get(PROJECT_ID)

now_utc = datetime.now(timezone.utc) # timezone aware object, unlike datetime.utcnow().
#now_utc_str = now_utc.strftime("%Y/%m/%d %H:%M:%S")

# find each block of data
re_per_block = re.compile(">([^< ]+)<")

textfile = open("Job IOT_SOH_M001, Step 1.htm", 'r')
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

# save to BQ
### Save to bigquery
def get_bq_credentials():
    try:
        client = service_account.Credentials.from_service_account_file(**{
        'filename':CREDS_FILE_LOC, 
        'scopes':["https://www.googleapis.com/auth/cloud-platform"],
        })
    except:
        client = bigquery.Client(project=PROJECT_ID)
    return client

credentials = get_bq_credentials()
# Update the in-memory credentials cache (added in pandas-gbq 0.7.0).

pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = PROJECT_ID

pd.io.gbq.to_gbq(bq_df, 'sandpit.hilton_soh', PROJECT_ID, chunksize=100000, reauth=False, if_exists='replace')

# client = get_bq_credentials()
# try:
#     job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
#     job = client.load_table_from_dataframe(bq_df, 'sandpit.hilton_soh', job_config=job_config)
# except:
#     job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
#     job = client.load_table_from_dataframe(bq_df, 'sandpit.hilton_soh', job_config=job_config)

