from flask import Flask, request 

import os, logging

import pandas as pd
import threading
import json

from logging.config import dictConfig

from urllib.parse import urlparse
from pathlib import Path

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] [%(name)s] %(levelname)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})

app = Flask(__name__)

logger = logging.getLogger('waystation')
logger.info("Starting luna waystation...")

s3_access_key = os.environ['S3_ACCESS_KEY']
s3_secret_key = os.environ['S3_SECRET_KEY']
s3_root_url   = os.environ['S3_ROOT_URL']

url_result = urlparse(s3_root_url)
s3_scheme = url_result.scheme
s3_endpoint = url_result.netloc
s3_host = url_result.hostname

root_data_dir = str(Path(url_result.path).relative_to('/'))
s3_bucket = root_data_dir.split('/')[0]

logger.info(f"Writing to: {s3_endpoint}")

logger.info ((s3_bucket, root_data_dir))
if s3_scheme == 'https':
    os.system(f"echo | openssl s_client -servername {s3_host} -connect {s3_endpoint} |  sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > /certificate.crt")
    os.environ['SSL_CERT_FILE'] = '/certificate.crt'

lock = threading.Lock()

# THIS HAS TO COME AFTER SSL THING
import pyarrow.parquet as pq
from pyarrow import fs, Table
import s3fs
from pathlib import Path

def ensure_bucket(s3_bucket):
    s3 = s3fs.S3FileSystem(
    key=s3_access_key,
    secret=s3_secret_key,
    client_kwargs={
        'endpoint_url': f'{s3_scheme}://{s3_endpoint}',
        'verify':False
    }
    )
    try:
        s3.mkdir(s3_bucket)
    except FileExistsError as exc:
        logger.info(f"Bucket {s3_bucket} already exists!")

@app.route('/')
def index(): return "Hello from Luna Waystation"

@app.route('/healthcheck')
def healthcheck(): return {'running':True}

@app.route('/datasets/views/<string:dsid>', methods=['GET'])
def get_dataset_view(dsid):
    minio = fs.S3FileSystem(scheme=s3_scheme, access_key=s3_access_key, secret_key=s3_secret_key, endpoint_override=s3_endpoint)

    ds_dir = os.path.join(root_data_dir, dsid, 'data.parquet')
    df = pq.read_table(ds_dir, filesystem=minio).to_pandas()
    return f"{df}\n"


@app.route('/datasets/<string:dsid>/segments/<string:sid>', methods=['POST'])
def post_dataset_segment(dsid, sid):
    minio = fs.S3FileSystem(scheme=s3_scheme, access_key=s3_access_key, secret_key=s3_secret_key, endpoint_override=s3_endpoint)

    file_segment = request.files['segment_data']
    segment_keys = request.form.get('segment_keys', None)
    
    logger.info(segment_keys)
 
    file_type = Path(file_segment.filename).suffix 

    if file_type=='.parquet':
        data = pd.read_parquet(file_segment).reset_index()
    elif file_type=='.csv':
        data = pd.read_csv(file_segment).reset_index()
    else:
        logger.error(f"Invalid filetype: {file_type}")

    data = data.drop(columns='index', errors='ignore')

    data['SEGMENT_ID'] = sid
 
    re_indexors = ['SEGMENT_ID']
    
    if segment_keys is not None:
        segment_keys = json.loads(segment_keys)
        for key, value in segment_keys.items():
            data.loc[:, key] = value
            re_indexors.append(key)
 
    data = data.set_index(re_indexors).reset_index()
    
    print (data)
 
    ensure_bucket(s3_bucket)

    data = data.set_index('SEGMENT_ID')
    logger.info (f"Recieved {len(data)} rows of data for dataset={dsid}, segment={sid}")
    ds_dir = os.path.join(root_data_dir, dsid, 'data.parquet')
    
    with lock:
        try:
            df = pq.read_table(ds_dir, filesystem=minio).to_pandas()
        except:
            df = data
        else:
            if not df.index.name == 'SEGMENT_ID': df = df.set_index("SEGMENT_ID")
            df = pd.concat((df.drop(index=sid, errors='ignore'), data))
        finally:
            pq.write_table( Table.from_pandas(df), ds_dir, filesystem=minio)
    
    logger.info (f"Current dataset length={len(df)}")

    return {'status':'success', 'dsid':dsid, 'sid':sid, 'rows_written': len(data)}

def main():

    app.run(host='0.0.0.0',port=6077, threaded=True, debug=True)

if __name__ == '__main__':
    main()

    
