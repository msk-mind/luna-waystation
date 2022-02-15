from flask import Flask, request, jsonify
import click

app = Flask(__name__)

import pandas as pd

root_data_dir = '/tmp/data/tables'

import os
import threading
lock = threading.Lock()

DS_AGG = {}
 
@app.route('/datasets/<string:dsid>/segments/<string:sid>', methods=['POST'])
def post_dataset_segment(dsid, sid):
    print (f"Recieved data for dataset={dsid}, segment={sid}")

    file = request.files['segment']
    data = pd.read_csv(file)
    data['SEGMENT_ID'] = sid

    data = data.set_index('SEGMENT_ID')

    ds_dir = os.path.join(root_data_dir, dsid)
    
    with lock:
        if not os.path.exists(ds_dir): 
            os.makedirs(ds_dir)
            df = data
            df.to_parquet(os.path.join(ds_dir, 'data.parquet'))
        else:
            df = pd.read_parquet(os.path.join(ds_dir, 'data.parquet'), engine='fastparquet')

            if not df.index.name == 'SEGMENT_ID': df = df.set_index("SEGMENT_ID")

            df = df.drop(index=sid, errors='ignore').append(data)

            df.to_parquet(os.path.join(ds_dir, 'data.parquet'))
    
    print (f"Current dataset length={len(df)}")

    return {'status':'success', 'dsid':dsid, 'sid':sid, 'rows_written': len(data)}

@click.command()
def main():

    app.run(host='0.0.0.0',port=6077, threaded=True, debug=True)

if __name__ == '__main__':
    main()

    