import pandas as pd
import json


# data utils class
class DataUtils:

    # static method: chunk csv file into dataframe with filter
    @staticmethod
    def chunk_csv2dataframe(filename, chunksize=5000, filter=None, header=None):
        # if file is too large, use chunksize to split into chunks and append to dataframe, first row is header
        df = pd.DataFrame()

        for chunk in pd.read_csv(filename, chunksize=chunksize, header=header):
            temp_chunk = filter(chunk)
            df = pd.concat([df, temp_chunk])

        return df

    # static method: load json config file and return a dict
    @staticmethod
    def load_json_config(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
