import pandas as pd
import numpy as np
import time
import utils


# original data filter function
def original_data_filter(df, filter_dict, null_count=0):
    # filter by filter_dict column index and value
    for key in filter_dict:
        df = df[df[key] == filter_dict[key]]

    # filter by null count or empty string
    df = df.replace('', np.nan)
    result = df.dropna(thresh=null_count)

    return result


# this is parse original data function
def parse_original_data():
    # load config file
    config_dict = utils.DataUtils.load_json_config('config.json')

    # original data file path
    origin_file_path = config_dict['origin_file_path']

    # load original data cvs file into dataframe with filter function
    origin_df = utils.DataUtils.chunk_csv2dataframe(origin_file_path,
                                                    filter=lambda x: original_data_filter(x, {3: 880}, null_count=20),
                                                    header=None)

    # get current time minutes + seconds string to current_time
    current_time = time.strftime("%H%M%S_", time.localtime())

    # save to csv file with prefix "output_" + minutes and seconds
    origin_df.to_csv("output_" + current_time + origin_file_path, index=False)


if __name__ == '__main__':
    parse_original_data()
