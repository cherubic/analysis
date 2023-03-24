import pandas as pd
import numpy as np
import time
import utils
import os


# original data filter function
def original_data_filter(df, filter_dict, values_count=0):
    # filter by filter_dict column index and value
    for key in filter_dict:
        df = df[df[key] == filter_dict[key]]

    # filter by null count or empty string
    result = df.dropna(thresh=values_count)

    return result


# this is parse original data function
def parse_original_data():
    # load config file
    config_dict = utils.DataUtils.load_json_config('config.json')

    # original data files directory
    origin_directory_path = config_dict['origin_directory_path']

    origin_out_dir = config_dict['origin_out_dir']
    if not os.path.exists(origin_out_dir):
        os.mkdir(origin_out_dir)

    # loop through all file path in directory
    for origin_file_path in utils.DataUtils.get_file_path(origin_directory_path):

        # load original data cvs file into dataframe with filter function
        # values_count is the minimum number of valid values count in a row
        valid_values_count = 50

        origin_df = utils.DataUtils.chunk_csv2dataframe(origin_file_path,
                                                        filter=lambda x: original_data_filter(x, {3: 880}, values_count=valid_values_count),
                                                        header=None)

        # get current time minutes + seconds string to current_time
        current_time = time.strftime("%H%M%S_", time.localtime())

        # filter path and extension
        file_name = os.path.splitext(os.path.basename(origin_file_path))[0]

        # generate csv file name with prefix "output_" + valid_values_count + minutes and seconds
        output_file_path = "output_" + str(valid_values_count) + "_" + current_time + file_name + ".csv"

        # save to csv file under origin_directory_path directory connect output_file_path
        origin_df.to_csv(os.path.join(origin_out_dir, output_file_path), index=False)


if __name__ == '__main__':
    parse_original_data()
