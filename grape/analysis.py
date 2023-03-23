# This is a sample Python script.
import pandas as pd
import utils


# search related vds_id by function, not implemented yet
def search_related_vds_id(df, vds_id, time):
    # TODO: search related vds_id by id and time
    return NotImplemented


# this is analysis function
def analysis():
    # load config file
    config_dict = utils.DataUtils.load_json_config('config.json')

    # incident csv file path
    incident_file_path = config_dict['incidents_file_path']

    # vds csv file path
    vds_file_path = config_dict['vds_file_path']

    # load incidents cvs file into dataframe
    incident_df = utils.DataUtils.chunk_csv2dataframe(incident_file_path, header=0)

    # load vds cvs file into dataframe
    vds_df = utils.DataUtils.chunk_csv2dataframe(vds_file_path, header=0)

    # merge two dataframe by vds_id
    merged_df = pd.merge(incident_df, vds_df, on='vds_abs')

    for incident in incident_df:
        related_vds = search_related_vds_id(vds_df, incident['vds_abs'], incident['time'])
        # TODO: merge related vds data to merged_df
        print(related_vds)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    analysis()

