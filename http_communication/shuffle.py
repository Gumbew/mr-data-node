import json
import os

import dask.dataframe as dd
import pandas as pd
import requests

from receive_commands.receive_commands import Command

# uncomment for Ubuntu as it runs __file__ as ~
os.chdir(os.path.dirname(os.path.dirname(__file__)))

with open(os.path.join("config", "config.json")) as config_file:
    config = json.load(config_file)

with open(os.path.join('config', 'data_node_info.json')) as arbiter_node_json_data:
    self_node_ip = json.load(arbiter_node_json_data)['self_address']


class ShuffleCommand:
    def __init__(self, data, file_path, field_delimiter):
        self._data = {
            'data_node_ip': data['data_node_ip'],
            'content': data['content'],
            'file_path': file_path,
            'field_delimiter': field_delimiter
        }

    def send(self):
        data = {
            'content': self._data['content'],
            'file_path': self._data['file_path'],
            'field_delimiter': self._data['field_delimiter']
        }
        url = f'http://{self._data["data_node_ip"]}/command/finish_shuffle'
        try:
            response = requests.post(url, json=data, timeout=10)
            return response
        except requests.exceptions.ReadTimeout:
            pass
        except requests.exceptions.ConnectionError:
            pass


def shuffle(content):
    file_id = content["file_id"]
    full_file_path = os.path.join(Command.paths_per_file_name[file_id]["shuffle_folder_name_path"], 'shuffled.csv')
    field_delimiter = content['field_delimiter']

    for f in Command.paths_per_file_name[file_id]["segment_list"]:
        data_f = pd.read_parquet(os.path.join(Command.paths_per_file_name[file_id]["map_folder_name_path"],
                                              f, "part.0.parquet"))

        # headers = list(data_f.columns)

        for i in content['nodes_keys']:
            index_list = []
            for index, item in enumerate(data_f.loc[:, 'key_column']):

                min, max = i["hash_keys_range"]
                last_node = max == content['max_hash']
                hash_item = Command.hash_f(item)
                hash_item_in_range = min <= hash_item < max
                if hash_item_in_range or (hash_item == max and last_node):
                    index_list.append(index)

            if i['data_node_ip'] == self_node_ip:
                dd.from_pandas(data_f.iloc[index_list], npartitions=1).to_parquet(full_file_path,
                                                                                  write_index=False,
                                                                                  engine="pyarrow"
                                                                                  )
            else:
                data = {'content': data_f.iloc[index_list].to_json(),
                        'data_node_ip': i['data_node_ip']}

                sc = ShuffleCommand(data, full_file_path, field_delimiter)
                sc.send()
