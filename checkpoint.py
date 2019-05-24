import os
import pickle


# Checkpoint: data to load and save to record file
class Checkpoint(object):
    def __init__(self, value_set: set) -> None:
        self.processed_files = set()
        self.store_file_index = {v: 1 for v in value_set}
        self.row_count = {v: 0 for v in value_set}
        self.current_file = ''
        self.offset = 0

    def get_file_name(self, value: str, out_dir: str) -> str:
        """Get name of file of storage.
        format: [out_dir]/[protocol]_[store_file_index].csv

        :param value: str, name of protocol
        :param out_dir: directory of parsed data storage
        :return:
        """

        index = self.store_file_index[value]
        prefix = os.path.join(out_dir, value.replace('/', '-'))
        return '{}_{}.csv'.format(prefix, index)

    def save(self, rec_file: str) -> None:
        """Dump checkpoint data to file."""
        with open(rec_file, 'wb') as f:
            pickle.dump(self, f)

    def load(self, rec_file: str) -> None:
        """Load checkpoint data from file."""
        with open(rec_file, 'rb') as f:
            temp: Checkpoint = pickle.load(f)
        self.processed_files = temp.processed_files
        self.store_file_index = temp.store_file_index
        self.row_count = temp.row_count
        self.current_file = temp.current_file
        self.offset = temp.offset

    def update_file_index(self, v: str):
        """Add file index by 1, which means opening a new file
        to store rows whose value == v.
        """

        assert v in self.store_file_index
        self.store_file_index[v] += 1
