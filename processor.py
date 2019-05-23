import os
import sys
import time
import pickle
from config import *


# Checkpoint: data to load and save to record file
class Checkpoint:
    def __init__(self):
        self.processed_files = set()
        self.store_file_index = {p: 1 for p in protocol_set}
        self.packet_index = {p: 0 for p in protocol_set}
        self.current_file = ''
        self.offset = 0

    def get_file_name(self, protocol: str):
        """Get name of file of storage.
        format: [out_dir]/[protocol]_[store_file_index].csv

        :param protocol: str, name of protocol
        :return:
        """

        index = self.store_file_index[protocol]
        prefix = os.path.join(out_dir, protocol.replace('/', '-'))
        return '{}_{}.csv'.format(prefix, index)

    def save(self):
        """Dump checkpoint data to file."""
        with open(record_file, 'wb') as f:
            pickle.dump(self, f)
        print('Checkpoint saved in `{}`.'.format(record_file))

    def load(self):
        """Load checkpoint data from file."""
        with open(record_file, 'rb') as f:
            temp: Checkpoint = pickle.load(f)
        self.processed_files = temp.processed_files
        self.store_file_index = temp.store_file_index
        self.packet_index = temp.packet_index
        self.current_file = temp.current_file
        self.offset = temp.offset
        print('Checkpoint loaded from `{}`.'.format(record_file))


def split_if_necessary() -> None:
    """Check size of each storage file, called each batch
    close and open a new one to store if size exceeds max_split_size
    """

    global checkpoint, out_files
    for p in protocol_set:
        file_size = out_files[p].tell()
        if file_size >= file_split_size:
            checkpoint.store_file_index[p] += 1
            out_files[p].close()
            new_name = checkpoint.get_file_name(p)
            out_files[p] = open(new_name, 'a')
            print('File size grows over {:.2f} Mb, store in new file `{}`...'
                  .format(file_split_size / 1e6, new_name))


def process_line(line: str or bytes) -> None:
    """Process each line, including verifying validness of packet,
     check if packet is recorded, and record packet.

    :param line: str or bytes, line to process ('\n' included)
    """

    # Convert from bytes to str if needed
    if isinstance(line, bytes):
        line = str(line, 'utf-8')
    items = line.split('\t')
    # Check item count
    if len(items) != 14:
        return
    # Check protocol
    protocol = items[7]
    if protocol not in protocol_set:
        return
    protocol = items[7]
    # Check validness of packet_id
    try:
        packet_id = int(items[0])
    except ValueError:
        return
    # Check if packet is already parsed and recorded
    if protocol in checkpoint.packet_index:
        if packet_id <= checkpoint.packet_index[protocol]:
            return
    # Remove needless parts: data, parse_time, create_time, length
    # Delete from back to front will prevent index mistakes
    del items[11:]
    del items[8]
    # Write to related file
    out_files[protocol].write('\t'.join(items))
    out_files[protocol].write('\n')
    # Update index
    checkpoint.packet_index[protocol] = packet_id


def process_file(filename: str, is_old_file: bool = False):
    """Process a text file (ends with '.dat') or gzip file (ends with .gz).

    :param filename: str, name of file to process
    :param is_old_file: bool, whether this file has been processed before
            if it has been, we should skip batches already read.
    :return: int, 0 if this file is ignored or 1 if processed
    """

    # Check file type
    file_type = filename[filename.rfind('.'):]
    if file_type not in open_funcs:
        print('Fail to process `{}`, unsupported file type.'.format(filename))
        return
    # Open file according to its type
    f = open_funcs[file_type](filename)

    global start_time, bytes_count
    # Large old file: needs to recover to the starting point
    if is_old_file:
        f.seek(checkpoint.offset)
        print('Time for loading: {:.2f} s'.format(time.time() - start_time))
        start_time = time.time()  # This should be the start of processing
    else:
        # Record current file
        checkpoint.current_file = filename

    print('Start processing `{}`...'.format(filename))
    while True:
        checkpoint.offset = f.tell()
        batch = f.read(batch_size)
        # EOF
        line = f.readline()
        if line:
            batch += line
        if not batch:
            break
        # Parse batch
        _ = [process_line(line) for line in batch.splitlines()]
        bytes_count += len(batch)
        # Split large files and change storage to new files
        split_if_necessary()
    f.close()


def process_dir(dirname: str):
    """Recursively process files in given directory.

    :param dirname: str, directory of files to precess
    :return: number of files processed under this directory
    """

    file_list = sorted(os.listdir(dirname))
    for name in file_list:
        # Full name of file
        name = os.path.join(dirname, name)
        # Check if this file is already processed
        if name in checkpoint.processed_files:
            continue
        if os.path.isfile(name):
            process_file(name)
            checkpoint.processed_files.add(name)
        elif os.path.isdir(name) and recursive:
            process_dir(name)


def before_process() -> None:
    """Create directory if needed, and load records."""
    global start_time, bytes_count
    start_time = time.time()
    bytes_count = 0
    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)
    # Load checkpoints from file
    if os.path.exists(record_file):
        checkpoint.load()
    # Open files to write
    global out_files
    out_files = {p: open(checkpoint.get_file_name(p), 'a')
                 for p in protocol_set}


def after_process(is_interrupted: bool) -> None:
    """Deal with opened files, useless files and save records."""
    # Close files, and remove files with zero size
    for file in out_files.values():
        file.close()
        if os.path.getsize(file.name) == 0:
            os.remove(file.name)
    # Handle interrupts
    if is_interrupted:
        checkpoint.save()
    # Normal ending, remove record file
    elif os.path.exists(record_file):
        os.remove(record_file)
    global start_time, bytes_count
    print('Processed {:.2f} Mb in {:.2f} s.'
          .format(bytes_count / 1e6, time.time() - start_time))
    exit(int(is_interrupted))


if __name__ == '__main__':
    # Runtime globals, used for processing
    bytes_count = 0
    start_time = 0.0
    out_files = {}
    checkpoint = Checkpoint()

    try:
        # Prepare for processing
        before_process()
        # Recover from file processed last time
        if os.path.exists(checkpoint.current_file):
            print('Reloading `{}` from last checkpoints...'
                  .format(checkpoint.current_file))
            process_file(checkpoint.current_file, is_old_file=True)
        # Process command files
        dir_list = sys.argv[1:]
        if len(dir_list) == 0:
            print('Please specify at least one directory or file to parse.')
        # Process each directory / file
        for dir_name in dir_list:
            if os.path.isdir(dir_name):
                process_dir(dir_name)
            elif os.path.isfile(dir_name):
                process_file(dir_name)
            else:
                print('`{}` is not a valid directory / file; '
                      'skipped.'.format(dir_name))
    except KeyboardInterrupt as e:
        after_process(is_interrupted=True)
    else:
        after_process(is_interrupted=False)
