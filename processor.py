import os
import sys
import time
import config
import logging
from typing import TextIO
from checkpoint import Checkpoint


class Processor(object):
    """Class for processing dump files from postgresql."""
    MILLION = 1024 * 1024

    def __init__(self):
        self.bytes_count = 0
        self.start_time = 0.0
        self.out_files = {}
        self.checkpoint = Checkpoint(config.VALUE_SET)
        self.init_time()

    def init_time(self):
        """Init time."""

        self.start_time = time.time()

    def add_bytes_count(self, count: int):
        """Add up bytes count."""

        self.bytes_count += count

    def split_if_necessary(self) -> None:
        """Check size of each storage file, called each batch
        close and open a new one to store if size exceeds max_split_size
        """

        # Convert MB to Byte
        for v in config.VALUE_SET:
            file_size = self.out_files[v].tell()
            if file_size >= config.FILE_SPLIT_SIZE:
                self.checkpoint.update_file_index(v)
                new_file = self.checkpoint.get_file_name(v, config.OUT_DIR)
                self.out_files[v].close()
                self.out_files[v] = open(new_file, 'a')
                logging.info('File size grows over {:.2f} MB, '
                             'store in new file `{}`...'
                             .format(config.FILE_SPLIT_SIZE / self.MILLION,
                                     new_file))

    def process_line(self, line: str) -> None:
        """Process each line, including verifying validness of
         group by attribute, check if packet is recorded, and record packet.

        :param line: str, line to process ('\n' not included)
        """

        attributes = line.split('\t')
        # Check value in values to group by
        value = attributes[config.GROUP_BY_ATTR_INDEX]
        if value not in config.VALUE_SET:
            return
        row_count = int(attributes[config.INDEX_ROW_COUNT])
        # Check if packet is already parsed and recorded
        if row_count <= self.checkpoint.row_count[value]:
            return
        # Keep attributes we're interested in
        data = [attributes[i] for i in config.RECORD_ATTR_INDEX_LIST]
        # Write to related file
        self.out_files[value].write('\t'.join(data))
        self.out_files[value].write('\n')
        # Update index
        self.checkpoint.row_count[value] = row_count

    @staticmethod
    def verify_file_schema(fp: TextIO) -> bool:
        """Verify the schema of data contained in file.
        The dump files of postgresql should contain exactly one table each.
        """

        line = fp.readline()
        # Remember to return head of file
        fp.seek(0)
        if isinstance(line, bytes):
            line = str(line, encoding='utf-8')
        # Remove empty cells
        attributes = list(filter(None, line.split('\t')))
        # Check attribute count
        if len(attributes) != config.ATTR_COUNT:
            return False
        # Check validness of index attribute
        try:
            _ = int(attributes[config.INDEX_ROW_COUNT])
        except ValueError:
            return False
        return True

    def process_file(self, filename: str, is_old_file: bool = False) -> None:
        """Process a text file (ends with '.dat') or gzip file (ends with .gz).

        :param filename: str, name of file to process
        :param is_old_file: bool, whether this file has been processed before
                if it has been, we should skip batches already read.
        :return: int, 0 if this file is ignored or 1 if processed
        """

        # Check file type
        file_type = filename[filename.rfind('.'):]
        if file_type not in config.OPEN_FUNCS:
            logging.info('Fail to process `{}`: unsupported file type.'
                         .format(filename))
            return
        # Open file according to its type
        fp = config.OPEN_FUNCS[file_type](filename)

        # Old file: needs to recover to the starting point
        if is_old_file and self.checkpoint.offset > 0:
            fp.seek(self.checkpoint.offset)
            logging.info('Time for seeking file offset: {:.2f} s'
                         .format(time.time() - self.start_time))
            # This should be the start of processing
            self.init_time()
        else:
            # New files:
            # needs to verify whether this file contains the table we want
            if not self.verify_file_schema(fp):
                logging.info('Schema of `{}` doesn\'t fit; skip.'
                             .format(filename))
                fp.close()
                return
            # Record current file
            self.checkpoint.current_file = filename

        logging.info('Start processing `{}`...'.format(filename))
        while True:
            self.checkpoint.offset = fp.tell()
            batch = fp.read(config.BATCH_SIZE)
            # EOF
            line = fp.readline()
            if line:
                batch += line
            if not batch:
                break
            # Convert from bytes to str if needed
            if isinstance(batch, bytes):
                batch = str(batch, 'utf-8')
            # Parse batch
            for line in batch.splitlines():
                self.process_line(line)
            self.add_bytes_count(len(batch))
            # Split large files and change storage to new files
            if config.SPLIT:
                self.split_if_necessary()
        fp.close()

    def process_dir(self, dirname: str) -> None:
        """Recursively process files in given directory.

        :param dirname: str, directory of files to precess
        :return: number of files processed under this directory
        """

        file_list = sorted(os.listdir(dirname))
        for name in file_list:
            # Full name of file
            name = os.path.join(dirname, name)
            # Check if this file is already processed
            if name in self.checkpoint.processed_files:
                continue
            if os.path.isfile(name):
                self.process_file(name)
                self.checkpoint.processed_files.add(name)
            elif os.path.isdir(name) and config.RECURSIVE:
                self.process_dir(name)

    def before_process(self) -> None:
        """Create directory if needed, and load records."""
        if not os.path.isdir(config.OUT_DIR):
            os.mkdir(config.OUT_DIR)
        # Load checkpoints from file
        if os.path.exists(config.RECORD_FILE):
            self.checkpoint.load(config.RECORD_FILE)
            logging.info('Checkpoint loaded from `{}`.'
                         .format(config.RECORD_FILE))
        # Open files to write
        self.out_files = {
            v: open(self.checkpoint.get_file_name(v, config.OUT_DIR), 'a')
            for v in config.VALUE_SET
        }

    def after_process(self, is_interrupted: bool) -> None:
        """Deal with opened files, useless files and save records."""
        # Close files, and remove files with zero size
        for file in self.out_files.values():
            file.close()
            if os.path.getsize(file.name) == 0:
                os.remove(file.name)
        # Handle interrupts
        if is_interrupted:
            self.checkpoint.save(config.RECORD_FILE)
            logging.info('Checkpoint saved in `{}`.'.format(config.RECORD_FILE))
        # Normal ending, remove record file
        elif os.path.exists(config.RECORD_FILE):
            os.remove(config.RECORD_FILE)
        # Analyse speed
        total_mb = self.bytes_count / self.MILLION
        total_time = time.time() - self.start_time
        avg_speed = total_mb / total_time
        logging.info('Processed {:.2f} MB in {:.2f} s, {:.2f} MB/s on average.'
                     .format(total_mb, total_time, avg_speed))
        exit(int(is_interrupted))

    def process(self, dir_list: list) -> None:
        """Process list of directories / files"""
        try:
            # Prepare for processing
            self.before_process()
            # Recover from file processed last time
            if os.path.exists(self.checkpoint.current_file):
                logging.info('Reloading `{}` from last checkpoints...'
                             .format(self.checkpoint.current_file))
                self.process_file(self.checkpoint.current_file,
                                  is_old_file=True)
            if len(dir_list) == 0:
                logging.error(
                    'Please specify at least one directory or file to process.')
            # Process each directory / file
            for dir_name in dir_list:
                if os.path.isdir(dir_name):
                    self.process_dir(dir_name)
                elif os.path.isfile(dir_name):
                    self.process_file(dir_name)
                else:
                    logging.warning('`{}` is not a directory / file; skip.'
                                    .format(dir_name))
        except KeyboardInterrupt:
            self.after_process(is_interrupted=True)
        else:
            self.after_process(is_interrupted=False)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    p = Processor()
    p.process(sys.argv[1:])
