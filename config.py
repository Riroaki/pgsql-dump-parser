import gzip
from functools import partial

# Number of attributes in the desired relation
ATTR_COUNT = 14

# Index of attribute you want to group by whose values
GROUP_BY_ATTR_INDEX = 7
VALUE_SET = {'DNP3', 'ENIP', 'CIP', 'IEC104', 'IEC61850/GOOSE',
             'IEC61850/SV', 'MODBUS', 'OPCUA', 'OPCDA/OPCAE',
             'PROFIENT(IO/DCP/PTCP/RT)', 'S7COMM(S7)',
             'BACnet-APDU/BACnet-NPDU(BACNET)', 'MMS'}

# Index of attribute that records the index of row
# Your table should include an attribute that count the row's index,
# which is used to prevent duplicates of a same row in the output.
INDEX_ROW_COUNT = 0  # This means the 0 column records row index

# Index of attributes you want to record
RECORD_ATTR_INDEX_LIST = [1, 2, 3, 4, 5, 6, 7, 9, 10]

# File or directories
RECORD_FILE = 'rec'
OUT_DIR = 'processed'

# Parse directories recursively
RECURSIVE = False

# Settings about file size, batch, etc.
BATCH_SIZE = 50 * 1024 * 1024  # 50 MB for one batch
SPLIT = True  # Enable split files
FILE_SPLIT_SIZE = 500 * 1024 * 1024  # 500 MB per file

# Functions to open files.
# [extension name] : [partial of opening function]
OPEN_FUNCS = {
    '.gz': partial(gzip.open, mode='rb'),
    '.dat': partial(open, mode='r')
    # You can add more types of file extensions and its openers
}
