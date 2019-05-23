import gzip
from functools import partial

# Protocols
protocol_set = {'DNP3', 'ENIP', 'CIP', 'IEC104', 'IEC61850/GOOSE',
                'IEC61850/SV', 'MODBUS', 'OPCUA', 'OPCDA/OPCAE',
                'PROFIENT(IO/DCP/PTCP/RT)', 'S7COMM(S7)',
                'BACnet-APDU/BACnet-NPDU(BACNET)', 'MMS'}

# File or directories
record_file = '.rec'
out_dir = 'processed'

# Parse directories recursively
recursive = False

# Settings about file size, batch, etc.
batch_size = 10_000_000  # 10Mb for one batch
file_split_size = 500_000_000  # 500Mb per file


# Functions to open files.
# [extension name] : [partial of opening function]
open_funcs = {
    '.gz': partial(gzip.open, mode='rb'),
    '.dat': partial(open, mode='r')
    # You can add more types of file extensions and its openers
}
