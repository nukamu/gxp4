import logging

# Define ports number used.
metaport=15806
dataport=15807

# Define the directory for log
log_dir="/tmp"

# Define buffer size and block size in mogami
bufsize=1024
blsize=1024 * 1024

# Define max write length without communication
writelen_max=1024 * 1024

# Configurations for prefetch
prefetch=True
force_prenum=False
prenum=10

write_local=True
multithreaded=True

# Log level
fs_loglevel=logging.INFO
meta_loglevel=logging.INFO
data_loglevel=logging.INFO

# Access patterns
ap=False  # get access pattern or not
ap_comm="/tmp/mogami_ap"  # path for internal communication

# Type of metadata
meta_type='fs'   # should be 'fs' or 'db'

# configure for some optimizations
local_request=False
auto_repl=False
