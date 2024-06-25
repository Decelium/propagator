import decelium_wallet.core as core
from Snapshot import Snapshot
import os,json
from type.BaseData import BaseData
import time
import pprint, argparse
from BackupManager import BackupManager
#sb = SystemBackup()
#early_stop = False
#file_types = ['ipfs','json','file','host','directory','user']
#file_types = ['json']
def main(dir,host,protocol,mode, types):
    sb = BackupManager()
    early_stop = False
    file_types = types.split(',')
    results = sb.run(dir,host,protocol,mode, file_types, early_stop)
    pprint.pprint(results)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run SystemBackup with specified mode and files.')
    parser.add_argument('--mode', type=str, help='The mode to run the SystemBackup in')
    parser.add_argument('--types', type=str, help='Comma-separated list of file types')
    parser.add_argument('--host', type=str, help='The host URL of the remote node',default='devdecelium.com')
    parser.add_argument('--protocol', type=str, help='The protocol to use',default='http')
    parser.add_argument('--dir', type=str, help='The local dir to use',default='../devdecelium.com_systembackup')

    args = parser.parse_args()
    main(args.dir,args.host,args.protocol,args.mode, args.types)

# Git interface
# git remote add origin ADDRESS
# git remote remove origin ADDRESS
# git 


#sb.run('validate',file_types, early_stop)
#pprint.pprint(sb.run('status',file_types, early_stop))
#if __name__ == "__main__":
#    ...


# Next Tasks - 
# All FileTypes working
# Edits / Create / Modified dates need to be present, considered when pulling / pushing
# All Payloads protected with hashes
# Unit tests of all above, in either regular tests or backup tests


# python3 -u deploy_raw_api.py >> /app/database/job_logs//deploy_raw_api.py_out.txt 2>> /app/database/job_logs//deploy_raw_api.py_errors.txt


# Next Steps
# --------------------------------------------

# June 17 1 - System runs without crashing:
# - Can clean IPFS and reboot just the IPFS server though the signal mechanism
# - Can trigger IPFS reboot and clean at 50% disk capacity
# - (Passive) Test Server runs 1 week without needing intervention

# June 17 2 - System can be monitored easily
# - Can get disk, memory, processes, and DB status from endpoint
# - Can raise alert (locally) if node starts "misbehaving"

# June 24 3 - Node can be automatically administered
# - Can deploy node from outside
# - Can restore data
# - And rebase
# - And shutdown / delete node
# - All with simple CLI tool (SecretAgent)