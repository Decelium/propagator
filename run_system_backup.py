import decelium_wallet.core as core
from Snapshot import Snapshot
import os,json
from type.BaseData import BaseData
import time
import pprint, argparse
from BackupManager import BackupManager

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