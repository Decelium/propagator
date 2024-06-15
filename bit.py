import warnings
warnings.filterwarnings('ignore', 'urllib3 v2 only supports OpenSSL 1.1.1+')

import decelium_wallet.core as core
from Snapshot import Snapshot
import os,json
from type.BaseData import BaseData
import time
import pprint, argparse
from BackupManager import BackupManager
import sys

def main(dir,host,protocol,mode, types):
    sb = BackupManager()
    early_stop = False
    file_types = types.split(',')
    results = sb.run(dir,host,protocol,mode, file_types, early_stop)
    pprint.pprint(results)

def get_configs_dir(dirstr):
    if dirstr== None:
        dirstr = '../propagator_default/'
    if not os.path.exists(dirstr):
        os.makedirs(dirstr)
    return dirstr

def get_config_filepath(dirstr,originstr):
    
    config_dir = os.path.join(dirstr,originstr+'.remote.json')
    return config_dir

def command_remote():
    parser = argparse.ArgumentParser(description='Create a remote snapshot directory')
    parser.add_argument('command', type=str, help='The command (remote)')
    parser.add_argument('subcommand', type=str, help='The sub command (add or remove)')
    parser.add_argument('origin',nargs='?', type=str,  help='The name of the origin')
    parser.add_argument('host',nargs='?', type=str,  help='the host of the origin (like www.something.com)')
    parser.add_argument('directory',nargs='?' , type=str,  help='the directory to store the snapshot',default=None)
    args = parser.parse_args()
    args.directory = get_configs_dir(args.directory)
    
    if args.subcommand == 'add':
        config_dir = get_config_filepath(args.directory,args.origin)
        remote_config = {
            'origin':args.origin,
            'host':args.host,
            'dir':args.directory,
        }
        with open(config_dir,'w') as f:
            f.write(json.dumps(remote_config,indent=1))

    elif args.subcommand == 'remove':
        config_dir = get_config_filepath(args.directory,args.origin)
        if os.path.exists(config_dir):
            os.remove(config_dir)

    elif args.subcommand == 'ls':
        files = os.listdir(args.directory)
        found = False
        for filename in files:
            if filename.endswith('.remote.json'):
                found = True
                print(filename.replace('.remote.json',''))
        if found == False:
            print(".empty")
    else:
        print("could not process remote command. Sub command should be (add,remove,ls)")
def command_push():
    pass

def command_pull():
    pass

def command_validate():
    parser = argparse.ArgumentParser(description='Create a remote snapshot directory')
    parser.add_argument('command', type=str, help='The command (validate)')
    parser.add_argument('origin', type=str,  help='The name of the origin')
    #parser.add_argument('host',nargs='?', type=str,  help='the host of the origin (like www.something.com)')
    parser.add_argument('directory',nargs='?' , type=str,  help=' (optional) the directory to store the snapshot',default=None)
    args = parser.parse_args()
    args.directory = get_configs_dir(args.directory)
    config_dir = get_config_filepath(args.directory,args.origin)

    with open(config_dir,'r') as f:
        remote_config = json.loads(f.read())
    assert 'origin' in remote_config, f"Invalid remote {config_dir} file is missing 'origin' field"
    assert 'host' in remote_config, f"Invalid remote {config_dir} file is missing 'host' field"
    assert 'dir' in remote_config, f"Invalid remote {config_dir} file is missing 'dir' field"
    protocol_host = remote_config['host'].split("://")
    protocol = protocol_host[0].replace("/","")
    host = protocol_host[1].replace("/","")
    sb = BackupManager()
    early_stop = True
    file_types = ['json']
    results = sb.run(remote_config['dir'],host,protocol,'validate', file_types, early_stop)
    assert 'json' in results and results['json'] == True,"Could not run validation query"
    
    print(results)
def command_status():
    pass
def command_append():
    pass

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Not enough arguments provided.")
        sys.exit(1)

    # Extract the first two positional arguments
    command = sys.argv[1]
    commands = {
                'remote':command_remote,
                'push':command_push,
                'pull':command_pull,
                'validate':command_validate,
                'append':command_append,
                'status':command_status,
                }
    if not command in commands:
        print("Could not find command in "+ str(list(commands.keys())) )
    commands[command]()

# Git interface
# remote add origin ADDRESS
# remote remove origin ADDRESS
# push origin
# pull origin
# status origin
# validate origin
# validate origin


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