'''
import decelium_wallet.core as core
from Snapshot import Snapshot
import os,json
from type.BaseData import BaseData
import time
import pprint, argparse
from BackupManager import BackupManager

def main(dir,host,protocol,mode, types,self_id):
    sb = BackupManager()
    early_stop = False
    file_types = types.split(',')
    results = sb.run(dir=dir,host=host,
                     protocol=protocol,
                     job_id=mode,
                     file_types= file_types, 
                     early_stop=early_stop,
                     self_id=self_id)
    pprint.pprint(results)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run SystemBackup with specified mode and files.')
    parser.add_argument('--self_id', default="", type=str, help='Object Id')
    parser.add_argument('--mode', type=str, help='The mode to run the SystemBackup in')
    parser.add_argument('--types', type=str, help='Comma-separated list of file types')
    parser.add_argument('--host', type=str, help='The host URL of the remote node',default='devdecelium.com')
    parser.add_argument('--protocol', type=str, help='The protocol to use',default='http')
    parser.add_argument('--dir', type=str, help='The local dir to use',default='../devdecelium.com_systembackup')

    args = parser.parse_args()
    main(args.dir,args.host,args.protocol,args.mode, args.types,args.self_id)

    # python3 run_system_backup.py --mode "backup" --types attrib --host devdecelium.com --protocol http --dir ./backup_temp_test
'''

import decelium_wallet.core as core
from Snapshot import Snapshot
import os, json
from type.BaseData import BaseData
import time
import pprint
from BackupManager import BackupManager
from decelium_wallet.commands.BaseService import BaseService  # Assuming your BaseService from earlier

class SystemBackupService(BaseService):
    @classmethod
    def run(cls, **kwargs):
        """
        This method overrides BaseService's exec to handle the actual system backup logic.
        Arguments come in through kwargs, and this method interacts with BackupManager to perform the backup.
        """

        # Extract the necessary arguments from kwargs
        dir = kwargs.get('dir', '../devdecelium.com_systembackup')
        host = kwargs.get('host', 'devdecelium.com')
        protocol = kwargs.get('protocol', 'http')
        mode = kwargs.get('mode')
        types = kwargs.get('types', '')
        self_id = kwargs.get('self_id', '')

        # BackupManager setup
        sb = BackupManager()
        early_stop = True
        file_types = types.split(',') if types else []

        # Run the backup operation
        results = sb.run(
            dir=dir,
            host=host,
            protocol=protocol,
            job_id=mode,
            file_types=file_types,
            early_stop=early_stop,
            self_id=self_id
        )

        # Pretty print the results
        pprint.pprint(results)
        return 0  # Return success code


# Example usage in case of direct execution
if __name__ == "__main__":
    SystemBackupService.run_cli()  # Inherit CLI behavior from BaseService


#python3 run_system_backup.py mode="backup" types=ipfs host=devdecelium.com protocol=http dir=./backup_temp_test