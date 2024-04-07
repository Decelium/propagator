import decelium_wallet.core as core
from Snapshot import Snapshot
from actions.SnapshotActions import CreateDecw

class SystemBackup():

    def __init__(self):
        self.memory = {}
        decw, connected = CreateDecw().run({
            'wallet_path': '../.wallet.dec',
            'wallet_password_path':'../.wallet.dec.password',
            'fabric_url': 'https://dev.paxfinancial.ai/data/query',
            },self.memory)
        self.decw = decw
        self.user_context = {
                'api_key':decw.dw.pubk()
        }
        self.connection_settings = {'host': "devdecelium.com",
                                'port':5001,
                                'protocol':"http"
        }
        self.ipfs_req_context = {**self.user_context, **{
                'file_type':'ipfs', 
                'connection_settings':self.connection_settings
        }}
        self.decelium_path = 'temp/test_folder.ipfs'
        self.backup_path = '../devdecelium_backup/'
        # local_test_folder = './test/testdata/test_folder'
        # --- Remove old snapshot #
        #backup_path = "../test/system_backup_test"
        #try:
        #    shutil.rmtree(backup_path)
        #except:
        #    pass

    def run(self):
        #filter = {'attrib':{'self_id':record['obj_id']}}
        filter = {'attrib':{'file_type':'ipfs'}}
        limit = 20
        offset = 0
        res = Snapshot.append_from_remote(self.decw, self.connection_settings, self.backup_path, limit, offset,filter)
        print(len(res))
        while len(res) == 20:
            offset = offset + 20
            print(f"RUNNING {offset} {limit}")
            res = Snapshot.append_from_remote(self.decw, self.connection_settings, self.backup_path, limit, offset,filter)


sb = SystemBackup()
sb.run()