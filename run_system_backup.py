import decelium_wallet.core as core
from Snapshot import Snapshot
import os

class SystemBackup():

    def setup(self):
        wallet_path= '../.wallet.dec',
        wallet_password_path =  '../.wallet.dec.password'
        node_url =  'https://dev.devdecelium.com/data/query',
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

        decw:core.core = core()
        loaded = decw.load_wallet(decw.rd_path(wallet_path),decw.rd_path(wallet_password_path))
        connected = decw.initial_connect(target_url=node_url, api_key=decw.dw.pubk())
        assert loaded == True
        assert connected == True
        self.decw = decw
        return True

    def update_snapshot(self,backup_path='../devdecelium_backup/'):
        if not os.
        filter = {'attrib':{'file_type':'ipfs'}}
        limit = 20
        offset = 0
        res = Snapshot.append_from_remote(self.decw, self.connection_settings, self.backup_path, limit, offset,filter)
        print(len(res))
        while len(res) == 20:
            offset = offset + 20
            print(f"RUNNING {offset} {limit}")
            res = Snapshot.append_from_remote(self.decw, self.connection_settings, self.backup_path, limit, offset,filter)
            break

sb = SystemBackup()
sb.run()