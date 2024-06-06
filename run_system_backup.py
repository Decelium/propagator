import decelium_wallet.core as core
from Snapshot import Snapshot
import os,json

class SystemBackup():

    def setup(self):
        wallet_path= '../.wallet.dec'
        wallet_password_path =  '../.wallet.dec.password'
        node_url =  'http://devdecelium.com:5000/data/query'
        decw = core()
        decw:core.core = core()
        loaded = decw.load_wallet(decw.rd_path(wallet_path),decw.rd_path(wallet_password_path))

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

        connected = decw.initial_connect(target_url=node_url, api_key=decw.dw.pubk())
        assert loaded == True
        assert connected == True
        self.decw = decw
        return True
    
    def backup_status(self,file_type,backup_path,early_stop = False):
        import pprint
        validation_report = self.load_validation_report(file_type,backup_path,early_stop)
        #pprint.pprint(validation_report)
        complete = {}
        pushable = {}
        pullable = {}
        repairable = {}
        corrupt = {}
        for k in validation_report.keys():
            item:dict = validation_report[k]
            r =  item['remote']
            l =  item['local']
            rm =  item['remote_mirror']
            #repair_status = self.decw.net.repair_entity({'api_key':'UNDEFINED','self_id':item['self_id']})
            if rm == True and r == True and l == True:
                complete[k] = item.copy()
            if r == False and l == True:
                pushable[k] = item.copy()
            if r == True and l == False:
                pullable[k] = item.copy()
            if r != rm:
                repairable[k] = item.copy()
            if r == rm and rm == l and l == False:
                corrupt[k] = item.copy()
        summary = {
                'summary':{
                    'len_complete':len(complete),
                    'len_pushable':len(pushable),
                    'len_pullable':len(pullable),
                    'len_repairable':len(repairable),
                    'len_corrupt':len(corrupt)
                },
                'complete':complete,
                'pushable':pushable,
                'pullable':pullable,
                'repairable':repairable,
                'corrupt':corrupt
                }
        return summary
    
    def load_validation_report(self,file_type,backup_path,early_stop = False):
        with open(os.path.join(backup_path,'validation_report_latest.json'),'r') as f:
            content = json.loads(f.read())
        return content
    
    def purge_corrupt(self,file_type,backup_path,early_stop = False):
        validation_report = self.backup_status(file_type,backup_path,early_stop)
        assert 'corrupt' in  validation_report
        for item in validation_report['corrupt'].values():
            print(item)
            assert item['local'] == False
            assert item['remote'] == False
            assert item['remote_mirror'] == False
            #assert Snapshot.remove_entity({'self_id':item['self_id']},backup_path) == True
            #
            file_datasoruce = Snapshot.get_datasource(file_type,"local")
            assert file_datasoruce.remove_entity({'self_id':item['self_id']},backup_path) == True
            del_req = self.decw.dw.sign_request({'self_id':item['self_id'],'api_key':self.decw.dw.pubk(),'as_node_admin':True},["admin"])
            del_resp =  self.decw.net.delete_entity(del_req)
            print(del_resp)
            assert del_resp == True or ('error' in del_resp and 'could not find' in del_resp['error'])
            print("Purged "+item['self_id'])
        print("Finished purge")
        #self.create_validation_report(self,file_type,backup_path,early_stop)
        #print("Finished New report")
        #return True
    
    def create_validation_report(self,file_type,backup_path,early_stop = False):
        import pprint
        filter = {'attrib':{'file_type':file_type}}
        chunk_size = 20
        limit = chunk_size+1
        offset = 0
        res_new = Snapshot.validate_snapshot(self.decw, self.connection_settings, backup_path,limit, offset)
        for k in res_new:
            del res_new[k]['local_error']
            del res_new[k]['local_message']
            del res_new[k]['remote_error']
            del res_new[k]['remote_message']
            del res_new[k]['remote_mirror_error']
            del res_new[k]['remote_mirror_message']
        
        if early_stop == True:
            return res_new
        '''
            ...
            'obj-06dbfa8e-d534-45ae-b569-ea3870711653': {'local': False,
                                                        'remote': False,
                                                        'remote_mirror': False,
                                                        'self_id': 'obj-06dbfa8e-d534-45ae-b569-ea3870711653'},   
            ...     
        '''
        res = {}
        res.update(res_new)
        while len(res_new) >= chunk_size:
            offset = offset + chunk_size
            print("Running "+ str(offset))

            res_new = Snapshot.validate_snapshot(self.decw, self.connection_settings, backup_path,limit, offset)
            for k in res_new:
                del res_new[k]['local_error']
                del res_new[k]['local_message']
                del res_new[k]['remote_error']
                del res_new[k]['remote_message']
                del res_new[k]['remote_mirror_error']
                del res_new[k]['remote_mirror_message']
            res.update(res_new)
        with open(os.path.join(backup_path,'validation_report_latest.json'),'w') as f:
            f.write(json.dumps(res,indent=1))
        return True

    
    def backup_all_type(self,file_type,backup_path,early_stop = False):
        import pprint
        filter = {'attrib':{'file_type':file_type}}
        chunk_size = 20
        limit = chunk_size + 1
        offset = 0
        res = Snapshot.append_from_remote(self.decw, self.connection_settings, backup_path, limit, offset,filter)
        if early_stop == True:
            return res
        while len(res) >= chunk_size:
            offset = offset + chunk_size
            print(f"RUNNING {offset} {limit}")
            res = Snapshot.append_from_remote(self.decw, self.connection_settings, backup_path, limit, offset,filter)
        return res
    

    def run(self,job_id,file_types,early_stop=False):
        import pprint
        func = None
        if job_id == 'validation':
            func = self.create_validation_report
        if job_id == 'backup':
            func = self.backup_all_type
        if job_id == 'status':
            func = self.backup_status
        if job_id == 'purge_corrupt':
            func = self.purge_corrupt
        self.setup()
        results = {}
        for type in file_types:
            result = func(type,'../devdecelium_systembackup/'+type+"/",early_stop)
            results[type] = result
            if result == True:
                print("SUCCESS" + type)
            else: 
                print("FAILURE")
            if early_stop:
                break
        return results
       
import pprint
sb = SystemBackup()
early_stop = False
file_types = ['ipfs','json','file','host','directory','user']
file_types = ['ipfs']

# sb.run_backup(file_types, early_stop)
#sb.run('repair',file_types, early_stop)
results = sb.run('purge_corrupt',file_types, early_stop)
#pprint.pprint(results['ipfs']['summary'])

# sb.run('validation',file_types, early_stop)

# - Purge the corrupt local and on server
# - repair the repairable
# - pull the pullable
# - push the pushable

