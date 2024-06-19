import decelium_wallet.core as core
try:
    from Snapshot import Snapshot
    from type.BaseData import BaseData
except:
    from .Snapshot import Snapshot
    from .type.BaseData import BaseData
    
import os,json
import time

class BackupManager():
    def setup(self,host='devdecelium.com',
              protocol='http',
              wallet_path= '../.wallet.dec',
              wallet_password_path =  '../.wallet.dec.password', 
              url =  ':5000/data/query',
             decw_in=None):
        node_url =  f'{protocol}://{host}:{url}'
        if decw_in:
            decw = decw_in
            loaded=True
        else:
            decw = core()
            loaded = decw.load_wallet(decw.rd_path(wallet_path),decw.rd_path(wallet_password_path))

        try:
            self.user_context = {
                    'api_key':decw.dw.pubk()
            }
        except:
            self.user_context = {
                    'api_key':"UNDEFINED"
            }
            
        self.connection_settings = {'host': host,
                                'port':5001,
                                'protocol':'http'
        }
        self.ipfs_req_context = {**self.user_context, **{
                'file_type':'ipfs', 
                'connection_settings':self.connection_settings
        }}

        connected = decw.initial_connect(target_url=node_url, api_key=self.user_context['api_key'])
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
        with open(os.path.join(backup_path,file_type+'_validation_report_latest.json'),'r') as f:
            content = json.loads(f.read())
        return content
    
    def purge_corrupt(self,file_type,backup_path,early_stop = False):
        validation_report = self.backup_status(file_type,backup_path,early_stop)
        assert 'corrupt' in  validation_report
        for item in validation_report['corrupt'].values():
            if not self.decw.has_entity_prefix(item['self_id']):
                continue

            assert item['local'] == False
            assert item['remote'] == False
            assert item['remote_mirror'] == False
            #assert Snapshot.remove_entity({'self_id':item['self_id']},backup_path) == True
            #
            file_datasoruce = Snapshot.get_datasource(file_type,"local")
            result = file_datasoruce.remove_entity({'self_id':item['self_id']},backup_path) 
            print(result)
            assert result == True
            del_req = self.decw.dw.sign_request({'self_id':item['self_id'],'api_key':self.decw.dw.pubk(),'as_node_admin':True},["admin"])
            del_resp =  self.decw.net.delete_entity(del_req)
            print(del_resp)
            assert del_resp == True or ('error' in del_resp and 'could not find' in del_resp['error'])
            print("Purged "+item['self_id'])
        print("Finished purge")

    def repair(self,file_type,backup_path,early_stop = False):
        validation_report = self.backup_status(file_type,backup_path,early_stop)
        assert 'repairable' in  validation_report
        #assert 'pushable' in  validation_report
        for k in ['repairable']:
            for item in validation_report[k].values():
                if not self.decw.has_entity_prefix(item['self_id']):
                    continue

                print(k)
                print(item)
                assert item['remote'] == False or item['remote_mirror'] == False
                assert item['remote'] == True or item['remote_mirror'] == True

                req = self.decw.dw.sign_request({'self_id':item['self_id'],'api_key':self.decw.dw.pubk()},["admin"])
                repair_resp =  self.decw.net.repair_entity(req)
                if type(repair_resp) == dict and 'error' in repair_resp:
                    print("Retry Repair... Sometimes IPFS can be slow.")
                    time.sleep(3)
                    repair_resp =  self.decw.net.repair_entity(req)

                assert repair_resp == True, "Could not repair "+str(repair_resp)
                print("Repaired "+item['self_id'])
        print("Finished repair")    
    
    def create_validation_report(self,file_type,backup_path,early_stop = False):
        import pprint
        filter = {'attrib':{'file_type':file_type}}
        chunk_size = 20
        limit = chunk_size+1
        offset = 0
        show_errors = True 
        
        res_new = Snapshot.validate_snapshot(decw=self.decw, 
                                             connection_settings=self.connection_settings, 
                                             download_path=backup_path,
                                             limit=limit, 
                                             offset=offset,
                                             filter=filter)
        #res_new = Snapshot.validate_snapshot(self.decw, self.connection_settings, backup_path,limit, offset,filter)
        if show_errors == False:
            for k in res_new:
                del res_new[k]['local_error']
                del res_new[k]['local_message']
                del res_new[k]['remote_error']
                del res_new[k]['remote_message']
                del res_new[k]['remote_mirror_error']
                del res_new[k]['remote_mirror_message']
        
        if early_stop == True:
            with open(os.path.join(backup_path,file_type+'_validation_report_latest.json'),'w') as f:
                f.write(json.dumps(res_new,indent=1))
            print("EARLY STOP")
            return True
        
        res = {}
        res.update(res_new)
        print(len(res_new))
        while len(res_new) >= chunk_size:
            offset = offset + chunk_size
            print("Running "+ str(offset))

            res_new = Snapshot.validate_snapshot(self.decw, self.connection_settings, backup_path,limit, offset)

            if show_errors == False:
                for k in res_new:
                    del res_new[k]['local_error']
                    del res_new[k]['local_message']
                    del res_new[k]['remote_error']
                    del res_new[k]['remote_message']
                    del res_new[k]['remote_mirror_error']
                    del res_new[k]['remote_mirror_message']
            res.update(res_new)
        print("FULL DATA SET")
        with open(os.path.join(backup_path,file_type+'_validation_report_latest.json'),'w') as f:
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
    
    def push(self,file_type,backup_path,early_stop = False):
        import pprint
        filter = {'attrib':{'file_type':file_type}}
        chunk_size = 20
        limit = chunk_size + 1
        offset = 0
        res = Snapshot.push_to_remote(self.decw, self.connection_settings, backup_path, limit, offset,filter)
        if early_stop == True:
            return res
        while len(res) >= chunk_size:
            offset = offset + chunk_size
            print(f"RUNNING {offset} {limit}")
            res = Snapshot.push_to_remote(self.decw, self.connection_settings, backup_path, limit, offset,filter)
        return res    

    def pull(self,file_type,backup_path,early_stop = False):
        validation_report = self.backup_status(file_type,backup_path,early_stop)
        assert 'pullable' in  validation_report
        res = {}
        for item in validation_report['pullable'].values():
            if not self.decw.has_entity_prefix(item['self_id']):
                continue
            print("PULLING")
            filter = {'attrib':{'self_id':item['self_id']}}
            chunk_size = 20
            filter = {'attrib':{'self_id':item['self_id']}}
            overwrite = True
            res_add = Snapshot.append_from_remote(self.decw, self.connection_settings, backup_path,1, 0,filter,overwrite,api_key=self.decw.dw.pubk(),attrib=False)      
            res.update(res_add)
        return res   

    def run(self,dir,host,protocol,job_id,file_types,early_stop=False,use_type_dir=True,decw_in=None):
        print(job_id,file_types)
        func = None
        if job_id == 'validate':
            func = self.create_validation_report
        if job_id == 'backup':
            func = self.backup_all_type
        if job_id == 'append':
            func = self.backup_all_type
        if job_id == 'status':
            func = self.backup_status
        if job_id == 'purge_corrupt':
            func = self.purge_corrupt
        if job_id == 'push':
            func = self.push
        if job_id == 'repair':
            func = self.repair
        if job_id == 'pull':
            func = self.pull
        self.setup(host=host,protocol=protocol,decw_in=decw_in)
        results = {}
        
        for type in file_types:
            print(dir)
            print(type)
            if use_type_dir == True:
                type_path = os.path.join(dir,type)
            else:
                type_path = os.path.join(dir)
                
            result = func(type,type_path,early_stop)
            results[type] = result
            if early_stop:
                break
        return results