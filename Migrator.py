import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint
import pandas
from Messages import Messages

class Migrator():
    @staticmethod
    def is_directory(decw,connection_settings,hash):
        # Example pseudocode for IPFS. You'll need to adapt this based on your IPFS client library or command line usage
        _,ipfs_api = decw.net.create_ipfs_connection(connection_settings)
        object_info = ipfs_api.ls(hash)
        object_info = object_info['Objects'][0]
        # print(object_info)
        if object_info and 'Links' in object_info:
            print("Inspecting links")
            for link in object_info['Links']:
                if len(link['Name']) > 0:
                    return True
            # return len(object_info['Links']) > 0  # Has links, likely a directory
        return False  # No links, likely a file
    @staticmethod
    def find_batch_object_ids(decw,offset,limit,filter=None):
        if filter ==None:
            filter ={'attrib':{'file_type':'ipfs'}}
        filter['limit'] = limit
        filter['offset'] = offset
        docs = decw.net.list(filter)
        print("Searching for objs")
        # print(filter)
        # print(docs)
        obj_ids = []
        for doc in docs:
            obj_ids.append(doc['self_id'])
        return obj_ids
    @staticmethod
    def find_batch_cids(decw,offset,limit,filter=None):
        found = []
        if filter ==None:
            filter ={'attrib':{'file_type':'ipfs'}}
        filter['limit'] = limit
        filter['offset'] = offset
        docs = decw.net.list(filter)
        returned = len(docs)
        for doc in docs:
            if 'settings' in doc.keys():
                if 'ipfs_cid' in doc['settings'].keys():
                    rec = {"self_id":doc['self_id']}
                    rec['cid'] = doc['settings']['ipfs_cid']
                    found.append(rec)
                if 'ipfs_cids' in doc['settings'].keys():
                    for pin in doc['settings']['ipfs_cids'].values():
                        rec = {"self_id":doc['self_id'],"cid":pin}
                        found.append(rec)                
        return found,returned

    def decelium_has_cids(decw,new_cids):
        all_cids = Migrator.find_all_cids(decw)
        df = pandas.DataFrame(all_cids)
        all_cids = list(df['cid'])
        is_subset = set(new_cids) <= set(all_cids)
        return is_subset

    def ipfs_has_cids(decw,new_cids, connection_settings):
        all_cids = Migrator.ipfs_pin_list( connection_settings)
        is_subset = set(new_cids) <= set(all_cids)
        return is_subset

    @staticmethod
    def find_all_cids(decw,offset=0,limit=20): #find all cids on decelium
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = Migrator.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        print('')
        return found

    @staticmethod
    def find_download_entity(decw,offset=0,limit=20):
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = Migrator.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        return found

    def backup_ipfs_entity(item,current_pins,download_path,client,overwrite=False):
        new_cids = []
        cid = item['cid']
        file_path = os.path.join(download_path, cid)

        # Check if the file already exists to avoid double writing
        if (os.path.exists(file_path+".file") or os.path.exists(file_path+".dag")) and overwrite == False:
            print(f"CID {cid} already exists in {file_path}")
            return new_cids
        cids = current_pins
        if type(current_pins) == dict:
            cids = current_pins['Keys']
        try:
            # Check if the item is pinned on this node
            pinned = False
            if cid in cids:
                pinned = True
            if not pinned:
                print(f"CID {cid} IS NOT PINNED {file_path}")
                return new_cids

            # If pinned, proceed to download
            try:
                res = client.cat(cid)
                with open(file_path+".file", 'wb') as f:
                    f.write(res)
                print(f"Downloaded {cid} to file {file_path}")
            except Exception as e:
                if "is a directory" in str(e):
                    print(f"Downloaded {cid} to dir {file_path}")
                    dir_json = Migrator.backup_directory_dag(client,cid)
                    for new_item in dir_json['Links']:
                        #print(item)
                        #print(dir_json)
                        new_cids.append({'self_id':item['self_id'],'cid':new_item['Hash']})
                    # dir_json = client.object.get(cid)
                    # print(json.dumps(dict(dir_json)))
                    with open(file_path+".dag", 'w') as f:
                        f.write(json.dumps(dir_json))
                    print("Finished Directory")
                else:
                    raise e
            return new_cids
        except Exception as e:
            import traceback as tb
            print(f"Error downloading {cid}: {e}")
            print(tb.format_exc())
            return new_cids

    @staticmethod
    def ipfs_pin_list( connection_settings):
        c = connection_settings
        ipfs_string = f"/dns/{c['host']}/tcp/{c['port']}/{c['protocol']}"

        with ipfshttpclient.connect(ipfs_string) as client:
            try:
                pin_response = client.pin.ls(type='recursive')
                pins = pin_response['Keys']
                return pins
            except:
                print(f"Error checking pin status")
                return []
    def download_ipfs_data(docs, download_path, connection_settings,overwrite=False):
        c = connection_settings
        # Ensure the download directory exists
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        ipfs_string = f"/dns/{c['host']}/tcp/{c['port']}/{c['protocol']}"

        current_docs = docs
        next_batch = []
        #with ipfshttpclient.connect(ipfs_string) as client:
        #    try:
        #        pins = client.pin.ls(type='recursive')
        #    except Exception as pin_check_error:
        #        print(f"Error checking pin status for {cid}: {pin_check_error}")
        pins = Migrator.ipfs_pin_list( connection_settings)
        with ipfshttpclient.connect(ipfs_string) as client:
            while len(current_docs) > 0:
                for item in current_docs:
                    dic = item
                    if type(item) == str:
                        dic = {'cid':item,'self_id':None}
                    new_pins = Migrator.backup_ipfs_entity(dic,pins,download_path,client,overwrite)
                    if len(new_pins) > 0:
                        next_batch = next_batch + new_pins
                print("Moving to new batch-------------")
                current_docs = next_batch
                next_batch = []

    @staticmethod
    def upload_ipfs_data(decw,download_path,connection_settings):
        uploaded_something = False
        # for root, dirs, files in os.walk(download_path):
        #    for file in files:
        #    file_path = os.path.join(root, file)
        cids = [] 
        for item in os.listdir(download_path):
            # Construct the full path of the item-
            file_path = os.path.join(download_path, item)
            if file_path.endswith('.file'):
                payload_type = 'local_path'
            elif file_path.endswith('.dag'):
                payload_type = 'ipfs_pin_list'
            else:
                continue
            result = decw.net.create_ipfs({
                    'api_key':"UNDEFINED",
                    'file_type':'ipfs', 
                    'connection_settings':connection_settings,
                    'payload_type':payload_type,
                    'payload':file_path})
            
            messages = Messages("Migrator.upload_ipfs_data")
            messages.add_error(result[0]['cid'] in file_path,"Could not local file for "+result[0]['cid'] ) 
            cids.append(result[0]['cid'])
        return cids,messages

    @staticmethod
    def download_object(decw,object_ids,download_path,connection_settings, overwrite=False):
        if type(object_ids) == str:
            object_ids = [object_ids]
        results = {}
        for obj_id in object_ids:
            messages = Messages("Migrator.download_object(for {obj_id})")
            try:
                obj = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':obj_id,'attrib':True})

                assert messages.add_error('settings' in obj,"Settings not present in "+obj_id )
                assert messages.add_error('ipfs_cid' in obj['settings'],"Core IPFS CID not present in "+obj_id ) 
                new_cids = [obj['settings']['ipfs_cid']]
                if 'ipfs_cids' in obj['settings']:
                    for cid in obj['settings']['ipfs_cids'].values():
                        new_cids.append(cid)
                result = Migrator.download_ipfs_data(new_cids, download_path+'/'+obj_id, connection_settings,overwrite)
                # messages.append(download_messages)
                with open(download_path+'/'+obj_id+'/object.json','w') as f:
                    f.write(json.dumps(obj))
                results[obj_id] = (True,messages)
            except:
                import traceback as tb
                exc = tb.format_exc()
                with open(download_path+'/'+obj_id+'/object_error.txt','w') as f:
                    f.write(exc)
                messages.add_error(False,"Exception encountered for "+obj_id+": "+exc ) 
                results[obj_id] = (False,messages)
        return results

    @staticmethod
    def validate_local_against_remote_object(decw,object_id,download_path,connection_settings):
        messages = Messages("Migrator.validate_local_against_remote_object(for {object_id})")

        # Compares the local object with the remote
        obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})
        with open(download_path+'/'+object_id+'/object.json','r') as f:
            obj_local = json.loads(f.read())

        messages.add_error(obj_local['self_id'] == obj_remote['self_id'] ,"local.self_id is not identical to remote.self_id "+object_id )
        messages.add_error(obj_local['parent_id'] == obj_remote['parent_id'] ,"local.parent_id is not identical to remote.parent_id "+object_id )
        messages.add_error(obj_local['dir_name'] == obj_remote['dir_name'] ,"local.dir_name is not identical to remote.dir_name "+object_id )
        if messages.add_error('settings' in obj_local  and  'settings' in obj_remote 
                              ,"local or remote do not have settings "+object_id ):
            messages.add_error(obj_local['settings']['ipfs_cid'] == obj_remote['settings']['ipfs_cid'] 
                               ,"local.ipfs_cid is not identical to remote.ipfs_cid "+object_id )
            messages.add_error(obj_local['settings']['ipfs_name'] == obj_remote['settings']['ipfs_name'] 
                               ,"local.ipfs_name is not identical to remote.ipfs_name "+object_id )
        
        messages.add_error(obj_local['settings']['ipfs_cid'] == obj_remote['settings']['ipfs_cid'] ,"local.dir_name is not identical to remote.dir_name "+object_id )
        if 'ipfs_cids' in obj_local['settings']:
            for key in obj_local['settings']['ipfs_cids'].keys():
                messages.add_error(obj_local['settings']['ipfs_cids'][key] == obj_remote['settings']['ipfs_cids'][key] 
                                   ,"local mismatch in keys for "+object_id )
            for key in obj_remote['settings']['ipfs_cids'].keys():
                messages.add_error(obj_local['settings']['ipfs_cids'][key] == obj_remote['settings']['ipfs_cids'][key] 
                                   ,"remote mismatch in keys for "+object_id )
                
        for item in os.listdir(download_path+'/'+object_id):
            # Construct the full path of the item-
            file_path = os.path.join(download_path+'/'+object_id, item)
            if file_path.endswith('.file'):
                payload_type = 'local_path'
            elif file_path.endswith('.dag'):
                payload_type = 'ipfs_pin_list'
            else:
                continue
            messages.add_error(os.path.exists(file_path) == True 
                                ,"The local file does not exist "+object_id )        
                
        return len(messages.get_error_messages() == 0),messages

    @staticmethod
    def validate_local_object(decw,object_id,download_path,connection_settings):
        # Validate the local representation of an object
        with open(download_path+'/'+object_id+'/object.json','r') as f:
            obj_local = json.loads(f.read())

        cids_pinned = []
        cids_downloaded = []
        assert obj_local['self_id']  # CHANGEASSERT
        assert obj_local['parent_id'] # CHANGEASSERT
        assert obj_local['dir_name'] # CHANGEASSERT
        assert obj_local['settings']['ipfs_cid']  # CHANGEASSERT
        cids_pinned.append (obj_local['settings']['ipfs_cid'] )

        assert obj_local['settings']['ipfs_name']  # CHANGEASSERT
        if 'ipfs_cids' in obj_local['settings']:
            for key in obj_local['settings']['ipfs_cids'].keys():
                assert obj_local['settings']['ipfs_cids'][key]  # CHANGEASSERT
                cids_pinned.append (obj_local['settings']['ipfs_cids'][key] )
        
        for item in os.listdir(download_path+'/'+object_id):
            # Construct the full path of the item-
            file_path = os.path.join(download_path+'/'+object_id, item)
            if item.endswith('.file') or item.endswith('.dag'):
                cids_downloaded.append(item.split('.')[0])
        missing = []
        for pin in cids_pinned:
            try:
                assert pin in cids_downloaded # CHANGEASSERT
            except:
                missing.append(pin)
        if len(missing) > 0:
            raise Exception(download_path+'/'+object_id+" is missing "+str(len(missing))+" pins :"+str(missing) )
        return True
    
    @staticmethod
    def validate_remote_object(decw,object_id,download_path,connection_settings,obj_remote = None):
        # Compares the local object with the remote
        if obj_remote == None:
            obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})

        assert obj_remote['self_id'] == obj_remote['self_id'] # CHANGEASSERT
        assert obj_remote['parent_id'] == obj_remote['parent_id'] # CHANGEASSERT
        assert obj_remote['dir_name'] == obj_remote['dir_name'] # CHANGEASSERT
        assert obj_remote['settings']['ipfs_cid'] == obj_remote['settings']['ipfs_cid'] # CHANGEASSERT
        cids = [obj_remote['settings']['ipfs_cid']]
        assert obj_remote['settings']['ipfs_name'] == obj_remote['settings']['ipfs_name'] # CHANGEASSERT
        if 'ipfs_cids' in obj_remote['settings']:
            for key in obj_remote['settings']['ipfs_cids'].keys():
                assert obj_remote['settings']['ipfs_cids'][key] # CHANGEASSERT
            cids.append(obj_remote['settings']['ipfs_cids'][key])
        for cid in cids:
            result = decw.net.check_pin_status({
                    'api_key':"UNDEFINED",
                    'connection_settings':connection_settings,
                    'cid': cid})
            assert result == True # CHANGEASSERT

        return True

    @staticmethod
    def upload_object_query(decw,obj_id,download_path,connection_settings):
        '''
            Validates the object, and generates a query to reupload the exact object
        '''
        if not os.path.isfile(download_path+'/'+obj_id+'/object.json'):
            return {"error":"could not fine object.json in the selected path"}
            
        with open(download_path+'/'+obj_id+'/object.json','r') as f:
            obj = json.loads(f.read()) 
        assert 'settings' in obj # CHANGEASSERT
        assert 'ipfs_cid' in obj['settings'] # CHANGEASSERT
        assert 'ipfs_cids' in obj['settings'] # CHANGEASSERT
        #obj_cids = [obj['settings']['ipfs_cid']]
        obj_cids = []
        for path,cid in obj['settings']['ipfs_cids'].items():
            cid_record = { 'cid':cid,
                           'name':path }
            cid_record['name'] = cid_record['name'].replace(obj_id+'/',"")
            if len(path) > 0:
                cid_record['root'] = True
            obj_cids.append(cid_record)
            print(cid_record)
            assert os.path.isfile(download_path+'/'+obj_id+'/'+cid+'.file') or os.path.isfile(download_path+'/'+obj_id+'/'+cid+'.dag')   # CHANGEASSERT          
        #assert Migrator.upload_ipfs_data(decw,download_path+'/'+obj_id,connection_settings) == True
        # TODO - create a Types package to share types
        # Create entity can take instances of itself as an upload. (Or, perhaps there is a "restore" entity)
        # import pprint
        # pprint.pprint(obj) 
        query ={
            'parent_id':obj['parent_id'],
            'self_id':obj['self_id'],
            'name':obj['dir_name'],
            'file_type':'ipfs',
            'payload_type':'ipfs_pin_list',
            'payload':obj_cids}
        return query

    @staticmethod
    def backup_directory_dag(client, cid, path=""):
        item_details_response = client.object.get(cid)
        item_details = {
            'Links': [{
                'Name': link['Name'],
                'Hash': link['Hash'],
                'Size': link['Size']
            } for link in item_details_response['Links']]
        }

        return item_details