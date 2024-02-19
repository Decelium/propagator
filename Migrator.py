import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint

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
        print(filter)
        print(docs)
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

    def backup_ipfs_entity(item,current_pins,download_path,client):
        new_cids = []
        cid = item['cid']
        file_path = os.path.join(download_path, cid)

        # Check if the file already exists to avoid double writing
        if os.path.exists(file_path+".file") or os.path.exists(file_path+".dag"):
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
                    print(json.dumps(dict(dir_json)))
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
                print(f"Error checking pin status for {cid}: {pin_check_error}")
                return []
    def download_ipfs_data(docs, download_path, connection_settings):
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
                    new_pins = Migrator.backup_ipfs_entity(dic,pins,download_path,client)
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
            print(file_path)
            print(result)
            assert result[0]['cid'] in file_path
            cids.append(result[0]['cid'])
        return cids

    @staticmethod
    def download_object(decw,object_ids,download_path,connection_settings):
        if type(object_ids) == str:
            object_ids = [object_ids]
        results = {}
        for obj_id in object_ids:
            obj = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':obj_id,'attrib':True})
            assert 'settings' in obj
            assert 'ipfs_cid' in obj['settings']
            assert 'ipfs_cids' in obj['settings']
            new_cids = [obj['settings']['ipfs_cid']]

            for cid in obj['settings']['ipfs_cids'].values():
                new_cids.append(cid)
            Migrator.download_ipfs_data(new_cids, download_path+'/'+obj_id, connection_settings)
            with open(download_path+'/'+obj_id+'/object.json','w') as f:
                f.write(json.dumps(obj))
            results[obj_id] = True
        return results

    @staticmethod
    def validate_backedup_object(decw,object_id,download_path,connection_settings):
        # Compares the local object with the remote
        obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})
        with open(download_path+'/'+object_id+'/object.json','r') as f:
            obj_local = json.loads(f.read())
        assert obj_local['self_id'] == obj_remote['self_id']
        assert obj_local['parent_id'] == obj_remote['parent_id']
        assert obj_local['dir_name'] == obj_remote['dir_name']
        assert obj_local['settings']['ipfs_cid'] == obj_remote['settings']['ipfs_cid']
        assert obj_local['settings']['ipfs_name'] == obj_remote['settings']['ipfs_name']
        for key in obj_local['settings']['ipfs_cids'].keys():
            assert obj_local['settings']['ipfs_cids'][key] ==   obj_remote['settings']['ipfs_cids'][key]
        for key in obj_remote['settings']['ipfs_cids'].keys():
            assert obj_remote['settings']['ipfs_cids'][key] ==   obj_local['settings']['ipfs_cids'][key]
        for item in os.listdir(download_path+'/'+object_id):
            # Construct the full path of the item-
            file_path = os.path.join(download_path+'/'+object_id, item)
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
            assert result[0]['cid'] in file_path
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
        assert 'settings' in obj
        assert 'ipfs_cid' in obj['settings']
        assert 'ipfs_cids' in obj['settings']
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
            assert os.path.isfile(download_path+'/'+obj_id+'/'+cid+'.file') or os.path.isfile(download_path+'/'+obj_id+'/'+cid+'.dag')            
        #assert Migrator.upload_ipfs_data(decw,download_path+'/'+obj_id,connection_settings) == True
        # TODO - create a Types package to share types
        # Create entity can take instances of itself as an upload. (Or, perhaps there is a "restore" entity)
        import pprint
        pprint.pprint(obj) 
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