import decelium_wallet.core as core
import pandas
try:
    from Messages import ObjectMessages
except:
    from ..Messages import ObjectMessages
import traceback as tb
import ipfshttpclient

class TpIPFSDecelium():
    @classmethod
    def download_ipfs_data(cls,TpDestination,decw,cids, download_path, connection_settings,overwrite=False):
        c = connection_settings
        ipfs_string = f"/dns/{c['host']}/tcp/{c['port']}/{c['protocol']}"

        current_docs = cids
        next_batch = []

        all_pins = cls.ipfs_pin_list(decw, connection_settings)
        with ipfshttpclient.connect(ipfs_string) as client:
            while len(current_docs) > 0:
                for item in current_docs:
                    dic = None
                    if type(item) == dict:
                        dic = item.copy()
                    if type(item) == str:
                        dic = {'cid':item,'self_id':None}
                    if not dic['cid'] in all_pins:
                        print("REFRESHING PINS, as it seems we could be missing one")
                        all_pins = cls.ipfs_pin_list(decw, connection_settings,refresh=True)    
                        
                    new_pins = TpDestination.backup_ipfs_entity(TpIPFSDecelium,dic,all_pins,download_path,client,overwrite)
                    if len(new_pins) > 0:
                        next_batch = next_batch + new_pins
                current_docs = next_batch
                next_batch = []

    @classmethod
    def get_cid_read_stream(cls,client,root_cid):
        return client.cat(root_cid, stream=True)    
    
    @classmethod
    def load_entity(cls,query,decw):
        assert 'api_key' in query
        assert 'self_id' in query or 'path' in query 
        assert 'attrib' in query
        return decw.net.download_entity(query)

    @classmethod
    def download_directory_dag(cls,client, cid, path=""):
        item_details_response = client.object.get(cid)
        item_details = {
            'Links': [{
                'Name': link['Name'],
                'Hash': link['Hash'],
                'Size': link['Size']
            } for link in item_details_response['Links']]
        }
        return item_details

    @classmethod
    def validate_remote_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        print("validate_remote_object.Evaluating Remote Attrib")
        entity_success,entity_messages = cls.validate_remote_object_attrib(decw,object_id,download_path,connection_settings)
        print(entity_success)
        print("validate_remote_object.Evaluating Remote Payload")
        payload_success,payload_messages = cls.validate_remote_object_payload(decw,object_id,download_path,connection_settings)
        print(payload_success)
        entity_messages:ObjectMessages = entity_messages
        all_messages:ObjectMessages = payload_messages
        all_messages.append(entity_messages)
        return entity_success and payload_success,all_messages
    
    @classmethod
    def validate_remote_object_mirror(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        entity_success,entity_messages = cls.validate_remote_object_attrib_mirror(decw,object_id,download_path,connection_settings)
        payload_success,payload_messages = cls.validate_remote_object_payload_mirror(decw,object_id,download_path,connection_settings)
        entity_messages:ObjectMessages = entity_messages
        all_messages:ObjectMessages = payload_messages
        all_messages.append(entity_messages)
        return entity_success and payload_success,all_messages
    
    @classmethod
    def validate_remote_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("TpIPFSDecelium.validate_remote_object_entity(for {"+object_id+"})")
        obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
        if messages.add_assert(obj_valid == True, f"{object_id} seems to be invalid, as reported by DB validate_remote_object_entity:"+str(obj_valid)) == False:
            return False, messages
        return len(messages.get_error_messages()) == 0, messages      

    @classmethod
    def validate_remote_object_attrib_mirror(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("TpIPFSDecelium.validate_remote_object_entity_mirror(for {"+object_id+"})")
        obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id,'mirror':True})
        if messages.add_assert(obj_valid == True, f"{object_id} seems to be invalid, as reported by DB validate_remote_object_entity_mirror:"+str(obj_valid)) == False:
            return False, messages
        return len(messages.get_error_messages()) == 0, messages      

    @classmethod
    def validate_remote_object_payload_mirror(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("TpIPFSDecelium.validate_remote_object_entity_payload(for {"+object_id+"})")
        obj_valid = decw.net.validate_payload( {'api_key':'UNDEFINED', 'self_id':object_id,'mirror':True})
        if messages.add_assert(obj_valid == True, f"{object_id} seems to be invalid, as reported by DB validate_remote_object_payload_mirror:"+str(obj_valid)) == False:
            return False, messages
        return len(messages.get_error_messages()) == 0, messages      


    @classmethod
    def validate_remote_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("TpIPFSDecelium.validate_remote_object_payload(for {"+object_id+"})")
        if obj_remote == None:
            obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})
        print("LOW LEVEL TpIPFSDecelium.validate_remote_object_payload")
        
        cids_pinned = []
        for k in ['self_id','parent_id','dir_name','settings']:
            if messages.add_assert(k in obj_remote and obj_remote[k] != None, "missing {k} for {object_id}") == False:
                return False, messages
        
        if messages.add_assert('ipfs_cid' in obj_remote['settings'], "missing settings.ipfs_cid for {object_id}"):
            cids_pinned.append (obj_remote['settings']['ipfs_cid'] )

        messages.add_assert('ipfs_name' in obj_remote['settings'], "missing settings.ipfs_name for {object_id}")
        if 'ipfs_cids' in obj_remote['settings']:
            for key in obj_remote['settings']['ipfs_cids'].keys():
                if messages.add_assert(key in obj_remote['settings']['ipfs_cids'], "missing {key} from settings.ipfs_cids for {object_id}"):
                    cids_pinned.append (obj_remote['settings']['ipfs_cids'][key] )
        
        for cid in cids_pinned:
            result = decw.net.check_pin_status({
                    'api_key':"UNDEFINED",
                    'connection_settings':connection_settings,
                    'cid': cid,
                    'do_refresh':True})
            messages.add_assert(result == True, "cid is missing from remote "+cid)

        return len(messages.get_error_messages()) == 0, messages  

    @classmethod
    def ipfs_pin_list(cls,decw, connection_settings,refresh=False):
        pins = decw.net.download_pin_status({
                'api_key':"UNDEFINED",
                'do_refresh':refresh,
                'connection_settings':connection_settings})       
        assert not 'error' in pins
        return pins
    
    @classmethod
    def upload_path_to_ipfs(cls,decw,connection_settings,payload_type,file_path):
        result = decw.net.create_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':payload_type,
                'payload':file_path})
        return result
    
    @classmethod
    def find_download_entity(cls,decw,offset=0,limit=20):
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = cls.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        return found
        
    @classmethod
    def is_directory(cls,decw,connection_settings,hash):
        # Example pseudocode for IPFS. You'll need to adapt this based on your IPFS client library or command line usage
        _,ipfs_api = decw.net.create_ipfs_connection(connection_settings)
        object_info = ipfs_api.ls(hash)
        object_info = object_info['Objects'][0]
        
        if object_info and 'Links' in object_info:
            for link in object_info['Links']:
                if len(link['Name']) > 0:
                    return True
            # return len(object_info['Links']) > 0  # Has links, likely a directory
        return False  # No links, likely a file    pass
    
    @classmethod
    def find_batch_object_ids(cls,decw,offset,limit,filter=None):
        if filter ==None:
            filter ={'attrib':{'file_type':'ipfs'}}
        filter['limit'] = limit
        filter['offset'] = offset
        docs = decw.net.list(filter)
        obj_ids = []
        for doc in docs:
            obj_ids.append(doc['self_id'])
        return obj_ids
    
    @classmethod
    def find_batch_cids(cls,decw,offset,limit,filter=None):
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
    
    @classmethod
    def decelium_has_cids(cls,decw,new_cids):
        all_cids = cls.find_all_cids(decw)
        df = pandas.DataFrame(all_cids)
        all_cids = list(df['cid'])
        is_subset = set(new_cids) <= set(all_cids)
        return is_subset
    
    @classmethod
    def ipfs_has_cids(cls,decw,new_cids, connection_settings,refresh=False):
        all_cids = cls.ipfs_pin_list(decw, connection_settings,refresh)
        is_subset = set(new_cids) <= set(all_cids)
        #if not is_subset:
        #    print("Missing some CIDS from subset")
        #    print(set(new_cids) - set(all_cids))
        return is_subset
    
    @classmethod
    def find_all_cids(cls,decw,offset=0,limit=20): #find all cids on decelium
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = cls.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        return found