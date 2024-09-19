import decelium_wallet.core as core
import pandas
try:
    from Messages import ObjectMessages
except:
    from ..Messages import ObjectMessages
import traceback as tb
import ipfshttpclient
from .DsGeneral import DsGeneral
import json
import datetime

class jsondateencode_local:
    def loads(dic):
        return json.loads(dic,object_hook=jsondateencode_local.datetime_parser)
    def dumps(dic):
        return json.dumps(dic,default=jsondateencode_local.datedefault)

    def datedefault(o):
        if isinstance(o, tuple):
            l = ['__ref']
            l = l + o
            return l
        if isinstance(o, (datetime.date, datetime.datetime,)):
            return o.isoformat()

    def datetime_parser(dct):
        DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
        for k, v in dct.items():
            if isinstance(v, str) and "T" in v:
                try:
                    dct[k] = datetime.datetime.strptime(v, DATE_FORMAT)
                except:
                    pass
        return dct

class DsGeneralDecelium(DsGeneral):
    @classmethod
    def download_ipfs_data(cls,TpDestination,decw,cids, download_path, connection_settings,overwrite=False,failure_limit=5):
        c = connection_settings
        ipfs_string = f"/dns/{c['host']}/tcp/{c['port']}/{c['protocol']}"
        print(ipfs_string)
        current_docs = cids
        next_batch = []

        all_pins = cls.ipfs_pin_list(decw, connection_settings)
        failures = 0
        count = 0
        print("A")
        length = len(current_docs) 
        print("B")

        with ipfshttpclient.connect(ipfs_string,headers={'X-Api-Token':'your_secret_token'}) as client:
            for item in current_docs:
                count = count + 1
                dic = None
                if type(item) == dict:
                    dic = item.copy()
                if type(item) == str:
                    dic = {'cid':item,'self_id':None}
                if not dic['cid'] in all_pins:
                    all_pins = cls.ipfs_pin_list(decw, connection_settings,refresh=True)    
                
                print(f"{count}/{length} - Backing up "+dic['cid'] )
                new_pins = TpDestination.backup_ipfs_entity(DsGeneralDecelium,dic,all_pins,download_path,client,overwrite)
                if len(new_pins) == 0:
                    failures = failures + 1
                    if failures > failure_limit and failure_limit >0:
                        print("Hit Failure Limit")
                        return {'error':"too many failures"}
                if len(new_pins) > 0:
                    next_batch = next_batch + new_pins
            current_docs = next_batch
            next_batch = []
        return True
                
    @classmethod
    def download_payload_data(cls,decw,obj):
        try:
            # TODO - temp workaround to be removed when wallet is upgraded to decelium_wallet from paxdk
            result = decw.net.download_entity({"api_key":"UNDEFINED","self_id":obj['self_id']},remote=True)
        except:
            result = decw.net.download_entity({"api_key":"UNDEFINED","self_id":obj['self_id']})
            
        if type(result) == dict and 'error' in result:
            return result,None
        # Cretae bytes
        if type(result) == str:
            dat = bytes(result.encode("utf-8"))
        elif type(result) in [dict,list]:
            dat = bytes(jsondateencode_local.dumps(result).encode("utf-8"))
        elif type(result) == bytes:
            dat = bytes
        else:
            raise Exception("Could not parse type "+str(type(result)))
        return True,dat
        

        
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
    def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
        payload_success,payload_messages = cls.validate_object_payload(decw,object_id,download_path,connection_settings)
        entity_messages:ObjectMessages = entity_messages
        all_messages:ObjectMessages = payload_messages
        all_messages.append(entity_messages)
        return entity_success and payload_success,all_messages
    
    
    @classmethod
    def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("DsGeneralDecelium.validate_object_entity(for {"+object_id+"})")
        obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
        if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to have an invalid hash, as reported by DB validate_object_entity:"+str(obj_valid)) == False:
            return False, messages
        return len(messages.get_error_messages()) == 0, messages      

   
    @classmethod
    def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("DsGeneralDecelium.validate_object_payload(for {"+object_id+"})")
        if obj_remote == None:
            obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})

        obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
        if messages.add_assert(obj_valid == True, f"B. validate_entity_hash({object_id}) seems to have an invalid hash, as reported by DB validate_object_entity:"+str(obj_valid)) == False:
            return False, messages

        
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
        first = True
        for cid in cids_pinned:
            result = decw.net.check_pin_status({
                    'api_key':"UNDEFINED",
                    'connection_settings':connection_settings,
                    'cid': cid,
                    'do_refresh':first})
            first = False
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
    def find_batch_objects(cls,decw,offset,limit,filter=None):
        if filter ==None:
            filter ={'attrib':{'file_type':'ipfs'}}
        #print("Searching Filter = "+str(filter))
        filter['limit'] = limit
        filter['offset'] = offset
        docs = decw.net.list(filter)
        if type(docs) == dict and 'error' in docs:
            return filter
        #print(docs)
        obj_ids = []
        for doc in docs:
            obj_ids.append(doc)
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