import decelium_wallet.core as core
import pandas
try:
    from Messages import ObjectMessages
except:
    from ..Messages import ObjectMessages
import traceback as tb
import ipfshttpclient

from .DsGeneral import DsGeneral, TpFacade
from .DsGeneralLocal import DsGeneralLocal
from .DsGeneralDecelium import DsGeneralDecelium
import json
class TpIPFS(TpFacade):
    class Local(DsGeneralLocal):
        @classmethod
        def merge_payload_from_remote(cls,TpSource,decw,obj,download_path,connection_settings, overwrite):
            merge_messages = ObjectMessages("TpIPFS.Local.__merge_payload_from_remote(for obj_id)"+str(obj['self_id']) )
    
            new_cids = [obj['settings']['ipfs_cid']]
            if 'ipfs_cids' in obj['settings']:
                for cid in obj['settings']['ipfs_cids'].values():
                    new_cids.append(cid)
            print("TpIPFS merge_payload_from_remote Downloading IPFS data for "+obj['self_id'])
            result = TpSource.download_ipfs_data(cls,decw,new_cids, download_path+'/'+obj['self_id'], connection_settings,overwrite)
            return result
        
        @classmethod
        def validate_object_attrib(cls,decw,object_id,download_path,connection_settings):
            # Validate the local representation of an object
            messages = ObjectMessages("TpIPFS.Local.validate_object(for {object_id})")
            try:
                file_path_test = download_path+'/'+object_id+'/object.json'
                with open(file_path_test,'r') as f:
                    obj_local = json.loads(f.read())
                valido_hasho = cls.compare_file_hash(file_path_test)
                if valido_hasho != True:
                    messages.add_assert(False, "Encountered A bad hash object.json :"+file_path_test)
                    return False,messages
            except Exception as e:
                messages.add_assert(False==True, "Could not validate presense of file file:"+str(download_path+'/'+object_id+'/object.json err:'+tb.format_exc()))
                return False,messages

            
            return len(messages.get_error_messages())== 0,messages   

    
    class LocalMirror(DsGeneral):
        pass

    class Decelium(DsGeneralDecelium):
        pass

    class DeceliumMirror(DsGeneralDecelium):
        @classmethod
        def load_entity(cls,query,decw):
            assert 'api_key' in query
            assert 'self_id' in query
            return decw.net.download_entity_mirror(query)    
        
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
            messages = ObjectMessages("TpIPFSDeceliumMirror.validate_object_entity_mirror(for {"+object_id+"})")
            obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id,'mirror':True})
            if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to be invalid, as reported by DB validate_object_entity_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages      

        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpIPFSDeceliumMirror.validate_object_entity_payload(for {"+object_id+"})")
            obj_valid = decw.net.validate_payload( {'api_key':'UNDEFINED', 'self_id':object_id,'mirror':True})
            if messages.add_assert(obj_valid == True, f"{object_id} seems to be invalid, as reported by DB validate_object_payload_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages