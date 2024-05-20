import decelium_wallet.core as core
import pandas
try:
    from Messages import ObjectMessages
except:
    from ..Messages import ObjectMessages
import traceback as tb
import ipfshttpclient

from .TpGeneralLocal import TpGeneralLocal
class TpIPFSLocal(TpGeneralLocal):
    pass


from .TpGeneralDecelium import TpGeneralDecelium
class TpIPFSDecelium(TpGeneralDecelium):
    pass

class TpIPFSDeceliumMirror(TpIPFSDecelium):
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