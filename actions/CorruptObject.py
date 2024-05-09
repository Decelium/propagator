try:
    from ..Snapshot import Snapshot
    from ..datasource.TpIPFSDecelium import TpIPFSDecelium
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    #from ..datasource.CorruptionData import CorruptionTestData
    from .Action import Action
except:
    from Snapshot import Snapshot
    from datasource.TpIPFSDecelium import TpIPFSDecelium
    #from datasource.TpIPFSLocal import TpIPFSLocal
    from Messages import ObjectMessages
    #from type.BaseData import BaseData,auto_c
    #from datasource.CorruptionData import CorruptionTestData
    from .Action import Action
import random,os,json

class CorruptObject(Action):    
    def explain(self,record,memory):
        return """
        CorruptObject

        This is an action which purposely corrupts a local file backup. This is to simulate various corruption methods
        such that the file can be restored and validated afterward. The complete version of this process ensures
        a) pre: The backup is complete before corruption
        b) complete a corruption
        c) post: The corruption is reported correctly by the validation tools
         ['delete_payload','remove_attrib','rename_attrib_filename']
        for record: """+ str(record)
    
    @staticmethod
    def corrupt_remote_mirror_corrupt_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"delete_payload",'mirror':True},["admin"]))
        if type(obj) == dict:
            assert not 'error' in obj
        raise Exception("Should verify removal")
    
    @staticmethod
    def corrupt_remote_mirror_remove_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"remove_attrib",'mirror':True},["admin"]))
        raise Exception("Should verify removal 2")

    @staticmethod
    def corrupt_remote_mirror_rename_attrib_filename(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        success = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"rename_attrib_filename",'mirror':True},["admin"]))
        assert success == True
        raise Exception("Should verify removal 3")

    @staticmethod
    def corrupt_remote_mirror_delete_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        corrupt_result = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"delete_payload",'mirror':True},["admin"]))
        assert corrupt_result == True
        raise Exception("Should verify removal 4")

    @staticmethod
    def corrupt_remote_corrupt_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"delete_payload"},["admin"]))
        if type(obj) == dict:
            assert not 'error' in obj
        pins = decw.net.download_pin_status({
                'api_key':"UNDEFINED",
                'do_refresh':True,
                'connection_settings':connection_settings})   
        # import pprint
        # print("corrupt_remote_corrupt_payload visual inspection")
        # pprint.pprint(obj)
        # pprint.pprint(pins)

    @staticmethod
    def corrupt_remote_remove_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"remove_attrib"},["admin"]))

    @staticmethod
    def corrupt_remote_rename_attrib_filename(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        success = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"rename_attrib_filename"},["admin"]))
        assert success == True

    @staticmethod
    def corrupt_remote_delete_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']

        obj = TpIPFSDecelium.load_entity({'self_id':self_id,'api_key':decw.dw.pubk(),"attrib":True},decw)

        cids = [obj['settings']['ipfs_cid']]
        if 'ipfs_cids' in obj['settings']:
            for cid in obj['settings']['ipfs_cids'].values():
                cids.append(cid)

        result:dict = decw.net.remove_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'cid',
                'payload':cids})
        #corrupt_result = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"delete_payload",'mirror':True},["admin"]))
        #assert corrupt_result == True

        for r in result.values():
            assert r['removed'] == True
        
        for r in result.values():
            result_verify = decw.net.check_pin_status({
                    'api_key':"UNDEFINED",
                    'do_refresh':True,
                    'connection_settings':connection_settings,
                    'cid': r['cid']})
            break

        for r in result.values():
            result_verify = decw.net.check_pin_status({
                    'api_key':"UNDEFINED",
                    'connection_settings':connection_settings,
                    'cid': r['cid']})
            assert result_verify == False
    
    @staticmethod
    def corrupt_local_delete_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        for filename in os.listdir(os.path.join(backup_path,self_id)):
            if filename.endswith('.dag') or filename.endswith('.file'):
                file_path = os.path.join(backup_path,self_id, filename)
                os.remove(file_path)
                memory['removed'].append(file_path)
        
    @staticmethod
    def corrupt_local_remove_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        file_path = os.path.join(backup_path, self_id, 'object.json')
        os.remove(file_path)
        memory['removed'].append(file_path)

    @staticmethod
    def corrupt_local_corrupt_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        file_path = os.path.join(backup_path, self_id, 'object.json')
        random_bytes_size = 1024
        random_bytes = random.getrandbits(8 * random_bytes_size).to_bytes(random_bytes_size, 'little')
        with open(file_path, 'wb') as corrupt_file:
            corrupt_file.write(random_bytes)
        memory['corrupted'].append(file_path)

    @staticmethod
    def corrupt_local_rename_attrib_filename(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        file_path = os.path.join(backup_path, self_id, 'object.json')
        with open(file_path, 'r') as f:
            correct_json = json.loads(f.read())
        correct_json['dir_name'] = "corrupt_name"
        with open(file_path, 'w') as f:
            f.write(json.dumps(correct_json))
        
    @staticmethod
    def corrupt_local_corrupt_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        for filename in os.listdir(os.path.join(backup_path, self_id)):
            if  filename.endswith('.file'): # filename.endswith('.dag') or
                file_path = os.path.join(backup_path, self_id, filename)
                random_bytes_size = 1024
                random_bytes = random.getrandbits(8 * random_bytes_size).to_bytes(random_bytes_size, 'little')
                with open(file_path, 'wb') as corrupt_file:
                    corrupt_file.write(random_bytes)
                memory['corrupted'].append(file_path)    

    def run_corruption(self,mode: str, corruption: str, record: dict, memory: dict):
        method_name = "corrupt_" + mode + "_" + corruption
        method = getattr(CorruptObject, method_name, None)
        if method:
            method(record, memory)
        else:
            raise Exception(f"Method {method_name} not found.")

    def prevalid(self,record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        connection_settings = record['connection_settings']
        decw = record['decw']
        mode = record['mode']
        assert mode in ['local','remote','remote_mirror']
        local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode)
        assert local_results[mode] == True
        return True

    def run(self,record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        memory['removed'] = []
        memory['corrupted'] = []
        corruption = record['corruption']
        mode = record['mode']
        assert corruption in ['delete_payload','corrupt_payload','remove_attrib','corrupt_attrib','rename_attrib_filename']
        self.run_corruption(mode, corruption, record, memory)
        return True 

    def postvalid(self,record,response,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        connection_settings = record['connection_settings']
        decw = record['decw']
        mode = record['mode']
        local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode)
        messages:ObjectMessages = messages
        try:
            assert local_results[mode] == False
            #assert len(memory['removed']) > 0
            if 'removed' in memory:
                for file_path in memory['removed']:
                    assert os.path.exists(file_path) == False
            if 'corrupted' in memory:
                for file_path in memory['corrupted']:
                    assert os.path.exists(file_path) == True
        except Exception as e:
            print("Printing messages along with failed corruption")
            print(local_results)
            print(messages.get_error_messages())
            raise e
        return True