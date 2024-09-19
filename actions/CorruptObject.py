try:
    from ..Snapshot import Snapshot
    from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    #from ..datasource.CorruptionData import CorruptionTestData
    from .Action import Action
    from ..type.CorruptionData import CorruptionTestData

except:
    from Snapshot import Snapshot
    from Messages import ObjectMessages
    #from type.BaseData import BaseData,auto_c
    #from datasource.CorruptionData import CorruptionTestData
    from .Action import Action
    from type.CorruptionData import CorruptionTestData

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

    @staticmethod
    def corrupt_remote_mirror_delete_entity(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"delete_entity",'mirror':True},["admin"]))
        if type(obj) == dict:
            assert not 'error' in obj

    @staticmethod
    def corrupt_remote_mirror_remove_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"remove_attrib",'mirror':True},["admin"]))

    @staticmethod
    def corrupt_remote_mirror_corrupt_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"corrupt_attrib",'mirror':True},["admin"]))

    @staticmethod
    def corrupt_remote_mirror_rename_attrib_filename(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        success = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"rename_attrib_filename",'mirror':True},["admin"]))
        assert success == True, "Did no succeed in removing at \n"+json.dumps(success)

    @staticmethod
    def corrupt_remote_mirror_delete_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        corrupt_result = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"delete_payload",'mirror':True},["admin"]))
        assert corrupt_result == True, "Got an invalid corruption result "+str(corrupt_result)


    @staticmethod
    def corrupt_remote_corrupt_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"corrupt_attrib"},["admin"]))

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
    def corrupt_remote_delete_entity(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"delete_entity"},["admin"]))
        if type(obj) == dict:
            assert not 'error' in obj, "Got an error trying to process the corruption "+ str(obj)
    
    @staticmethod
    def corrupt_remote_delete_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']

        obj = Snapshot.get_datasource("ipfs","remote").load_entity({'self_id':self_id,'api_key':decw.dw.pubk(),"attrib":True},decw)
        # obj = Snapshot.get_object_datasource(record['obj_id'],"remote").load_entity({'self_id':self_id,'api_key':decw.dw.pubk(),"attrib":True},decw)

        status = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"delete_payload"},["admin"]))
        print("corrupt_entity status"+ str(status))

        print("corrupt_entity - test_data")
        test_data = decw.net.download_entity({'self_id':self_id,'api_key':"UNDEFINED"})
        print(test_data)
        # assert True == False
        assert status == True, "Could not corrupt the entity "+str(status)
        print("corrupt_remote_delete_payload - payload")
        print(obj)
        validation_status = decw.net.validate_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk()},["admin"]))
        assert 'remote_payload' in validation_status, "a. Could not validate_entity " + str(validation_status)
        assert validation_status['remote_payload'][0]['remote_payload'] in [False,None], "b. Could not validate_entity " + str(validation_status) 

        '''
        # status =  Snapshot.remove_payload({'self_id':record['self_id']},record['backup_path']) 
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
        assert status == True
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
        '''

    
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
    def corrupt_local_delete_entity(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        Snapshot.get_datasource("ipfs","local").remove_entity({'self_id':self_id},backup_path)

    @staticmethod
    def corrupt_local_remove_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        file_path = os.path.join(backup_path, self_id, 'object.json')
        os.remove(file_path)
        memory['removed'].append(file_path)

    @staticmethod
    def corrupt_local_corrupt_attrib(record,memory):

        assert Snapshot.get_datasource("ipfs","local").corrupt_attrib({'self_id':record['obj_id']},record['backup_path']) == True
        file_path = os.path.join(record['backup_path'], record['obj_id'], 'object.json')
        memory['corrupted'].append(file_path)

    @staticmethod
    def corrupt_local_rename_attrib_filename(record,memory):

        assert Snapshot.get_datasource("ipfs","local").corrupt_attrib_filename({'self_id':record['obj_id']},record['backup_path']) == True
        #file_path = os.path.join(record['backup_path'], record['obj_id'], 'object.json')            
        
    @staticmethod
    def corrupt_local_corrupt_payload(record,memory):
        # outputTest = Snapshot.get_datasource("ipfs","local").corrupt_payload({'self_id':record['obj_id']},record['backup_path'])
        # print("corrupt_local_corrupt_payload.outputTest",outputTest)
        success,files_affected =  Snapshot.get_datasource("ipfs","local").corrupt_payload({'self_id':record['obj_id']},record['backup_path'])
        assert success == True
        for file_path in files_affected:
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
        assert self_id != None
        local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode)
        print ("THE SELF ID"+str(self_id))
        assert local_results[mode] == True,"Got some bad results "+ str(local_results)
        return True

    def run(self,record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        memory['removed'] = []
        memory['corrupted'] = []
        corruption = record['corruption']
        mode = record['mode']
        assert corruption in CorruptionTestData.Instruction.corruption_types
        assert corruption in ['delete_payload','corrupt_payload','remove_attrib','rename_attrib_filename','corrupt_attrib','delete_entity'],"Unsupported Corruption "+str(record['corruption'])
        self.run_corruption(mode, corruption, record, memory)
        return True 

    def postvalid(self,record,response,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        connection_settings = record['connection_settings']
        decw = record['decw']
        mode = record['mode']
        corruption = record['corruption']
        if 'payload' in corruption:
            local_results_attrib,messages_attrib = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode+"_attrib")
            local_results_payload,messages_payload = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode+"_payload")
            messages:ObjectMessages = messages_attrib
            try:
                assert local_results_payload[mode+"_payload"] in [False,None], "Corruption Failed: "+ str(record)+"-:-" + str(local_results)
                if 'removed' in memory:
                    for file_path in memory['removed']:
                        assert os.path.exists(file_path) == False
                if 'corrupted' in memory:
                    for file_path in memory['corrupted']:
                        assert os.path.exists(file_path) == True
            except Exception as e:
                print("Printing messages along with failed corruption")
                print(record)
                print(local_results_payload)
                print(messages.get_error_messages())
                raise e
        else:
            local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode)
            local_results_attrib,messages_attrib = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode+"_attrib")
            local_results_payload,messages_attrib = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode+"_payload")
            messages:ObjectMessages = messages
            try:
                assert local_results[mode] == False, "Corruption Failed: "+ str(record)+"-:-" + str(local_results)
                if 'removed' in memory:
                    for file_path in memory['removed']:
                        assert os.path.exists(file_path) == False
                if 'corrupted' in memory:
                    for file_path in memory['corrupted']:
                        assert os.path.exists(file_path) == True
            except Exception as e:
                print("Printing messages along with failed corruption")
                print(record)
                print(local_results)
                print(messages.get_error_messages())
                raise e

        return True