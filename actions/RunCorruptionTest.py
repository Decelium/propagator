try:
    #from ..Snapshot import Snapshot
    #from ..datasource.TpIPFSDecelium import TpIPFSDecelium
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    #from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    from ..type.CorruptionData import CorruptionTestData
    from ..type.BaseData import TestConfig
    from .Action import Action

except:
    from type.CorruptionData import CorruptionTestData
    from type.BaseData import TestConfig
    from .CorruptObject import CorruptObject
    from .PullObjectFromRemote import PullObjectFromRemote
    from .EvaluateObjectStatus import evaluate_object_status #Exported as a premade function
    from .ChangeRemoteObjectName import ChangeRemoteObjectName
    from .PushFromSnapshotToRemote import PushFromSnapshotToRemote
    from .Action import Action,agent_action

import json    
class RunCorruptionTest(Action):
    def run_corruption(self,decw,
                            obj,
                            backup_path,
                            connection_settings,
                            eval_context,
                            user_context,
                            corruption):
        corrupt_object_backup = CorruptObject()
        change_remote_object_name = ChangeRemoteObjectName()

        corruption = CorruptionTestData.Instruction(corruption)
        backup_instruction  ={
            'decw': decw,
            'obj_id':obj['self_id'],
            'backup_path':backup_path,        
            'connection_settings':connection_settings,        
        }
        backup_instruction["corruption"] = corruption['corruption']
        backup_instruction["mode"] = corruption['mode']
        # backup_instruction.update(corruption)
        print("\n RUNNIN CORRUPTION")
        print(backup_instruction)
        corrupt_object_backup(backup_instruction)

    def run_corruption_test(self,setup_config:TestConfig,
                            obj:dict,
                            all_corruptions:dict,
                            do_repair:bool):
        for corruption in all_corruptions:
            self.run_corruption(setup_config.decw(),
                                    obj,
                                    setup_config.backup_path(),
                                    setup_config.connection_settings(),
                                    setup_config.eval_context(),
                                    setup_config.user_context(),
                                    corruption)    
    def run(self,record,memory):
        setup_config:TestConfig = record['setup_config']
        # validation_data_pre = setup_config.decw().net.validate_entity({'self_id':setup_config.obj_id()})
        # print("validation_data_attib 0")
        # print(validation_data_pre["remote_attrib"])           
        self.run_corruption_test(record['setup_config'],record['obj'],record['corruptions'],record['do_repair'])
        # validation_data_pre = setup_config.decw().net.validate_entity({'self_id':setup_config.obj_id()})
        # print("validation_data_attib 0.5")
        # print(validation_data_pre["remote_attrib"])            
        return 

    def prevalid(self,record,memory):
        assert 'setup_config' in record
        assert 'obj' in record
        assert 'corruptions' in record
        assert 'corruption_evals' in record
        assert 'invalid_props' in record
        assert 'do_repair' in record
        assert 'post_repair_status' in record
        assert 'final_evals' in record
        assert 'push_target' in record
        setup_config:TestConfig = record['setup_config']
        evaluate_object_status({**setup_config.eval_context(),'target':'local','status':['complete']})  
        evaluate_object_status({**setup_config.eval_context(),'target':'remote','status':['complete']})        
        evaluate_object_status({**setup_config.eval_context(),'target':'remote_mirror','status':['complete']})  
        print("\n")
        print("\n")
        print("Ready for next test:")
        return True
    
    def postvalid(self,record,response,memory=None):

        # Step 1: In post, we want to first make sure the corruption indeed caused the kind of corruption we are looking for
        setup_config:TestConfig = record['setup_config']
        invalid_props = record['invalid_props']
        for eval in record['corruption_evals']:
            print("Evaluating Corruption:")
            print(eval)
            evaluate_object_status({**setup_config.eval_context(),**eval})
        rec = record.copy()
        del rec['setup_config']
        print(json.dumps(rec,indent=2))
        print("CORRUPTION SHOULD BE APPLIED")
        # TODO -- make sure after payload is removed, that the data is absolutely not online.
        #return True
        validation_data = setup_config.decw().net.validate_entity({'self_id':setup_config.obj_id()})
        if 'error' in validation_data:
            print(validation_data)
        print("PRE-REPAIR VALIDATUION SUMMARY:")
        for k in validation_data.keys():
            try:
                print (f"{k} is {validation_data[k][0][k]}")
            except:
                pass #print (f"{k} is broken")
        
        if record['do_repair'] == True:
            print("EXECUTING REPAIR")
            repair_status = setup_config.decw().net.repair_entity({'self_id':setup_config.obj_id()})
            print(repair_status)
            # return True
            '''  '''
            validation_data = setup_config.decw().net.validate_entity({'self_id':setup_config.obj_id()})
            if 'error' in validation_data:
                print(validation_data)
            print("TEMP VALIDATUION SUMMARY:")
            for k in validation_data.keys():
                try:
                    print (f"{k} is {validation_data[k][0][k]}")
                except:
                    print (f"{k} is broken"+str(validation_data[k]))
            # return True

            '''  '''
            if record['post_repair_status']  == True:
                assert record['post_repair_status'] == repair_status, "Expected Successful Repair:\n" + str(repair_status)
            if record['post_repair_status']  == False:
                assert type(repair_status)==dict and 'error' in repair_status, "Expected Failed Repair:\n" + str(repair_status)

            for eval in record['final_evals']:
                evaluate_object_status({**setup_config.eval_context(),**eval})
        #else:
        #    print("SKIPPING REPAIR")
        props = ['remote_attrib','remote_payload','remote_mirror_attrib','remote_mirror_payload']
        validation_data = setup_config.decw().net.validate_entity({'self_id':setup_config.obj_id()})
        if 'error' in validation_data:
            print(validation_data)
        print("VALIDATUION SUMMARY:")
        for k in validation_data.keys():
            try:
                print (f"{k} is {validation_data[k][0][k]}")
            except:
                pass #print (f"{k} is broken")

        for prop in props:
            if prop not in invalid_props:
                assert validation_data[prop][0][prop] == True,"Could not find that "+prop+" was valid from validation_data: \n"+json.dumps(validation_data[prop][0],indent=1)
            else:
                assert validation_data[prop][0][prop] == False,"Could not find that "+prop+" was INvalid from validation_data: \n"+json.dumps(validation_data[prop][0],indent=1)

        # Step 2: After we evaluate, we want to restore the object to its original state
        if record['push_target'] == 'local':
            pull_object_from_remote = PullObjectFromRemote()
            pull_object_from_remote({**setup_config,'overwrite': False,'expected_result':True,})
        elif record['push_target'] == 'remote':
            push_from_snapshot_to_remote = PushFromSnapshotToRemote()
            push_from_snapshot_to_remote({**setup_config,'overwrite': False,'expected_result':True,})
        else:
            assert True==False, "Forcing a failure as we are not corrupting remote or local"

        evaluate_object_status({**setup_config.eval_context(),'target':'local','status':['complete']})  
        evaluate_object_status({**setup_config.eval_context(),'target':'remote','status':['complete']})        
        evaluate_object_status({**setup_config.eval_context(),'target':'remote_mirror','status':['complete']})       
        return True
   
    def test(self,record):
        return True

    def explain(self,record,memory=None):
        result = '''
        RunCorruptionTest

        Run a corruption Test. Expects to be given an object ID to work with, and that this object is already complete present and on the selected server. 
        It then will 

        '''
        return result
    
    def generate(self,lang,record,memory=None):
        return ""