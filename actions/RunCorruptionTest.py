try:
    from ..Snapshot import Snapshot
    #from ..datasource.TpIPFSDecelium import TpIPFSDecelium
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    #from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    from ..type.CorruptionData import CorruptionTestData
    from ..type.BaseData import TestConfig
    from .Action import Action

except:
    from Snapshot import Snapshot
    from type.CorruptionData import CorruptionTestData
    from type.BaseData import TestConfig
    from .CorruptObject import CorruptObject
    from .PullObjectFromRemote import PullObjectFromRemote
    from .AppendObjectFromRemote import AppendObjectFromRemote
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
        print(backup_instruction)
        corrupt_object_backup(record=backup_instruction)

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
        evaluate_object_status(record={**setup_config.eval_context(),'target':'local','status':['complete']})  
        evaluate_object_status(record={**setup_config.eval_context(),'target':'remote','status':['complete']})        
        evaluate_object_status(record={**setup_config.eval_context(),'target':'remote_mirror','status':['complete']})  
        return True
    
    def get_validation_summary(self,decw,setup_config):
        validation_data = decw.net.validate_entity({'self_id':setup_config.obj_id()})
        local_validation_attrib = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_attrib')
        local_validation_payload = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_payload')
        validation_data['local_attrib'] = [local_validation_attrib[0]]
        validation_data['local_payload'] = [local_validation_payload[0]]
        return validation_data

    def postvalid(self,record,response,memory=None):

        # Step 1: In post, we want to first make sure the corruption indeed caused the kind of corruption we are looking for
        setup_config:TestConfig = record['setup_config']
        invalid_props = record['invalid_props']
        for eval in record['corruption_evals']:
            print("Evaluating pre repar corruption")
            print(eval)
            evaluate_object_status(record={**setup_config.eval_context(),**eval})
        rec = record.copy()
        del rec['setup_config']
        # TODO -- make sure after payload is removed, that the data is absolutely not online.
        #return True
        #validation_data = setup_config.decw().net.validate_entity({'self_id':setup_config.obj_id()})
        validation_data = self.get_validation_summary(setup_config.decw(),setup_config)
        if 'error' in validation_data:
            print(validation_data)
        print("\nPRE-REPAIR VALIDATUION SUMMARY:")
        for k in validation_data.keys():
            try:
                print (f"{k} is {validation_data[k][0][k]}")
            except:
                pass
                #print (f"{k} is broken:"+str(validation_data[k]))
        print("\n")
        print("\n")
        if record['do_repair'] == True:
            print("EXECUTING REPAIR")
            repair_status = setup_config.decw().net.repair_entity({'self_id':setup_config.obj_id()})
            print(repair_status)
            # return True
            '''  '''
            # validation_data = setup_config.decw().net.validate_entity({'self_id':setup_config.obj_id()})
            validation_data = self.get_validation_summary(setup_config.decw(),setup_config)
            if 'error' in validation_data:
                print(validation_data)
            print("\nPOST-REPAIR VALIDATUION SUMMARY:")
            for k in validation_data.keys():
                try:
                    print (f"{k} is {validation_data[k][0][k]}")
                except:
                    pass
                    # print (f"{k} is broken"+str(validation_data[k]))
            # return True
            print("\n")
            print("\n")
            '''  '''
            if record['post_repair_status']  == True:
                assert record['post_repair_status'] == repair_status, "Expected Successful Repair:\n" + str(repair_status)
            if record['post_repair_status']  == False:
                assert type(repair_status)==dict and 'error' in repair_status, "Expected Failed Repair:\n" + str(repair_status)

            for eval in record['final_evals']:
                evaluate_object_status(record={**setup_config.eval_context(),**eval})
        #else:
        #    print("SKIPPING REPAIR")
        props = ['remote_attrib','remote_payload','remote_mirror_attrib','remote_mirror_payload','local_attrib','local_payload']
        validation_data = self.get_validation_summary(setup_config.decw(),setup_config)
        if 'error' in validation_data:
            print(validation_data)
        for prop in props:
            if prop not in invalid_props:
                if 'payload' in prop:
                    checklist = [True,None]
                else:
                    checklist = [True]
                assert validation_data[prop][0][prop] in checklist,"Could not find that "+prop+" was valid from validation_data: \n"+json.dumps(validation_data[prop][0],indent=1)

            else:
                if 'payload' in prop:
                    checklist = [False,None]
                else:
                    checklist = [False]
                assert validation_data[prop][0][prop] in checklist,"Could not find that "+prop+" was INvalid from validation_data: \n"+json.dumps(validation_data[prop][0],indent=1)

        # Step 2: After we evaluate, we want to restore the object to its original state

        if record['push_target'] == 'local':
            repair_status = setup_config.decw().net.repair_entity({'self_id':setup_config.obj_id()})
            assert repair_status == True, "Should have been able to repair: "+ str(repair_status)
            pull_object_from_remote = PullObjectFromRemote()
            append_object_from_remote = AppendObjectFromRemote()
            method_to_repair = "pull"
            for corruption in record['corruptions']:
                if corruption['corruption'] == 'delete_entity' and corruption['mode'] == 'local':
                    method_to_repair = "append"
                    break
            if method_to_repair == "pull":
                pull_object_from_remote(record={**setup_config,'overwrite': False,'expected_result':True,})
            else:
                append_object_from_remote(record={**setup_config,'overwrite': False,'expected_result':True,})
        elif record['push_target'] == 'remote':
            push_from_snapshot_to_remote = PushFromSnapshotToRemote()
            push_from_snapshot_to_remote(record={**setup_config,'overwrite': False,'expected_result':True,})
        else:
            assert True==False, "Forcing a failure as we are not corrupting remote or local"

        evaluate_object_status(record={**setup_config.eval_context(),'target':'local','status':['complete']})  
        evaluate_object_status(record={**setup_config.eval_context(),'target':'remote','status':['complete']})    
        evaluate_object_status(record={**setup_config.eval_context(),'target':'remote_mirror','status':['complete']})       
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