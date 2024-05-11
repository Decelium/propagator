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
        corrupt_object_backup(backup_instruction)

    def run_corruption_test(self,setup_config:TestConfig,
                            obj:dict,
                            all_corruptions:dict):
        for corruption in all_corruptions:
            self.run_corruption(setup_config.decw(),
                                    obj,
                                    setup_config.backup_path(),
                                    setup_config.connection_settings(),
                                    setup_config.eval_context(),
                                    setup_config.user_context(),
                                    corruption)    
    def run(self,record,memory):
        self.run_corruption_test(record['setup_config'],record['obj'],record['corruptions'])
        return 

    def prevalid(self,record,memory):
        setup_config:TestConfig = record['setup_config']
        evaluate_object_status({**setup_config.eval_context(),'target':'local','status':['complete']})  
        evaluate_object_status({**setup_config.eval_context(),'target':'remote','status':['complete']})        
        evaluate_object_status({**setup_config.eval_context(),'target':'remote_mirror','status':['complete']})        
        return True
    
    def postvalid(self,record,response,memory=None):

        # Step 1: In post, we want to first make sure the corruption indeed caused the kind of corruption we are looking for
        setup_config:TestConfig = record['setup_config']
        for eval in record['corruption_evals']:
            evaluate_object_status({**setup_config.eval_context(),**eval})

        # Step 2: After we evaluate, we want to restore the object to its original state
        if record['repair_target'] == 'local':
            pull_object_from_remote = PullObjectFromRemote()
            pull_object_from_remote({**setup_config,'overwrite': False,'expected_result':True,})
        elif record['repair_target'] == 'remote':
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