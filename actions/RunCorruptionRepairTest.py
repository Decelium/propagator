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

from .SnapshotAgent import SnapshotAgent

import json    
class RunCorruptionRepairTest(Action):
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
    def run(self,**kwargs):
        #setup_config:TestConfig = record['setup_config']
        setup_type:TestConfig = kwargs['file_type']
        test_type:TestConfig = kwargs['test_type']
        remote_types:TestConfig = kwargs['remote_types']
        remote_mirror_types:TestConfig = kwargs['remote_mirror_types']
        self.test_corruptions_repair(setup_type,
                                     test_type,
                                     remote_types,
                                     remote_mirror_types)
        return 

    def prevalid(self,**kwargs):
        ''''''
        return True
    
    def get_validation_summary(self,decw,setup_config):
        validation_data = decw.net.validate_entity({'self_id':setup_config.obj_id()})
        local_validation_attrib = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_attrib')
        local_validation_payload = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_payload')
        validation_data['local_attrib'] = [local_validation_attrib[0]]
        validation_data['local_payload'] = [local_validation_payload[0]]
        return validation_data

    def postvalid(self,**kwargs):
        return True
   

    def explain(self,**kwargs):
        result = '''
        RunCorruptionRepairTest

        Run a corruption Test. Expects to be given an object ID to work with, and that this object is already complete present and on the selected server. 
        It then will 

        '''
        return result
    
    def generate(self,lang,**kwargs):
        return ""
    
    @classmethod
    def get_validation_summary(cls,decw,setup_config):
        validation_data = decw.net.validate_entity({'self_id':setup_config.obj_id()})
        local_validation_attrib = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_attrib')
        local_validation_payload = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_payload')
        validation_data['local_attrib'] = [local_validation_attrib[0]]
        validation_data['local_payload'] = [local_validation_payload[0]]
        return validation_data        

    @classmethod
    def test_corruptions_repair(cls,setup_type,test_type,remote_types,remote_mirror_types):
        from actions.SetupForTests import SetupForTests
        create_setup_config = SetupForTests()
        setup_config:TestConfig = create_setup_config(setup_type=setup_type)
        agent = SnapshotAgent()
        decw = setup_config.decw()
        obj = decw.net.download_entity({'self_id':setup_config.obj_id(),'attrib':True})
        configs = []
        validation_data = cls.get_validation_summary(decw,setup_config)

        modes = ['remote_attrib','remote_mirror_attrib','local_attrib']
        for mode in modes:
            assert mode in validation_data, f"1. Could not parse validation data for mode {mode}: "+str(validation_data)
            assert validation_data[mode][0][mode] == True, "2. Could not parse validation data: "+str(validation_data)
        modes = ['remote_payload','remote_mirror_payload','local_payload']
        for mode in modes:
            assert mode in validation_data, f"1. Could not parse validation data for mode {mode}: "+str(validation_data)
            assert validation_data[mode][0][mode] in [True,None], "2. Could not parse validation data: "+str(validation_data)

        
        target_type = setup_type
        assert test_type in ['remote_repair','remote_no_repair','local_no_repair']
        # CONFIG 1 : REMOTE REPAIR
        # -----
        if test_type == 'remote_repair':
            c_target_1 = 'remote'
            c_target_2 = 'remote_mirror'
            c_target_reserve = 'local'
            do_repair = True
            push_target = 'remote'
        
        # CONFIG 2 : REMOTE NO REPAIR
        # -----
        if test_type == 'remote_no_repair':
            c_target_1 = 'remote'
            c_target_2 = 'remote_mirror'
            c_target_reserve = 'local'
            do_repair = False
            push_target = 'remote'

        # CONFIG 3 : LOCAL NO REPAIR
        if test_type == 'local_no_repair':
            c_target_1 = 'remote'
            c_target_2 = 'local'
            c_target_reserve = 'remote_mirror'
            do_repair = False
            push_target = 'local'

        assert c_target_1 in ['remote','local','remote_mirror'] # The first datasource to corrupt
        assert c_target_2 in ['remote','local','remote_mirror'] # The second datasource to corrupt
        assert c_target_reserve in ['remote_mirror','local'] # The datasource that will be held stable, so we can restore after
        assert do_repair in [True,False] # Are we testing the repair process? (Only relevant for remote and remote_mirror tests)
        assert push_target in ['local','remote'] # Where we would like to push the repair data
        #assert target_type in ['ipfs','file']

        for corrupt_remote in remote_types:
            for corrupt_mirror in remote_mirror_types:
                print(f"CONFIGURING {corrupt_remote} , {corrupt_mirror}")
                config = CorruptionTestData.new_repair_corruption_config(corrupt_remote,
                                                    corrupt_mirror,
                                                    setup_config,
                                                    obj,
                                                        c_target_1,
                                                        c_target_2,
                                                        c_target_reserve,
                                                        do_repair,
                                                        push_target,
                                                        target_type)     
                print(f"FINISHED CONFIGURING {corrupt_remote} , {corrupt_mirror}")
                configs.append(config)

        print("corruption tests :"+str(len(configs)))
        for corruption_config in configs:
            print("----------------------------")
            print("----------------------------")
            print("Testing: \n"+ json.dumps(corruption_config['corruptions'],indent=4))
            print("setup_type,"+setup_type)
            print("test_type,"+test_type)
            agent.run_corruption_test(record=corruption_config)