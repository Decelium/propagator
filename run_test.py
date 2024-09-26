import decelium_wallet.core as core
import os, sys,json
import shutil
sys.path.append('..')
#from type.BaseData import BaseData,auto_c
from type.CorruptionData import CorruptionTestData
#from actions import CreateDecw
from type.BaseData import TestConfig,ConnectionConfig
from actions.SnapshotAgent import SnapshotAgent
from actions.SetupForTests import SetupForTests
from actions.RunCorruptionRepairTest import RunCorruptionRepairTest
from Messages import ObjectMessages
from Snapshot import Snapshot
from datetime import datetime,timedelta,time
'''
def test_corruptions_repair(setup_type,test_type,remote_types,remote_mirror_types):
    setup_config:TestConfig = test_setup(setup_type)
    agent = SnapshotAgent()
    decw = setup_config.decw()
    obj = decw.net.download_entity({'self_id':setup_config.obj_id(),'attrib':True})
    configs = []
    validation_data = get_validation_summary(decw,setup_config)

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

#####
def get_validation_summary(decw,setup_config):
    validation_data = decw.net.validate_entity({'self_id':setup_config.obj_id()})
    local_validation_attrib = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_attrib')
    local_validation_payload = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_payload')
    validation_data['local_attrib'] = [local_validation_attrib[0]]
    validation_data['local_payload'] = [local_validation_payload[0]]
    return validation_data        

'''

# setup_type - 'ipfs', 'file'
# test_type - 'remote_repair', 'remote_no_repair', 'local_no_repair'
# setup_type = 'ipfs'
# setup_type =  'json'
# setup_type = 'file'
# setup_type =  'host' # Awaiting DNS update 
# setup_type =  'directory' # Requires some refatoring
# setup_type =  'user' # *Should* work
# file_types = ['directory','user']
# []'delete_payload','corrupt_payload','remove_attrib','rename_attrib_filename','corrupt_attrib','delete_entity']


def perliminary_tests():
    agent = SnapshotAgent()
    test_setup: SetupForTests = SetupForTests()
    setup_config = test_setup()
    print("---- 3: Doing Small Delete")
    agent.delete_object_from_remote({
        'decw':setup_config.decw(),
        'user_context':setup_config.user_context(),
        'connection_settings':setup_config.connection_settings(),
        'path': setup_config.decelium_path(),     
    })
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'local','status':['complete']})
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'remote','status':['object_missing','payload_missing']})   
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'remote_mirror','status':['object_missing','payload_missing']})
    print("---- 3: Doing Small Push")
    agent.push_from_snapshot_to_remote({
        'decw': setup_config.decw(),
        'obj_id':setup_config.obj_id(),
        'user_context':setup_config.user_context(),
        'connection_settings':setup_config.connection_settings(),
        'backup_path':setup_config.backup_path(),
    })
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'local','status':['complete']})
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'remote','status':['complete']})  
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'remote_mirror','status':['complete']})

remote_types = CorruptionTestData.Instruction.corruption_types
remote_mirror_types = CorruptionTestData.Instruction.corruption_types

# MANUAL CONFIG
file_types = ['ipfs']
test_types = ['remote_repair']
#remote_types = ['rename_attrib_filename']
remote_types = ['delete_payload']
remote_mirror_types = ['delete_payload']
# rename_attrib_filename


for test_type in test_types:
    for file_type in file_types:
        f_runtest = RunCorruptionRepairTest()
        f_runtest(file_type=file_type,
                       test_type=test_type,
                       remote_types=remote_types,
                       remote_mirror_types=remote_mirror_types)
#print ("FINISHED")
