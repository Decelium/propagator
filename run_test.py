import decelium_wallet.core as core
import os, sys,json
import shutil
sys.path.append('..')
#from type.BaseData import BaseData,auto_c
from type.CorruptionData import CorruptionTestData
#from actions import CreateDecw
from type.BaseData import TestConfig,ConnectionConfig
from actions.SnapshotAgent import SnapshotAgent
from Messages import ObjectMessages
from Snapshot import Snapshot
from datetime import datetime,timedelta,time


def object_setup(agent:SnapshotAgent,
                 conn_config:ConnectionConfig,
                 setup_type:str):
    print("object_setup.setup_type",setup_type)
    assert setup_type in list(Snapshot.s_type_map.keys())
    user_context = conn_config.user_context()
    connection_settings = conn_config.connection_settings()
    local_test_folder = conn_config.local_test_folder()
    decw = conn_config.decw()
    if setup_type == 'ipfs':
        decelium_path = 'temp/test_folder.ipfs'
        ipfs_req_context = {**user_context, **{
                'file_type':'ipfs', 
                'connection_settings':connection_settings
        }}
        print("---- 2: Doing Small Upload")
        obj_id = agent.upload_directory_to_remote({
            'local_path': local_test_folder,
            'decelium_path': decelium_path,
            'decw': decw,
            'ipfs_req_context': ipfs_req_context,
            'user_context': user_context
        })
        return obj_id, decelium_path
    if setup_type == 'user':
        wallet_contents = decw.dw.get_raw()

        access_keys = wallet_contents["admin"]['user'].copy()
        access_keys['private_key'] = "destroy it"

        feature = {'username': "example_user",
                   'api_key': decw.dw.pubk("admin"),
                   'access_key':access_keys,
                   'password': "example_pass",
                   'password2': "example_pass",}
        result = decw.net.delete_entity(decw.dw.sr({'api_key':decw.dw.pubk("admin"),'path':'system_users','name':"example_user",}))   
        print("Delete result > "+str(result))
        decelium_path = 'system_users/example_user'
        obj_id = decw.net.user_register(feature)        
        
        assert decw.has_entity_prefix(obj_id), "Could not create the user "+str(obj_id)
        return obj_id, decelium_path

    if setup_type in ['file','json','host','directory']:
        if setup_type == 'directory':
            delete_request = { 'path':'/example_dir',
            }
            # You must place the public_key into the servers TXT records
            create_request = {
                'path':'/',
                'name':'example_dir',
                'file_type':'directory',
            }
        
        elif setup_type == 'host':
            delete_request = { 'path':'/example_domain.dns',
            }
            # You must place the public_key into the servers TXT records
            create_request = {
                'path':'/',
                'name':'example_domain.dns',
                'file_type':'host',
                'attrib':{'host':'techoactivism.com',
                          'target_id':'xbj-INVALID_FOR_TESTING',
                                'secret_password':"api_key"},
            }

        elif setup_type == 'file':
            delete_request = { 'path':'/example_html_file_test.html',
            }
            create_request = {
                'path':'/',
                'name':'example_html_file_test.html',
                'file_type':'file',
                'payload':'''<h1>This is a file</h1>''',
            }
        elif setup_type == 'json':
            delete_request = { 'path':'/temp_dict.json',
            }
            create_request = {
                'path':'/',
                'name':'temp_dict.json',
                'file_type':'json',
                'payload':{"example":"value"},
            }       
        else:
            raise Exception("Unsuported file type")
        decelium_path = '/example_html_file_test.html'
        # download_try = decw.net.download_entity(decw.dw.sr({**user_context, **delete_request,'attrib':True}))
        # print("download_try\n",str(download_try))

        list_try = decw.net.list(decw.dw.sr({**user_context, **delete_request}))
        print("list_try\n",str(list_try))
        for sub_file in list_try:
            del_inner = decw.net.delete_entity({**user_context, 'self_id':sub_file['self_id']})
            assert del_inner == True , "Could not clean up an inner file "+ str(del_inner)
        singed_req = decw.dw.sr({**user_context, **delete_request})
        del_try = decw.net.delete_entity(singed_req)
        try:
            assert del_try == True  or ('error' in del_try and 'could not find' in del_try['error']), "Got an invalid response for del_try "+ str(del_try)
        except Exception as e:
            print("Failing Delete Object Id" + str(del_try))
            raise e
        singed_req = decw.dw.sr({**user_context, **create_request})
        print(singed_req)
        obj_id = decw.net.create_entity(singed_req)
        assert decw.has_entity_prefix(obj_id), "Could not create the object "+str(obj_id) + "delete res "+ str(del_try)
        return obj_id, decelium_path
    
    return {"error":"Did not register the existing type."}, None




def test_setup(setup_type = 'ipfs') -> TestConfig:
    print("---- 1: Doing Setup")
    agent = SnapshotAgent()

    decw, connected = agent.create_wallet_action({
         'wallet_path': '../.wallet.dec',
         'wallet_password_path':'../.wallet.dec.password',
         'fabric_url': 'http://devdecelium.com:5000/data/query',
        })
    
    user_context = {
            'api_key':decw.dw.pubk()
    }
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"
    }
    local_test_folder = './test/testdata/test_folder'
    backup_path='../devdecelium_backup/'    
    # --- Remove old snapshot #
    try:
        shutil.rmtree(backup_path)
    except:
        pass
    conn_config = ConnectionConfig({
                 'local_test_folder':local_test_folder,
                 'decw':decw,
                 'connection_settings':connection_settings,
                 'backup_path':backup_path,
                 'user_context':user_context,
                })
    print("setup_type",setup_type)
    obj_id, decelium_path =  object_setup(
                 agent,
                 conn_config,
                 setup_type)
    print("run_test",obj_id)
    assert decw.has_entity_prefix(obj_id)
    eval_context = {key: conn_config.get(key) for key in ['backup_path','self_id','connection_settings','decw']}
    eval_context['self_id'] = obj_id
    agent.evaluate_object_status({**eval_context,'target':'local','status':['object_missing','payload_missing']})
    agent.evaluate_object_status({**eval_context,'target':'remote','status':['complete']})
    agent.evaluate_object_status({**eval_context,'target':'remote_mirror','status':['complete']})

    test_config = TestConfig({**conn_config,
            'decelium_path':decelium_path,
            'obj_id':obj_id,
            'eval_context':eval_context
            })

    print("---- 2: Doing Small Pull")
    obj,new_cids = agent.append_object_from_remote(test_config)

    agent.evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    agent.evaluate_object_status({**eval_context,'target':'remote','status':['complete']})
    agent.evaluate_object_status({**eval_context,'target':'remote_mirror','status':['complete']})

    return test_config

def perliminary_tests():
    agent = SnapshotAgent()
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

def new_corruption_config(setup_config:TestConfig,obj:dict,corruptions:list,pre_evals:list,invalid_props:list,final_evals,do_repair:bool,post_repair_status:bool,push_target:str):    
    corruptions = [CorruptionTestData.Instruction(corruption) for corruption in corruptions]
    pre_evals = [CorruptionTestData.Eval(evaluation) for evaluation in pre_evals]
    final_evals = [CorruptionTestData.Eval(evaluation) for evaluation in final_evals]
    print("corruptions")
    print(json.dumps(corruptions,indent=3))
    return {'setup_config':setup_config,
            'obj':obj,
            'corruptions':corruptions,
            'corruption_evals':pre_evals,
            'invalid_props':invalid_props,
            'do_repair':do_repair,
            'post_repair_status':post_repair_status,
            'final_evals':final_evals,
            'push_target':push_target,
                }


corruption_suffix_full = {
                    'delete_payload':['payload'],
                    'corrupt_payload':['payload'],
                    'remove_attrib':['attrib'], 
                    'rename_attrib_filename':['attrib'],
                    'corrupt_attrib':['attrib'], 
                    'delete_entity':['attrib','payload']
                    }  

corruption_suffix_attrib_only = {
                    'delete_payload':[],
                    'corrupt_payload':[],
                    'remove_attrib':['attrib'], 
                    'rename_attrib_filename':['attrib'],
                    'corrupt_attrib':['attrib'], 
                    'delete_entity':['attrib']
                    }  


def new_repair_corruption_config(corruption_1,
                                 corruption_2,
                                 setup_config,
                                 obj,
                                c_target_1,
                                c_target_2,
                                c_target_reserve,
                                do_repair,
                                push_target,
                                target_type
                                 ):
        # setup_config,                  # The server configuration
        # obj,                           # Object
        # target_type                    # The target type
        
        # The corruption Test -----------------------
        assert type(corruption_1) == str # The first corruption to apply to c_target_1
        assert type(corruption_2) == str # The second corruption to apply to c_target_2
        assert c_target_1 in ['remote','local','remote_mirror'] # The first datasource to corrupt
        assert c_target_2 in ['remote','local','remote_mirror'] # The second datasource to corrupt
        assert c_target_reserve in ['remote_mirror','local'] # The datasource that will be held stable, so we can restore after
        assert do_repair in [True,False] # Are we testing the repair process? (Only relevant for remote and remote_mirror tests)
        assert push_target in ['local','remote'] # Where we would like to push the repair data
        attrib_only_targets = ['host','dict','directory','node']
        corruption_map = corruption_suffix_full
        if (target_type in attrib_only_targets):
            corruption_map = corruption_suffix_attrib_only
        invalid_props = []
        pre_invalid_props = []
        post_invalid_props = []
        # The corruptions we can apply, and what they will break.
        #
        if 'attrib' in corruption_map[corruption_2] and 'attrib' in corruption_map[corruption_1]:
            # If both attributes are corrupt, then no repair can validate the payload
            post_invalid_props = [f'{c_target_1}_payload',f'{c_target_2}_payload']
        post_invalid_remote =  ['_'.join([c_target_1,suffix]) for suffix in corruption_map[corruption_1] if suffix in corruption_map[corruption_2] ]
        post_invalid_remote_mirror =  ['_'.join([c_target_2,suffix]) for suffix in  corruption_map[corruption_2] if suffix in corruption_map[corruption_1]]
        post_invalid_props = post_invalid_props+ post_invalid_remote + post_invalid_remote_mirror

        #
        if 'attrib' in corruption_map[corruption_2]:
            pre_invalid_props =  pre_invalid_props + [f'{c_target_2}_payload']
        if 'attrib' in corruption_map[corruption_1]:
            pre_invalid_props =  pre_invalid_props + [f'{c_target_1}_payload']
        pre_invalid_remote =  ['_'.join([c_target_1,suffix]) for suffix in corruption_map[corruption_1] ]
        pre_invalid_remote_mirror =  ['_'.join([c_target_2,suffix]) for suffix in  corruption_map[corruption_2]]
        pre_invalid_props = pre_invalid_props+ pre_invalid_remote + pre_invalid_remote_mirror

        #
        if do_repair == True:
            invalid_props  = post_invalid_props
        else:
            invalid_props = pre_invalid_props
        print("\n\nINVALID DEBUG IN new_repair_corruption_config")
        print("-- do_repair",do_repair)
        print("-- pre_invalid_remote",pre_invalid_remote)
        print("-- pre_invalid_remote_mirror",pre_invalid_remote_mirror)
        print("-- pre_invalid_props",pre_invalid_props)
        print("\n\n")
        repair_success_expectation = True
        
        if 'payload' in corruption_map[corruption_1] and 'payload' in corruption_map[corruption_2]:
            repair_success_expectation = False
        elif 'attrib' in corruption_map[corruption_1] and 'attrib' in corruption_map[corruption_2]:
            repair_success_expectation = False
        pre_eval_1 = {'target':c_target_1,'status':['object_missing','payload_missing']}
        pre_eval_2 = {'target':c_target_2,'status':['object_missing','payload_missing']}

        # Small patch to acknolwedge that payload corruption of attribute only entities should not have any effect
        if len(pre_invalid_remote) == 0:
            pre_eval_1 = {'target':c_target_1,'status':['complete']} # The corruption should not do anything
            if do_repair == False:
                assert corruption_1 in ['delete_payload','corrupt_payload']
            assert target_type in attrib_only_targets

        if len(pre_invalid_remote_mirror) == 0:
            pre_eval_2 = {'target':c_target_2,'status':['complete']} # The corruption should not do anything
            if do_repair == False:
                assert corruption_2 in ['delete_payload','corrupt_payload']
            assert target_type in attrib_only_targets

        pre_evals = [
            {'target':c_target_reserve,'status':['complete']},
            pre_eval_1,
            pre_eval_2
            ]
        final_evals = []

        if repair_success_expectation == True and do_repair == True:
            final_evals = [
                {'target':c_target_reserve,'status':['complete']},
                {'target':c_target_1,'status':['complete']},
                {'target':c_target_2,'status':['complete']}
                ]
            invalid_props = []
        
        ## TODO refactor for repair. Kind of janky
        
        config = new_corruption_config(setup_config,obj,
            [{'corruption':corruption_1,"mode":c_target_1},
                {'corruption':corruption_2,"mode":c_target_2},],
            pre_evals,
            invalid_props,
            final_evals,
            do_repair,
            repair_success_expectation,
            push_target)
        return config

def get_validation_summary(decw,setup_config):
    validation_data = decw.net.validate_entity({'self_id':setup_config.obj_id()})
    local_validation_attrib = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_attrib')
    local_validation_payload = Snapshot.object_validation_status(decw,setup_config.obj_id(),setup_config.backup_path(),setup_config.connection_settings(),'local_payload')
    validation_data['local_attrib'] = [local_validation_attrib[0]]
    validation_data['local_payload'] = [local_validation_payload[0]]
    return validation_data

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
            config = new_repair_corruption_config(corrupt_remote,
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
        agent.run_corruption_test(corruption_config)


# setup_type - 'ipfs', 'file'
#test_type - 'remote_repair', 'remote_no_repair', 'local_no_repair'

# setup_type = 'ipfs'
# setup_type =  'json'
# setup_type = 'file'
# setup_type =  'host' # Awaiting DNS update 
# setup_type =  'directory' # Requires some refatoring
# setup_type =  'user' # *Should* work



test_types = ['remote_repair','remote_no_repair','local_no_repair']
file_types = ['ipfs','json','file','host','directory','user']
remote_types = CorruptionTestData.Instruction.corruption_types
remote_mirror_types = CorruptionTestData.Instruction.corruption_types


file_types = ['ipfs']
test_types = ['remote_repair']
remote_types = ['delete_payload']
remote_mirror_types = ['remove_attrib']

for test_type in test_types:
    for file_type in file_types:
        test_corruptions_repair(file_type,test_type,remote_types,remote_mirror_types)
print ("FINISHED")
# test_corruptions(setup_type)


# TODO -- compare backed up file bytes with actual file bytes for all file mirrors