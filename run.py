import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint
from Migrator import Migrator
import pandas
import shutil


def run_ipfs_backup():
    decw = core()
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"}
    connected = decw.initial_connect(target_url="https://dev.paxfinancial.ai/data/query",api_key="UNDEFINED")
    found = Migrator.find_all_cids(decw,0,100)
    Migrator.download_ipfs_data(found, './ipfs_backup/',connection_settings)

    print("finished")
    print(found)


def test_ipfs_file_backup():
    '''
    Low level test that verifies that the IPFS Migrator is backing up IPFS files in a manner that is perfect down to the bit.
    '''
    # [ ] create test data 
    decw = core()
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"}
    connected = decw.initial_connect(target_url="https://dev.paxfinancial.ai/data/query",api_key="UNDEFINED")

    # Upload to IPFS directly, & verify
    pins = decw.net.create_ipfs({
            'api_key':"UNDEFINED",
            'file_type':'ipfs', 
            'connection_settings':connection_settings,
            'payload_type':'local_path',
            'payload':'./test/testdata/img_test_1.png'})
    print(pins)
    assert len(pins) > 0
    assert 'cid' in pins[0]

    # Backup from IPFS locally & verify
    found = [{'cid':pins[0]['cid'],'self_id':None}]
    Migrator.download_ipfs_data(found, './test/testbackup',connection_settings)
    file_path = './test/testbackup/'+pins[0]['cid']+'.file'

    # Assert the file exists
    assert os.path.exists(file_path), "The backup file does not exist."
    with open('./test/testdata/img_test_1.png', 'rb') as original_file:
        original_content = original_file.read()
    with open(file_path, 'rb') as backup_file:
        backup_content = backup_file.read()
    assert original_content == backup_content, "The contents of the files are not identical."


    # Unpin from IPFS & Verify
    result = decw.net.remove_ipfs({
            'api_key':"UNDEFINED",
            'file_type':'ipfs', 
            'connection_settings':connection_settings,
            'payload_type':'cid',
            'payload':[pins[0]['cid'],"Abject_Failure"]})
    assert result[pins[0]['cid']]['removed'] == True
    # LS and verify no pin exists for file
    # TODO LS and verify
    # Reupload backup & Verify
    pins_new = decw.net.create_ipfs({
            'api_key':"UNDEFINED",
            'file_type':'ipfs', 
            'connection_settings':connection_settings,
            'payload_type':'local_path',
            'payload':file_path})
    assert len(pins_new) > 0
    assert 'cid' in pins_new[0]
    assert 'cid' in pins[0]
    assert pins[0]['cid'] == pins_new[0]['cid']
    # TODO LS and verify


def test_ipfs_folder_backup():
    '''
    Low level test that verifies that the IPFS Migrator is backing up IPFS folders in a manner that is perfect down to the bit.
    '''
    # [ ] create test data 
    decw = core()
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"}
    connected = decw.initial_connect(target_url="https://dev.paxfinancial.ai/data/query",api_key="UNDEFINED")

    # Upload to IPFS directly, & verify
    pins = decw.net.create_ipfs({
            'api_key':"UNDEFINED",
            'file_type':'ipfs', 
            'connection_settings':connection_settings,
            'payload_type':'local_path',
            'payload':'./test/testdata/test_folder'})
    print(pins)
    assert len(pins) > 0
    assert 'cid' in pins[0]
    # Backup from IPFS locally & verify
    
    found = []
    # TODO - streamline download interface
    root = {}
    for pin in pins:
        print(pin)
        found.append({'cid':pin['cid'],'self_id':None})
        root = {'cid':pin['cid'],'self_id':None}
            
    Migrator.download_ipfs_data(found, './test/testbackup',connection_settings)
    file_path = './test/testbackup/'+pins[0]['cid']+'.file'

    # Assert all the files exist & match  -
    assert len(pins) == 6
    for pin in pins:
        if Migrator.is_directory(decw,connection_settings,pin['cid']):
            continue
        path_original = './test/testdata/test_folder/'+pin['name']
        path_destination = './test/testbackup/'+pin['cid']+".file"
        print(pin)
        assert os.path.exists(path_original)
        assert os.path.exists(path_destination)
        with open(path_original, 'rb') as original_file:
            original_content = original_file.read()
        with open(path_destination, 'rb') as backup_file:
            backup_content = backup_file.read()
        assert original_content == backup_content, "The contents of the files are not identical."

        # Unpin from IPFS & Verify
        result = decw.net.remove_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'cid',
                'payload':[pin['cid'],"Abject_Failure"]})

        assert result[pin['cid']]['removed'] == True
        
        # Reupload backup & Verify
    
        pins_new = decw.net.create_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'local_path',
                'payload':path_destination})
        assert len(pins_new) > 0
        assert 'cid' in pins_new[0]
        assert pin['cid'] == pins_new[0]['cid']

        # result = decw.net.remove_ipfs({
        #        'api_key':"UNDEFINED",
        #        'file_type':'ipfs', 
        #        'connection_settings':connection_settings,
        #        'payload_type':'cid',
        #        'payload':[pin['cid']]})
        # assert result[pin['cid']]['removed'] == True


    for pin in pins:
        print("searching for dir")
        print(pin)
        print(Migrator.is_directory(decw,connection_settings,pin['cid']))
        if not Migrator.is_directory(decw,connection_settings,pin['cid']):
            continue
        print("PROCESSING AS DIR")
        path_folder_dest = './test/testbackup/'+pin['cid']+".dag"
        folder_json = {}
        with open(path_folder_dest,'r') as f:
            folder_json = json.loads(f.read())

        # Unpin from IPFS & Verify
        result = decw.net.remove_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'cid',
                'payload':[pin['cid'],"Abject_Failure"]})
        print ("LAST STEP--------------")
        print(folder_json['Links'])
        print(str(folder_json['Links']))
        print ("LAST STEP 2--------------")

        result = decw.net.create_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'ipfs_pin_list',
                'payload':folder_json['Links']})
        print("Restored dir cid:")
        print(result)
        print(result[0])
        print("From dir cid:")
        print(pin)
        assert result[0]['cid'] == pin['cid']
        print(folder_json)
    # TODO LS and verify

def test_object_backup():
    '''
    Low level test that verifies that the IPFS Migrator is backing up IPFS files in a manner that is perfect down to the bit.
    '''
    # [ ] create test data 
    decw = core()
    with open('../.wallet.dec','r') as f:
        data = f.read()
    with open('../.wallet.dec.password','r') as f:
        password = f.read()
    loaded = decw.load_wallet(data,password)
    print(decw.dw.list_accounts())
    assert loaded == True
    user_context = {
            'api_key':decw.dw.pubk()}
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"}
    connected = decw.initial_connect(target_url="https://dev.paxfinancial.ai/data/query",
                                      api_key=user_context['api_key'])

    ipfs_req_context = {**user_context, **{
            'file_type':'ipfs', 
            'connection_settings':connection_settings
        }}
    # Upload to IPFS directly, & verify

    # -- Upload Object --
    pins = decw.net.create_ipfs({**ipfs_req_context, **{
            'payload_type':'local_path',
            'payload':'./test/testdata/test_folder'
     }})
    print(pins)
    singed_req = decw.dw.sr({**user_context, **{
            'path':'temp/test_folder.ipfs'}})
    obj_id = decw.net.delete_entity(singed_req)
    singed_req = decw.dw.sr({**user_context, **{
            'path':'temp/test_folder.ipfs',
            'file_type':'ipfs',
            'payload_type':'ipfs_pin_list',
            'payload':pins}})
    obj_id = decw.net.create_entity(singed_req)
    print(obj_id)
    assert 'obj-' in obj_id

    # -- Download Object --
    obj = decw.net.download_entity( {**user_context, **{'self_id':obj_id,'attrib':True}})
    import pprint
    pprint.pprint(obj)
    assert 'settings' in obj
    assert 'ipfs_cid' in obj['settings']
    assert 'ipfs_cids' in obj['settings']
    new_cids = [obj['settings']['ipfs_cid']]

    # -- Verify Pin Exists --
    for cid in obj['settings']['ipfs_cids'].values():
        new_cids.append(cid)
    print(new_cids)

    all_cids = Migrator.find_all_cids(decw)
    df = pandas.DataFrame(all_cids)
    all_cids = list(df['cid'])
    # print(all_cids)
    is_subset = set(new_cids) <= set(all_cids)
    assert is_subset
    print(new_cids)

    # -- Download Backup --
    download_path = './test/testobjbackup'
    try:
        shutil.rmtree(download_path)
    except:
        pass
    Migrator.download_ipfs_data(new_cids, download_path, connection_settings)
    

    # -- Remove Object --
    singed_req = decw.dw.sr({**user_context, **{
            'path':'temp/test_folder.ipfs'}})
    del_res = decw.net.delete_entity(singed_req)
    assert del_res == True
    # -- Verify Download Object Fails --
    obj = decw.net.download_entity( {**user_context, **{'self_id':obj_id,'attrib':True}})
    assert 'error' in obj

    
    # -- Verify Pin Missing --
    all_cids = Migrator.find_all_cids(decw)
    df = pandas.DataFrame(all_cids)
    all_cids = list(df['cid'])
    is_subset = set(new_cids) <= set(all_cids)
    assert is_subset == False


    # -- Upload Backup & Verify Correctness -- 
    print(new_cids)
    backup_cids = []
    for root, dirs, files in os.walk(download_path):
        for file in files:
            file_path = os.path.join(root, file)
            if file_path.endswith('.file'):
                payload_type = 'local_path'
            if file_path.endswith('.dag'):
                payload_type = 'ipfs_pin_list'

            result = decw.net.create_ipfs({**ipfs_req_context,
                    'payload_type':payload_type,
                    'payload':file_path})
            print(result)
            backup_cid = result[0]['cid']
            backup_cids.append(backup_cid)
            print(file)
            print(backup_cid)
            assert backup_cid in new_cids

# run_ipfs_backup() 
# test_ipfs_file_backup()
# test_ipfs_folder_backup()
test_object_backup()