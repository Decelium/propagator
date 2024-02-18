import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint
from Migrator import Migrator
import pandas
import shutil
'''
Backups are likely the MOST important aspect of Decelium.
This file tests the Migrator, a small utility that is the powerhouse behind creating and restoring backup data from the
Decelium network. 
'''

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
    Low level test that verifies that the IPFS Migrator is backing up IPFS *files* in a manner that is perfect down to the bit.
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
    Low level test that verifies that the IPFS Migrator is backing up IPFS *FOLDERS* in a manner that is perfect down to the bit.
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
    Low level test that verifies that the IPFS Migrator is backing up Decelium Objects in a manner that is perfect down to the bit.
    '''
    # [ ] create test data 
    decw = core()
    with open('../.wallet.dec','r') as f:
        data = f.read()
    with open('../.wallet.dec.password','r') as f:
        password = f.read()
    loaded = decw.load_wallet(data,password)
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

    singed_req = decw.dw.sr({**user_context, **{
            'path':'temp/test_folder.ipfs'}})
    del_try = decw.net.delete_entity(singed_req)

    singed_req = decw.dw.sr({**user_context, **{
            'path':'temp/test_folder.ipfs',
            'file_type':'ipfs',
            'payload_type':'ipfs_pin_list',
            'payload':pins}})
    obj_id = decw.net.create_entity(singed_req)
    assert 'obj-' in obj_id

    # -- Download Object --
    obj = decw.net.download_entity( {**user_context, **{'self_id':obj_id,'attrib':True}})
    obj_old = obj.copy()

    assert 'settings' in obj
    assert 'ipfs_cid' in obj['settings']
    assert 'ipfs_cids' in obj['settings']
    new_cids = [obj['settings']['ipfs_cid']]
    
    # -- Verify Pin Exists --
    for cid in obj['settings']['ipfs_cids'].values():
        new_cids.append(cid)

    all_cids = Migrator.find_all_cids(decw)
    df = pandas.DataFrame(all_cids)
    all_cids = list(df['cid'])
    is_subset = set(new_cids) <= set(all_cids)
    assert is_subset

    # -- Download Backup --
    download_obj_path = './test/testobjbackup'
    # download_path = './test/testbackup'
    try:
        shutil.rmtree(download_obj_path)
        shutil.rmtree(download_path)
    except:
        pass
    # Migrator.download_ipfs_data(new_cids, download_path, connection_settings)
    Migrator.download_object(decw,obj_id, download_obj_path, connection_settings)
    

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
    # -- Verify CIDS off of IPFS
    # TODO -- Verify the CIDS are also off IPFS
    

    # -- Upload IPFS only Backup & Verify Correctness -- 
    cids_reuploaded =  Migrator.upload_ipfs_data(decw,download_obj_path+'/'+obj_id,connection_settings) 
    assert len(cids_reuploaded) > 0
    
    #result = decw.net.remove_ipfs({
    #        'api_key':"UNDEFINED",
    #        'file_type':'ipfs', 
    #        'connection_settings':connection_settings,
    #        'payload_type':'cid',
    #        'payload':backup_cids})
    
    # -- Restore the object with object re-upload -- 
    query = Migrator.upload_object_query(decw,obj_id, download_obj_path, connection_settings)
    result = decw.net.create_entity(decw.dw.sr({**query,**user_context},["admin"]))
    assert 'obj-' in result
    obj_new = decw.net.download_entity( {**user_context, **{'self_id':result,'attrib':True}})
    assert obj_new['self_id'] == obj_old['self_id']
    assert obj_new['parent_id'] == obj_old['parent_id']
    assert obj_new['dir_name'] == obj_old['dir_name']
    assert obj_new['settings']['ipfs_cid'] == obj_old['settings']['ipfs_cid']
    assert obj_new['settings']['ipfs_name'] == obj_old['settings']['ipfs_name']
    for key in obj_new['settings']['ipfs_cids'].keys():
        assert obj_new['settings']['ipfs_cids'][key] ==   obj_old['settings']['ipfs_cids'][key]
    
    # assert 'error' in success
    #success = Migrator.upload_object(decw,obj_id, download_obj_path+'/'+obj_id, connection_settings)
    # assert success == True
    all_cids = Migrator.find_all_cids(decw)
    df = pandas.DataFrame(all_cids)
    all_cids = list(df['cid'])
    is_subset = set(new_cids) <= set(all_cids)
    assert is_subset == True
    

# run_ipfs_backup() 
# test_ipfs_file_backup()
# test_ipfs_folder_backup()
test_object_backup()


def quick_test():
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
    q = {'api_key': 'e66eebeb3b56bd627c082a36fb0528e45d1fa8d6a1b9e47d478c3af9a11baaf6431bfdb491ceb6d8c5a3674433dcf5a1a1f9af74cf5a9414d026b68fdcedfc5d',
        'file_type': 'ipfs',
        'name': 'test_folder.ipfs',
        'no_validation':True,
        'parent_id': 'dir-948b86b7-927d-456e-ba33-a2ea2a3bea2e',
        'payload': [{'cid': 'QmVKugVyynbLDmwgxHm9Z6JZMjqtyVNH6MqgxTxhTXX2US',
                    'name': '.DS_Store',
                    'root': True},
                    {'cid': 'QmQkBHa6uAcVm8bwfoufcmAiG25vfNYdo3Lvrt9Q7QWmZR',
                    'name': 'test.txt',
                    'root': True},
                    {'cid': 'QmYZsomCw9J9Fb8hLgiB7iA3W1iTYnLi7hbJXq3Bggz2rL',
                    'name': 'img_test.png',
                    'root': True},
                    {'cid': 'QmSi9vz6qz2rnES8hhv288UYK9MSWLo4p9HZcszDYdRXsF',
                    'name': 'test_sub/a_file.txt',
                    'root': True},
                    {'cid': 'QmbGSb2Gerf3WQUeS78yvEcYcSkTvurQghktXT3y9Fao6S',
                    'name': 'test_sub',
                    'root': True},
                    {'cid': 'QmS13EHyNHj8sYwXCnLFNrUQobr9EyoDsBTaFzm2ij5dPG',
                    'name': '',
                    'root': True}],
        'payload_type': 'ipfs_pin_list',
        'self_id': 'obj-a82f695f-1474-4bd9-8df3-df2eebbf6bb0'}
    # result = decw.net.delete_entity(decw.dw.sr({'api_key':q['api_key'],'self_id':q['self_id']},["admin"]))
    result = decw.net.delete_entity(decw.dw.sr({'api_key':q['api_key'],'parent_id':q['parent_id'],'name':q['name']},["admin"]))
    print({'api_key':q['api_key'],'parent_id':q['parent_id'],'name':q['name']})
    print(result)


    new_obj = decw.net.create_entity(decw.dw.sr(q,["admin"]))
    print(new_obj)
    #obj_new = decw.net.download_entity( {'self_id':new_obj,'attrib':True})


    #import pprint
    #pprint.pprint(obj_new)

# quick_test()