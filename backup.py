import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint
from Migrator import Migrator
from Snapshot import Snapshot
import pandas
import shutil
import pprint
# System Backup and restore
# [ ] Can restore all object types
# [ ] Can restore backed up object
# [ ] ---- When objects are uploaded, a hash gets stored on the server
# [ ] ---- if hash exists, restore_entity will function correctly
# [ ] ---- Some types

# ---------------
# [ ] - Refactor Snapshot Unit test for completeness and clarity
# [ ] - Push data source code into data sources
# [ ] - Create Snapshot Summary builders
# [ ] - Unit test Snapshot summaries
# [ ] - Change Migrator into some kind of Decorator
# [ ] - Datasources use the Migrator to move data
# Old notes
# -------


# pprint.pprint(df.iloc[5].to_dict())
# If it is invalid, it should still download

# TODO
# Validate current status
# Download Fresh data
# Restore missing data
# filter: {SOME-FILTER}
# 1 finish     object_validation_status refactor
'''
        results[obj_id] = {'self_id':obj_id}       
            local_results = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,'local')
            remote_results = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,'remote')
            results[obj_id].update(local_results)
            results[obj_id].update(remote_results)
'''
# 2. test append again
# 2. update old unit tests
# push_to_remote(decw,api_key, connection_settings, download_path,limit=20, offset=0):
# Write end-to-end test
# - push to remote
# - append
# - remove file
# - append
# - remote change
# - pull
# - delete remote
# - push
# - end



# local
# - items: 127
# - invalid: 120
# - correct_data: 12
# - correct_meta_data: 20
# - correct_meta_data: 20

# remote_invalid: 180
# 

# 3 - Add unit test to Corrupt a local, and restore from remote using pull & append
# 4 - Add unit test to Corrupt a remote, and restore from local using push
# 5 - Add unit test to Update a remote, and then update local from remote
# 6 - Add unit test to Update a local, and then push the update to remote
# 7 - Add in a unit test that can clean local orphans
# 4 - add in a timestamp to all uploads, so it is obvious which is newer
# 5 - add in a tombstone upon file uploads, to store a record of an object even after deletion
# X - COMPARE SIMILARITY
# 7 - Add in a unit test that can clean remote orphans
# 6b - the update should be impossible, if the updater is not the owner
# 6b - the update should be possible, if the updater is not the owner
# 6b - the update should be possible, if the file is just missing


def run_snapshot_job(func):
    # wallet = '../.wallet.dec'
    
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

    backup_path = "../decelium_backup/"

    limit = 10
    offset = 30
    objs = []
    found_objs = func(decw, connection_settings, backup_path, limit, offset)
    objs = found_objs
    offset = offset + limit
    return objs
    while (len(found_objs) >= limit):
        found_objs = func(decw, connection_settings, backup_path, limit, offset)
        offset = offset + limit
        objs = {**objs , **found_objs}
        #pprint.pprint(objs)
    return objs
# FULL SYNC
#run_snapshot_job(Snapshot.append_from_remote)
objs = run_snapshot_job(Snapshot.push_to_remote)


import pprint
pprint.pprint(objs)
# obj-916eccd6-6958-4dc6-990d-669e34f9325f