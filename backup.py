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

def run_snapshot_job(func):
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
    offset = 0
    objs = []
    found_objs = func(decw, connection_settings, backup_path, limit, offset)
    objs = found_objs
    offset = offset + limit
    #return objs
    while (len(found_objs) >= limit):
        found_objs = func(decw, connection_settings, backup_path, limit, offset)
        offset = offset + limit
        objs = {**objs , **found_objs}
        #pprint.pprint(objs)
    return objs
# def validate_backup()
# validate_local_object(decw,object_id,download_path,connection_settings):
# create_backup(Snapshot.append_from_remote)
results = run_snapshot_job(Snapshot.validate_snapshot)
#import pprint
#pprint.pprint(results)
for result in results.values():
    if result['remote']==True:
        pprint.pprint(result)

# TODO
# SESSION 1
# 1 - Have failure reasons that attach in a list

# 2 - add ability to filter / search a snapshot by object IDs, or by paths
# 3 - add in data about similarity between local and remote
# 4 - add in a timestamp to all uploads, so it is obvious which is newer

# SESSION 2
# 5 - add in a tombstone upon file uploads, to store a record of an object even after deletion

# SESSION 3
# 3 - Add unit test to Corrupt a local, and restore from remote using pull & append

# SESSION 4
# 4 - Add unit test to Corrupt a remote, and restore from local using push

# SESSION 5
# 5 - Add unit test to Update a remote, and then update local from remote

# SESSION 6
# 6 - Add unit test to Update a local, and then push the update to remote

# SESSION 7
# 7 - Add in a unit test that can clean local orphans

# SESSION 8
# 7 - Add in a unit test that can clean remote orphans
# 6b - the update should be impossible, if the updater is not the owner
# 6b - the update should be possible, if the updater is not the owner
# 6b - the update should be possible, if the file is just missing

        
# 5 - 