from .Action import Action
from .AppendObjectFromRemote import AppendObjectFromRemote
from .CorruptObject import CorruptObject
from .EvaluateObjectStatus import evaluate_object_status
from .CreateDecw import CreateDecw
from .PullObjectFromRemote import PullObjectFromRemote
from .UploadDirectoryToRemote import upload_directory_to_remote
from .ChangeRemoteObjectName import ChangeRemoteObjectName
from .DeleteObjectFromRemote import DeleteObjectFromRemote
from .PushFromSnapshotToRemote import PushFromSnapshotToRemote
from .RunCorruptionTest import RunCorruptionTest

class SnapshotAgent():
    '''
    a Functional agent. Meaning each action is an explanable function, that is technically independent, which can be verified fully by 
    inspecting any parameters and context passed.

    IMPORTANT: Agents are for unit tests, tutorials, and doc generation. They are heavy and not intended for production systems, rather, one is supposed to
    solve a problem with an agent, then inspect each action to understand how to use the API to solve the same problem with a lower level script.
    '''
    create_wallet_action = CreateDecw()
    append_object_from_remote = AppendObjectFromRemote()
    delete_object_from_remote = DeleteObjectFromRemote()
    push_from_snapshot_to_remote = PushFromSnapshotToRemote()
    corrupt_object = CorruptObject()
    change_remote_object_name = ChangeRemoteObjectName()
    pull_object_from_remote = PullObjectFromRemote()
    upload_directory_to_remote = upload_directory_to_remote
    evaluate_object_status = evaluate_object_status
    run_corruption_test = RunCorruptionTest()