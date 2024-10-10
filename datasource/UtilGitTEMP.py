import os
import shutil
import subprocess
import json
import hashlib
import requests
from decelium_wallet.commands.BaseService import BaseService  # Assuming your BaseService from earlier
import decelium_wallet.core as core

class UtilGit(BaseService):
    @classmethod
    def run(cls, **kwargs):
        print(f"MyService running with arguments: {kwargs}")
        return 0  # Success exit code

if __name__ == "__main__":
    UtilGit.run_cli()  # Inherit CLI behavior from BaseService

# python3 UtilGit.py list_github_repos github_user=justin.girard access_token=TOKEN
# python3 UtilGit.py download_git_data branch=master repo_url=https://github.com/justingirard/beanleaf download_path='./git_backup_test/' download_dir='beanleaf' username=justin.girard access_key=TOKEN
