import os
from common.configManager import ConfigManager

def get_root_paths():
    files_to_be_uploaded = []

    # Getting data
    config = ConfigManager()
    root_path = config.get('root_path')

    for root, dirnames, _ in os.walk(root_path):
        for dirname in dirnames:
            if dirname.startswith(config.get("folder_starting_word")):
                yield os.path.join(root, dirname)