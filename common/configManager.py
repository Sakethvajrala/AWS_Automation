import yaml
import os
import logging
class ConfigManager:
    def __init__(self, config_path="common/config.yml"):
        self.config = {}
        self.load_data(config_path)

    def load_data(self, path):
        if not os.path.exists(path):
            logging.error("Path doesn't exist")
        with open(path, "r") as f:
            self.config = yaml.safe_load(f)

    def get(self, key, default=None):
        return self.config.get(key, default)