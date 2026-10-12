import json
import os

CONFIG_PATH = "config.json" 

def load_config(config_path = CONFIG_PATH) -> dict:
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    raise FileNotFoundError(f"Missing config file: {config_path}")
