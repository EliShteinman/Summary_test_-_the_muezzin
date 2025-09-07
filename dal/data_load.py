from datetime import datetime
from pathlib import Path
import os
from pprint import pprint
import logging
import config

logger = logging.getLogger(__name__)

class DataLoad:
    def __init__(self, directory_path: str = config.DIRECTORY_PATH):
        self.directory_path = Path(directory_path)
        if not os.path.exists(directory_path):
            logger.warning(f"Directory {directory_path} does not exist.")
            raise FileNotFoundError(f"Directory {directory_path} does not exist.")


        logger.info(f"Working directory set to {self.directory_path}")

    def load_meta_data(self):
        for file in self.directory_path.iterdir():
            if file.suffix == ".wav":
                logger.info(f"Loading metadata from {file}")
                data = {
                "file_path" : f"{file.resolve()}",
                "meta_data" : {
                    "file_suffix" : file.suffix.replace(".", ""),
                    "file_name" : file.stem,
                    "file_size" : file.stat().st_size,
                    "file_creation_time" : datetime.fromtimestamp(file.stat().st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
                    "file_modification_time" : datetime.fromtimestamp(file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    "file_access_time" : datetime.fromtimestamp(file.stat().st_atime).strftime("%Y-%m-%d %H:%M:%S"),
                    "file_permissions" : datetime.fromtimestamp(file.stat().st_mode).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                }
                yield data
            else:
                logger.warning(f"File {file} does not have a .wav extension.")
                continue



if __name__ == "__main__":
    data_load = DataLoad("C:\podcasts")
    for file in data_load.load_meta_data():
        pprint(file)