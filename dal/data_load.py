from datetime import datetime
from pathlib import Path
import os
from pprint import pprint
import logging
import config

logger = logging.getLogger(__name__)


def load_meta_data_for_directory(directory_path):
    if not os.path.exists(directory_path):
        logger.warning(f"Directory {directory_path} does not exist.")
        raise FileNotFoundError(f"Directory {directory_path} does not exist.")
    directory_path = Path(directory_path)
    for file in directory_path.iterdir():
        yield load_meta_data_for_file(file)

def load_meta_data_for_file(fath):
    file = Path(fath)
    if not file.exists():
        logger.warning(f"File {file} does not exist.")
        return None
    if file.suffix == ".wav":
        logger.info(f"Loading metadata from {file}")
        meta_data = {
            "file_path": f"{file.resolve()}",
            "meta_data": {
                "file_suffix": file.suffix.replace(".", ""),
                "file_name": file.stem,
                "file_size": file.stat().st_size,
                "file_creation_time": datetime.fromtimestamp(file.stat().st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
                "file_modification_time": datetime.fromtimestamp(file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "file_access_time": datetime.fromtimestamp(file.stat().st_atime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        }
        logger.info(f"Metadata loaded from {file} : {meta_data}")
        return meta_data
    else:
        logger.warning(f"File {file} does not have a .wav extension.")
        return None



if __name__ == "__main__":
    # for data in load_meta_data_for_directory(config.DIRECTORY_PATH):
    #     pprint(data)

    print(Path(r"C:\podcasts\download.wav"))
    meta_data = load_meta_data_for_file(Path(r"C:\podcasts\download.wav"))
    pprint(meta_data)