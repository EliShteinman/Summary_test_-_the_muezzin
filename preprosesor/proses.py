import hashlib



class Proses:
    def __init__(self, mongodb, elasticsearch):
        self.mongodb = mongodb
        self.elasticsearch = elasticsearch

    async def proses(self, data:dict):
        path = data['file_path']
        meta_data = data['meta_data']
        file_hash = self.get_audio_file_hash(path)


    def get_audio_file_hash(self, file_path, algorithm='sha256', buffer_size=65536):
        """
        Calculates the hash of an audio file.

        Args:
            file_path (str): The path to the audio file.
            algorithm (str): The hashing algorithm to use (e.g., 'md5', 'sha1', 'sha256').
            buffer_size (int): The size of chunks to read the file in (in bytes).

        Returns:
            str: The hexadecimal representation of the file's hash.
        """
        try:
            hasher = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                while True:
                    data = f.read(buffer_size)
                    if not data:
                        break
                    hasher.update(data)
            print(hasher.hexdigest())
            return hasher.hexdigest()
        except FileNotFoundError:
            return f"Error: File not found at {file_path}"
        except Exception as e:
            return f"An error occurred: {e}"