import base64

class Encoding:
    @staticmethod
    def encode_base64(data):
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')

    @staticmethod
    def decode_base64(data):
        return base64.b64decode(data).decode('utf-8')