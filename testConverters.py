from server.serverUtils import WorkByteConverter
import json

class SimpleJSONConverter(WorkByteConverter):
    def __init__(self):
        super().__init__()
    
    def fromBytes(self, data):
        return json.loads(bytes.decode(data,encoding="utf-8"))
    
    def toBytes(self, data):
        return bytes(json.dumps(data) ,encoding="utf-8")
    
