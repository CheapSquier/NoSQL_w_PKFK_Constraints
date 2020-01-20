class FakeAerospike:
    'Class to use when aerospike client is not available.'
            
    #constructor
    def __init__(self, config):
        print("Init FakeAerospike")
    def connect():
        print("Opening fake connector to non-existant Aerospike database")
        return
    def close(self):
        print("Closing fake connector to non-existant Aerospike database")
        return
    def put(self, key, jsonString):
        print("Putting JSON string into non-existant Aerospike database")
        return
    def get(self, key):
        print("Returning nothing from non-existant Aerospike database")
        return "this", "that", "NoValueString"
    def get_key_digest(self, namespace, tablename, str_of_currentPKvals):
        print("Returning bogus hashkey from non-existant Aerospike database")
        return "BogusHashString"   