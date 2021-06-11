import DB_Connection as dbc
from configparser import ConfigParser

class Zoom:
    def __init__(self):
        self.con = dbc.DBconnection()
        self.headers = self.set_header_auth(self)
        self.payload = {}

    def set_header_auth(self):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read('./configs/zoom.ini')
        headers = {}
        if parser.has_section('Auth'):
            params = parser.items('Auth')
            for param in params:
                headers[param[0]] = param[1]
            print(headers)
            return headers 
        else:
            raise Exception('Section {0} not found in the file'.format('Auth'))
