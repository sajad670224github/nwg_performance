from clickhouse_driver import Client
from nwg_performance import settings

class ClickhouseApi:
    def __init__(self, cluster):
        if cluster == "new":
            username = settings.CLICKHOUSE.get("CL2_USERNAME")
            password = settings.CLICKHOUSE.get("CL2_PASSWORD")
            host = settings.CLICKHOUSE.get("CL2_HOST")
            port = settings.CLICKHOUSE.get("CL2_PORT")
            settings_ = None
            database='ios'
        else:
            username = "admin"
            password = "admin_password_123"
            host = 'clickhouse'
            port = 9000
            database='nwg'
            settings_ = {'connect_timeout': 10, 'send_receive_timeout': 30}

        self.client = Client(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            settings=settings_,
            compression='lz4')
        self.log_table = 'log'

    def close(self):
        self.client.disconnect()