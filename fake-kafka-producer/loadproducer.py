from faker.providers import BaseProvider
from faker.proxy import Faker
from uuid import uuid4
from datetime import datetime

import random
import string

class LoadProvider(BaseProvider):
    @staticmethod
    def generate_hostname():
        return ''.join(
            random.choice(string.ascii_lowercase + string.digits + "-" * 10)
            for _ in range(73)
        )
    
    @staticmethod
    def generate_ts_name():
        timeseries_names = [
            "CPU",
            "MemoryBytes",
            "ReaminingBytes",
            "UsedBytes",
            "InfoErrorCount",
            "FatalErrorCount",
            "RandomCount",
        ]
        return random.choice(timeseries_names)


    def produce_msg(self):
        hostname = self.generate_hostname()
        message = {
            "body": {
                "value": random.uniform(0,1),
                "timestamp": str(datetime.now()),
                "labels": {
                    "hostname": hostname,
                    "VictoriaMetricsAccountID:": random.randint(1e10, 1e11),
                    "deviceIPAddress": "10.10.12.23",
                    "__name__": self.generate_ts_name(),
                    "is_blackout": "false",
                    "entityName": self.generate_hostname() + "@volume@nfs",
                    "entityTypeId": "K8S_POD_VOLUME",
                    "entityId": self.generate_hostname() + "",
                    "source": "HM",
                    "deviceId": str(uuid4()),
                    "messageType": "METRIC_DATA",
                    "tenantId": "1237891273892",
                    "source": "PA",
                }
            }
        }
        key = {"hostname": hostname}
        return message, key

