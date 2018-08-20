from locust import HttpLocust, TaskSet, task
import json
import random

class UserBehavior(TaskSet):
    def __init__(self, *args, **kwargs):
        super(UserBehavior, self).__init__(*args, **kwargs)
        self.cert_path = "C:/work/locust.pem"

    @task
    def write(self):
        headers = {'content-type': 'application/json'}
        document = {"Name": "test1", "Supplier": 1, "Category": 1, "QuantityPerUnit": 1, "PricePerUnit": 1,
                    "UnitsInStock": 1, "UnitsOnOrder": 1, "Discontinued": False, "ReorderLevel": 1,
                    "@metadata": {"@collection": "Benchmarks"}}
        self.client.put("/databases/BenchmarkDB/docs?id=temp/", data=json.dumps(document), headers=headers, cert=self.cert_path)

    @task
    def read(self):
        num = random.randint(0, 8500000) + 1000000
        self.client.get("/databases/BenchmarkDB/docs?id=temp/000000000000{0}-A/".format(num), cert=self.cert_path)


class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 1
    max_wait = 1
