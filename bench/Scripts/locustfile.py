from locust import HttpLocust, TaskSet, task
import json
import random

class UserBehavior(TaskSet):
    def __init__(self, *args, **kwargs):
        super(UserBehavior, self).__init__(*args, **kwargs)
        self.cert_path = "C:/work/locust.pem"

    @task(5)
    def write(self):
        num = random.randint(0, 8500) + 1000
        headers = {'content-type': 'application/json'}
        document = {"Name": "Fitzchak {0}".format(num), "Supplier": 1, "Category": 1, "QuantityPerUnit": 1,
                    "PricePerUnit": 1,
                    "UnitsInStock": 1, "UnitsOnOrder": 1, "Discontinued": False, "ReorderLevel": 1,
                    "@metadata": {"@collection": "Benchmarks"}}
        self.client.put("/databases/BenchmarkDB/docs?id=temp/", data=json.dumps(document), headers=headers,
                        cert=self.cert_path)

    @task(2)
    def query(self):
        num = random.randint(0, 8500) + 1000
        headers = {'content-type': 'application/json'}
        document = {"Query": "FROM Benchmarks WHERE Name = $p0",
                    "QueryParameters": {"p0": "Fitzchak {0}".format(num)}}
        self.client.post("/databases/BenchmarkDB/queries?pageSize=5".format(num), data=json.dumps(document),
                         headers=headers, cert=self.cert_path)

    @task(1)
    def read(self):
        num = random.randint(0, 8500) + 1000
        self.client.get("/databases/BenchmarkDB/docs?id=temp/000000000000000{0}-A".format(num), cert=self.cert_path, name="/databases/BenchmarkDB/docs?id=[id]")


class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 1
    max_wait = 1
