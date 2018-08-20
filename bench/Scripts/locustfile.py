from locust import HttpLocust, TaskSet, task
import json

class UserBehavior(TaskSet):
    @task
    def write(l):
        headers = {'content-type': 'application/json'}
        document = {"Name": "test1", "Supplier": 1, "Category": 1, "QuantityPerUnit": 1, "PricePerUnit": 1,
                    "UnitsInStock": 1, "UnitsOnOrder": 1, "Discontinued": False, "ReorderLevel": 1,
                    "@metadata": {"@collection": "Benchmarks"}}
        l.client.put("/databases/BenchmarkDB/docs?id=temp/", data=json.dumps(document), headers=headers, cert="C:/work/locust.pem")

class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 1
    max_wait = 1
