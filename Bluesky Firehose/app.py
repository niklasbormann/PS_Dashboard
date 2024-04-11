import json
from elasticsearch import Elasticsearch
from Firehose import run_data_collection

client = Elasticsearch(hosts='https://localhost:9200',
                       basic_auth=("elastic", "GML0OBa-2=sjGhskAf*_"),
                       ca_certs=r'C:\Users\Nikla\Downloads\elasticsearch-8.13.1-windows-x86_64\elasticsearch-8.13.1\config\certs\http_ca.crt'
                       )

F = ["Ukraine", "Russia", "kyiv"]

#run_data_collection(10, F, 'data.json')

testdata = open('Dashboardtestdata.json')

doc = json.load(testdata)

resp = client.index(index="blueskydata", id="1", document=doc)
print(resp['result'])
