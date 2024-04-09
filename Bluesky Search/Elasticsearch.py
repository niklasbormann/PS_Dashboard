import json

import jsonify
from elasticsearch import Elasticsearch

client = Elasticsearch(hosts='https://localhost:9200',
                       basic_auth=("elastic", "GML0OBa-2=sjGhskAf*_"),
                       verify_certs=True,
                       ca_certs=r'C:\Users\Nikla\Downloads\elasticsearch-8.13.1-windows-x86_64\elasticsearch-8.13.1\config\certs\http_ca.crt'
                       )
searchresult = client.search(index='blueskydata', q="Ukraine", filter_path=['hits.hits._source.Text', 'hits.hits._source.Lang'])
print(searchresult.body)