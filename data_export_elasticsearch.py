#Data export
import json
from typing import Dict, List, Tuple, Union

import numpy as np
from elasticsearch import Elasticsearch
from datetime import datetime
from mage_ai.data_preparation.variable_manager import set_global_variable

@data_exporter
def elasticsearch(documents, *args, **kwargs):
    connection_string = kwargs.get('connection_string', 'http://localhost:9200')
    index_name_prefix = kwargs.get('index_name', 'documents')
    current_time = datetime.now().strftime("%Y%m%d_%M%S")
    index_name = f"{index_name_prefix}_{current_time}"
    print("index name:", index_name)
    #change the name according to your pipeline's name 
    set_global_variable('ominous_maelstrom', 'index_name', index_name)
    number_of_shards = kwargs.get('number_of_shards', 1)
    number_of_replicas = kwargs.get('number_of_replicas', 0)
    dimensions = kwargs.get('dimensions')

    
    es_client = Elasticsearch(connection_string)

    print(f'Connecting to Elasticsearch at {connection_string}')

    index_settings = {
    "settings": {
        "number_of_shards": number_of_shards,
        "number_of_replicas": number_of_replicas
    },
    "mappings": {
        "properties": {
            "text": {"type": "text"},
            "section": {"type": "text"},
            "question": {"type": "text"},
            "course": {"type": "keyword"},
            "document_id": {"type": "keyword"}
        }
    }
}
    # Recreate the index by deleting if it exists and then creating with new settings
    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)
        print(f'Index {index_name} deleted')

    es_client.indices.create(index=index_name, body=index_settings)
    print('Index created with properties:')
    print(json.dumps(index_settings, indent=2))
    

    count = len(documents)
    print(f'Indexing {count} documents to Elasticsearch index {index_name}')
    last_document = None
    for idx, document in enumerate(documents):
        if idx % 100 == 0:
		        print(f'{idx + 1}/{count}')

        es_client.index(index=index_name, document=document)
        last_document = document
    if last_document:
        print('Last document indexed:')
        print(json.dumps(last_document, indent=2))
