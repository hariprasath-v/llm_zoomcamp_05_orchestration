#iterative retrieval
from typing import Dict, List, Union

import numpy as np
from elasticsearch import Elasticsearch, exceptions


SAMPLE_QUERY = "When is the next cohort?"


@data_loader
def search(*args, **kwargs) -> List[Dict]:
    
    
    
    connection_string = kwargs.get('connection_string', 'http://localhost:9200')
    index_name = kwargs.get('index_name', 'documents')
    
    

    script_query = {
        "size": 1,
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query":SAMPLE_QUERY,
                        "fields": ["question^5", "text", "section"],
                        "type": "best_fields"
                    }
                }
            }
        }
    }


    

    
    print("Sending script query:", script_query)

    es_client = Elasticsearch(connection_string)
    
    try:
        response = es_client.search(
            index=index_name,
            body= script_query,
                
            
        )

        print("Raw response from Elasticsearch:", response)

        return [hit['_source'] for hit in response['hits']['hits']]
    
    except exceptions.BadRequestError as e:
        print(f"BadRequestError: {e.info}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []
