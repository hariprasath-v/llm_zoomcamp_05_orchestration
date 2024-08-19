#Data chunking

import re
from typing import Any, Dict, List
import hashlib


@transformer
def chunk_documents(data: List[Dict[str, Any]], *args, **kwargs):
    # Generate a unique document ID
    def generate_document_id(doc):
        combined = f"{doc['course']}-{doc['question']}-{doc['text'][:10]}"
        hash_object = hashlib.md5(combined.encode())
        hash_hex = hash_object.hexdigest()
        document_id = hash_hex[:8]
        return document_id
    documents = []
    for course_dict in data:
        for doc in course_dict['documents']:
            doc['course'] = course_dict['course']
            #print(doc)
            #break
            # previously we used just "id" for document ID
            doc['document_id'] = generate_document_id(doc)
            documents.append(doc)
    print(f'Documents:', len(documents))
    return [documents]
