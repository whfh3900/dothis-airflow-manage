import boto3
from opensearchpy  import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import time
import json
import os
from dotenv import load_dotenv



def opensearch_client():
    load_dotenv()
    credentials = boto3.Session().get_credentials()

    region = "ap-northeast-2"
    service = 'es'
    # auth = AWSV4SignerAuth(credentials, region, service)
    # os_host = 'search-dothis-js7jgo2actyuzihx7zz335k2nq.ap-northeast-2.es.amazonaws.com'

    # client = OpenSearch(
    #         hosts=[{'host': os_host, 'port': 443}],
    #         http_auth=auth,
    #         use_ssl=True,
    #         verify_certs=False,
    #         connection_class=RequestsHttpConnection,
    #         pool_maxsize=20
    #     )

    host = os.getenv('OPENSEARCH_HOST') 
    port = int(os.getenv('OPENSEARCH_PORT')) 
    user = os.getenv('OPENSEARCH_USER') 
    password = os.getenv('OPENSEARCH_PW') 

    # OpenSearch 클러스터에 연결
    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_auth=(user, password),
        use_ssl=True,
        verify_certs=False,  # SSL 인증서 검증 비활성화
        time_out=360
    )
    
    # 클러스터 상태 조회
    try:
        health = client.cluster.health()
        print("Cluster health:", health)
    except Exception as e:
        print("Error connecting to OpenSearch:", e)    

    return client

def make_url_list_from_os(client, index_name : str) -> list:
    result_list = []
    scroll_id = None
    try:
        start_time = time.time()
        search_query = {
        "size": 10000,
        "query": {
            "match_all": {}
            },
            "_source":["video_url", "channel_id"]
        }

        scroll_time = '30m'

        response = client.search(
            index = index_name,
            scroll = scroll_time,
            body = search_query
        )

        scroll_id = response['_scroll_id']
        total_len = response["hits"]["total"]["value"]
        print(total_len)

        hits = response['hits']['hits']
        
        for result in hits:
            result_list.append(result)

        while hits:
            # scroll next page
            response = client.scroll(
                scroll_id=scroll_id,
                scroll=scroll_time
            )
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

            for result in hits:
                result_list.append(result)


    except Exception as e:
        return

    finally:
        # 스크롤 컨텍스트 정리
        if scroll_id is not None:
            client.clear_scroll(scroll_id=scroll_id)

    return result_list
