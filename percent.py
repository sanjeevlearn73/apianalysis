#!/usr/local//bin/python3

import os
import json
import requests
import time
import csv

def api_call(url):
    time.sleep(3)
    path = url
    # Construct your Elasticsearch query with a bool query that matches the URI path and uri_path_filter
    json_data = {
        'size': 0,
             'query': {
                   'bool': {
                        'must': [
                            { 
                                'match': {
                                     'uri_path': path,
                                },
                            },
                         ],
                    },
                },
    'aggs': {
        'requests_per_minute': {
            'date_histogram': {
                'field': '@timestamp',
                'interval': '1m',
            },
            'aggs': {
                'requests_by_ip': {
                    'terms': {
                        'field': 'cf_connecting_ip',
                    },
                    'aggs': {
                        'uri_path_filter': {
                            'filter': {
                                'term': {
                                    'uri_path': path,
                                },
                            },
                        },
                    },
                },
                'max_requests': {
                    'max_bucket': {
                        'buckets_path': 'requests_by_ip>uri_path_filter>_count',
                    },
                },
            },
        },
        'percentile_max_requests': {
            'percentiles_bucket': {
                'buckets_path': 'requests_per_minute>max_requests',
                'percents': [
                     99
                ],
            },
        },
    },
}
# Query ElasticSearch/Kibana API's for aggregated data
    headers = {'Accept': 'application/json','Content-Type': 'application/json'}
    response = requests.post('http://x.x.x.x:9200/hapx-l1-*/_search?pretty', headers=headers, json=json_data)
    response_data = response.json()
    print('path:',path)
    buckets = response_data['aggregations']['percentile_max_requests']['values']
    print('Path and 99% percentile RPM:', path, buckets['99.0'])

# Execute your Elasticsearch query using the curl module
# add uri_path filter to query
#    uri_path_filter = {"match": {"uri_path": path}}
#    query["query"]["bool"]["must"].append(uri_path_filter)
#print(json_data)
#print(response.text)
#
#print('Path: {} and 99% percentile RPM: {}'.format(path, buckets['99.0']))
#print('Path:', path)
#if response.status_code == 200:
#    response_data = response.json()
#    if "aggregations" not in response_data:
#        print(f"No aggregations found in the response for URL: {path}")
#        buckets = response_data['aggregations']['percentile_max_requests']['values']
#        print('Path and 99% percentile RPM:', path, buckets['99.0'])
#    print(json.dumps(response_data, indent=4))
#    else:
#        print(f"Request failed with status code {response.status_code}")

# process the aggregations
#buckets = response_data["aggregations"]["requests_by_ip"]["buckets"]
#buckets = response_data["aggregations"]['percentile_max_requests']['values']

# File which contains API URL's internal/external for reading via the script
with open(apiurls.txt', 'r') as f:
    api_urls = f.read().splitlines()

for url in api_urls:
    api_call(url)
