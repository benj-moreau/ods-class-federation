import requests

import utils.requester as requester

DATA_CATALOG_API_URL = "https://data.opendatasoft.com/api/datasets/1.0/{}/"
DATA_CATALOG_API_SEARCH_V2_URL = "https://{domain_id}.opendatasoft.com/api/v2/catalog/datasets/"


class DatasetIdMissing(Exception):
    pass


def dataset_meta_request(dataset_id, api_key=None):
    if dataset_id:
        params = {'apikey': api_key}
        request = requests.get(DATA_CATALOG_API_URL.format(dataset_id),
                               params, timeout=requester.get_timeout(),
                               headers=requester.create_ods_headers())
        request.raise_for_status()
        return request.json()
    else:
        raise DatasetIdMissing


def search_v2(domain_id, where='', search='', refine='', exclude='', rows=10, start=0, sort='explore.popularity_score',
              api_key=None):
    params = {'where': where,
              'search': search,
              'refine': refine,
              'exclude': exclude,
              'rows': rows,
              'start': start,
              'sort': sort,
              'apikey': api_key}
    request = requests.get(f"https://{domain_id}.opendatasoft.com/api/v2/catalog/datasets/",
                           params,
                           timeout=requester.get_timeout(),
                           headers=requester.create_ods_headers())
    request.raise_for_status()
    return request.json()
