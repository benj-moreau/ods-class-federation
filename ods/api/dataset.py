import requests

import utils.requester as requester


class DatasetIdMissing(Exception):
    pass


def records_v2(domain_id, dataset_id, rows=10, api_key=None):
    params = {'rows': rows, 'apikey': api_key}
    request = requests.get(f'https://{domain_id}.opendatasoft.com/api/v2/catalog/datasets/{dataset_id}/records',
                           params,
                           timeout=requester.get_timeout(),
                           headers=requester.create_ods_headers())
    request.raise_for_status()
    return request.json()
