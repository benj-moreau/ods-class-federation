import requests

import utils.requester as requester

DATA_RECORD_API_V2_URL = "https://data.opendatasoft.com/api/v2/catalog/datasets/{}/records"


class DatasetIdMissing(Exception):
    pass


def records_v2(dataset_id, rows=10, api_key=None):
    if dataset_id:
        params = {'rows': rows, 'apikey': api_key}
        request = requests.get(DATA_RECORD_API_V2_URL.format(dataset_id),
                               params,
                               timeout=requester.get_timeout(),
                               headers=requester.create_ods_headers())
        request.raise_for_status()
        return request.json()
    else:
        raise DatasetIdMissing
