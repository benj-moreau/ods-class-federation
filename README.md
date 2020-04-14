# ods class federation

Linked Data (LD) is a set of best practices to publish data in
RDF format.
One advantage of LD is the interoperability between datasets.
Opendatasoft (ods) platform can transform structured datasets into RDF datasets
thanks to RDF Mappings.
With this script, we propose a solution to the following question:
**How to federate a set of datasets using only their RDF mappings**.
With this script, we propose to federate a set of datasets (from ods) by RDF classes.
For example, generates a dataset that contain all resources of type Plant from a set of datasets.

## Installation

You need Python 3.x and pip installed. [How to install python 3.x with pip?](https://realpython.com/installing-python/)

Installation in a [virtualenv](https://docs.python-guide.org/dev/virtualenvs/) is advised but not mandatory.

Before executing the script, you need to install some dependencies (requirements.txt) using the following command:

```bash
pip install -r requirements.txt
```

## Running the script

You can generate a federated dataset by querying an ods domain by class.
This can be done by executing the following command in the root folder:

```bash
python federate_by_class.py [-h] -o output_file -c class -d domain_id [--a api_key]
```

usage:
```
required arguments:
  -o O, -output O     json/csv file that will contain the federated dataset
  -c C, -class C      Class to filter datasets (as shown in class filter)
  -d D, -domain_id D  The domain-id of the domain
optional arguments:
  -h, --help          show this help message and exit
  --a A, --api_key A  An api-key for the domain
```

Notice that if the `api_key` is not provided, domain is queried as anonymous user.

To choose the format of the federated dataset, `output` file extension should be `.csv` or `.json`.
JSON format is strongly advised.

For example, the following command retrieves all entities of type `Person` on the `data` domain:

```bash
python federate_by_class.py -o dataset.json -c Person -d data
```

Or, if you have an `api_key`:

```bash
python federate_by_class.py -o dataset.json -c Person -d data --a ThisIsaFakeApiKey
```

## Result

After executing the script, a `.json`|`.csv` file containing the federated dataset is generated along with a `.rml.yml`.
This yml file contains an RDF mapping for the federated dataset.