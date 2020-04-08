import argparse
from ods.api.iterators import CatalogIterator


def main(domain_id, clas, api_key):
    catalog_iterator = CatalogIterator(domain_id=domain_id, where=f'semantic.classes:"{clas}"', api_key=api_key)
    for dataset in catalog_iterator:
        print(dataset.rml_mapping)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A tool that federate ods datasets by a specific class.')
    parser.add_argument('-c', '-class', type=str, help='Class to filter datasets (as shown in class filter)', required=True)
    parser.add_argument('-d', '-domain_id', type=str, help='The domain-id of the domain', required=True)
    parser.add_argument('--a', '--api_key', type=str, help='An api-key for the domain', default=None, required=False)
    args = parser.parse_args()
    main(args.d, args.c, args.a)
