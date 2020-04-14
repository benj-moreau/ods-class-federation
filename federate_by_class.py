import argparse
import os
import logging

from ods.class_federation import federate_datasets

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def main(domain_id, clas, api_key, output_file):
    if is_json_output(output_file):
        federate_datasets(domain_id, clas, api_key, output_file, format='json')
    else:
        federate_datasets(domain_id, clas, api_key, output_file, format='csv')


def is_json_output(output_file):
    _, file_extension = os.path.splitext(output_file.name)
    if file_extension == '.csv':
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A tool that federate ods datasets by a specific class.')
    parser.add_argument('-o', '-output', type=argparse.FileType('w'), help='csv file that will contain the federation',
                        required=True)
    parser.add_argument('-c', '-class', type=str, help='Class to filter datasets (as shown in class filter)',
                        required=True)
    parser.add_argument('-d', '-domain_id', type=str, help='The domain-id of the domain', required=True)
    parser.add_argument('--a', '--api_key', type=str, help='An api-key for the domain', default=None, required=False)
    args = parser.parse_args()
    main(args.d, args.c, args.a, args.o)
