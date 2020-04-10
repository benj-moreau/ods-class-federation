import csv
import yaml
import logging
from rdflib import RDF

from ods.api.iterators import CatalogIterator, DatasetIterator
from ods.rdf.mapping import RDFMapping, get_fields, get_suffix


def federate_datasets(domain_id, clas, api_key, output_file):
    filtered_mappings = _filtered_mappings(domain_id, clas, api_key)
    # schema of the federation (set of fields)
    federated_fields = _get_federation_fields(filtered_mappings, clas)
    with output_file as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=federated_fields)
        writer.writeheader()
        # Now we retrieve data from datasets
        for dataset_id, templates in filtered_mappings.items():
            # rows=100 to reduce http calls
            dataset_iterator = DatasetIterator(domain_id=domain_id, dataset_id=dataset_id, rows=100)
            for i, record in enumerate(dataset_iterator, start=1):
                if i % 50 == 0:
                    logging.info(f'Processed {i}/{len(dataset_iterator)} records in {dataset_id}.')
                for template, properties in templates.items():
                    row = {clas: record.value(template)}
                    for federate_field, field_name in properties.items():
                        row[federate_field] = record.value(field_name)
                    writer.writerow(row)


def _filtered_mappings(domain_id, clas, api_key):
    catalog_iterator = CatalogIterator(domain_id=domain_id, where=f'semantic.classes:"{clas}"', api_key=api_key)
    logging.info(f'Found {len(catalog_iterator)} datasets containing class {clas}.')
    filtered_mappings = {}
    for dataset in catalog_iterator:
        logging.info(f'{dataset.dataset_id}')
        filtered_mapping = {}
        rml_mapping = yaml.safe_load(dataset.rml_mapping)
        rdf_mapping = RDFMapping(rml_mapping)
        templates = set()
        for class_uri in rdf_mapping.search_classes(clas):
            templates = templates.union(rdf_mapping.templates(class_uri=class_uri))
        for template in templates:
            # for templates of rdf:type clas
            fields = _fields_to_str(get_fields(template))
            template_mapping = filtered_mapping.get(fields, {})
            # we retrieve suffix of their properties (column names)
            # and fields in the objects
            for property, object in rdf_mapping.properties_objects(template):
                if not property == str(RDF.type):
                    template_mapping[get_suffix(property)] = _fields_to_str(get_fields(object))
            filtered_mapping[fields] = template_mapping
        filtered_mappings[dataset.dataset_id] = filtered_mapping
    return filtered_mappings


def _get_federation_fields(filtered_mappings, clas):
    federation_fields = set()
    federation_fields.add(clas)
    for dataset_id, templates in filtered_mappings.items():
        for template_field, properties in templates.items():
            for federation_field in properties:
                federation_fields.add(federation_field)
    return federation_fields



def _fields_to_str(fields):
    return ' '.join(fields)
