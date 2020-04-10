import yaml
from rdflib import RDF

from ods.api.iterators import CatalogIterator
from ods.rdf.mapping import RDFMapping, get_fields, get_suffix


def federate_datasets(domain_id, clas, api_key):
    filtered_mappings = _filtered_mappings(domain_id, clas, api_key)


def _filtered_mappings(domain_id, clas, api_key):
    catalog_iterator = CatalogIterator(domain_id=domain_id, where=f'semantic.classes:"{clas}"', api_key=api_key)
    filtered_mappings = {}
    for dataset in catalog_iterator:
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


def _fields_to_str(fields):
    return ' '.join(fields)
