import yaml


def dump(rdf_mapping, dataset_id='dataset'):
    sources = {'dataset-source': [f'{dataset_id}.json~jsonpath', '$.[*].fields']}
    mappings = {}
    for template in rdf_mapping.templates():
        template_classes = rdf_mapping.classes(template)
        mapping_name = get_suffix(list(template_classes)[0])
        predicateobjects = []
        for predicate, object in rdf_mapping.properties_objects(template):
            predicateobjects.append([predicate, object])
        mapping = {'subject': template,
                   'predicateobjects': predicateobjects,
                   'source': 'dataset-source'}
        mappings[mapping_name] = mapping
    yarrrml_mapping = {'mappings': mappings,
                       'sources': sources}
    return yaml.safe_dump(yarrrml_mapping, default_flow_style=None)


def get_suffix(uri):
    return uri.split('/')[-1].split('#')[-1]
