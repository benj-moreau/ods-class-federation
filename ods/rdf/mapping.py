from rdflib import RDF, Graph, Literal, URIRef
import re

from ods.rdf.utils import yarrrml_parser, yarrrml_serializer

# to find references ex: $(field_name)
REGEX_REFERENCE_FIELD = re.compile('\$\((.*?)\)')


url_regex = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
    r'localhost|' #localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


class RDFMapping:
    def __init__(self, yarrrml_mapping=None):
        if yarrrml_mapping:
            self.graph = yarrrml_parser.get_rdf_mapping(yarrrml_mapping)
        else:
            self.graph = Graph()

    def __repr__(self):
        return self.graph.__repr__()

    def __str__(self):
        return self.graph.__str__()

    def __len__(self):
        return len(self.graph)

    @property
    def rdf_graph(self):
        return self.graph

    def triples(self):
        for s, p, o in self.graph.triples((None, None, None)):
            yield (str(s), str(p), str(o))

    def classes(self, subject=None):
        classes = set()
        if subject:
            subject = URIRef(subject)
        for _, _, o in self.graph.triples((subject, RDF.type, None)):
            classes.add(str(o))
        return classes

    def search_classes(self, string):
        for uri in self.classes():
            if string.lower() == get_suffix(uri).lower():
                yield uri

    def templates(self, class_uri=None):
        templates = set()
        if class_uri:
            for s, _, _ in self.graph.triples((None, RDF.type, URIRef(class_uri))):
                templates.add(str(s))
        else:
            for s, _, _ in self.graph.triples((None, RDF.type, None)):
                templates.add(str(s))
        return templates

    def properties(self, template=None):
        properties = set()
        if template:
            for _, p, _ in self.graph.triples((URIRef(template), None, None)):
                properties.add(str(p))
        else:
            for _, p, _ in self.graph.triples((None, None, None)):
                properties.add(str(p))
        return properties

    def properties_objects(self, template=None):
        if template:
            for _, p, o in self.graph.triples((URIRef(template), None, None)):
                yield str(p), str(o)
        else:
            for _, p, o in self.graph.triples((None, None, None)):
                yield str(p), str(o)

    def add(self, subject, predicate, object):
        if is_valid_uri(object):
            self.graph.add((URIRef(subject), URIRef(predicate), URIRef(object)))
        else:
            self.graph.add((URIRef(subject), URIRef(predicate), Literal(object)))

    def serialize(self, format='yaml', dataset_id=None):
        """‘xml’, ‘n3’, ‘turtle’, ‘nt’, ‘pretty-xml’, ‘trix’, ‘trig’ and ‘nquads’ formats are built in"""
        if format in ['yaml', 'yml', 'yarrrml']:
            return yarrrml_serializer.dump(self, dataset_id)
        else:
            return str(self.graph.serialize(format=format), 'utf-8')


def get_suffix(uri):
    return uri.split('/')[-1].split('#')[-1]


def get_fields(term):
    serialized_term = str(term)
    matched_fields = REGEX_REFERENCE_FIELD.findall(serialized_term)
    fields = []
    for field in matched_fields:
        fields.append(field)
    return fields


def is_valid_uri(uri):
    return re.match(url_regex, uri) is not None
