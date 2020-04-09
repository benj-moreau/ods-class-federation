from rdflib import RDF, URIRef
import re

from ods.rdf.parser import yarrrml_parser

# to find references ex: $(field_name)
REGEX_REFERENCES = re.compile('(\$\(.*?\))')
REGEX_UNREFERENCES = re.compile('\$\(.*?\)')
REGEX_REFERENCE_FIELD = re.compile('\$\((.*?)\)')


class RDFMapping:
    def __init__(self, yarrrml_mapping):
        self.graph = yarrrml_parser.get_rdf_mapping(yarrrml_mapping)

    @property
    def rdf_graph(self):
        return self.graph()

    def get_classes_uri(self):
        for _, _, o in self.graph.triples((None, RDF.type, None)):
            yield o

    def search_classes(self, string):
        for uri in self.get_classes_uri():
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


def get_suffix(uri):
    return uri.split('/')[-1].split('#')[-1]


def get_fields(term):
    serialized_term = str(term)
    matched_fields = REGEX_REFERENCE_FIELD.findall(serialized_term)
    fields = []
    for field in matched_fields:
        fields.append(field)
    return fields
