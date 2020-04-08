from rdflib import RDF

from ods.rdf.parser import yarrrml_parser


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


def get_suffix(uri):
    return uri.split('/')[-1].split('#')[-1]
