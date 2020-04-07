from ods.api.catalog import search_v2


class CatalogIterator:
    def __init__(self, domain_id, where='', search='', refine='', exclude='', rows=10, start=0, sort='explore.popularity_score',
                 api_key=None):
        self.domain_id = domain_id
        self.where = where
        self.search = search
        self.refine = refine
        self.exclude = exclude
        self.rows = rows
        self.start = start
        self.sort = sort
        self.api_key = api_key
        self.cpt = 0
        self.result = search_v2(domain_id, where, search, refine, exclude, rows, start, sort, api_key)
        self.nb_query = 1

    def __len__(self):
        return self.result['total_count']

    def __iter__(self):
        return self

    def __next__(self):
        if self.cpt <= len(self):
            if len(self.result['datasets']) > 0:
                self.cpt += 1
                return self.result['datasets'].pop(0)
            else:
                self.result = search_v2(self.domain_id, self.where, self.search, self.refine, self.exclude, self.rows,
                                        self.start + (self.nb_query * self.rows), self.sort, self.api_key)
                self.nb_query += 1
                if len(self.result['datasets']) > 0:
                    return self.__next__()
        raise StopIteration()

