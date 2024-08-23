# custom_iceberg_extractor.py
from pyiceberg.catalog import load_catalog
from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import TableMetadata
from pynessie import NessieClient

class IcebergNessieExtractor(Extractor):
    """
    Extractor to pull metadata from Apache Iceberg tables in a Nessie catalog.
    """
    def init(self, conf):
        self._conf = conf
        self._client = NessieClient(conf['nessie_url'], conf['nessie_branch'])
        self._catalog = load_catalog(conf['catalog_name'])
        self._iter = iter(self._catalog.list_tables())

    def extract(self):
        try:
            table = next(self._iter)
            table_metadata = self._catalog.load_table(table)
            
            return TableMetadata(
                database=table_metadata.identifier.namespace,
                cluster='gold',
                schema=table_metadata.schema.name,
                name=table_metadata.identifier.name,
                description='',
                columns=[column.name for column in table_metadata.schema.columns]
            )
        except StopIteration:
            return None

    def get_scope(self):
        return 'extractor.iceberg_nessie'


# from databuilder.extractor.base_extractor import Extractor
# from databuilder.models.table_metadata import TableMetadata, ColumnMetadata

# class NessieMetadataExtractor(Extractor):
#     def __init__(self, tables):
#         self.tables = tables
#         self.index = 0

#     def init(self, conf):
#         pass

#     def extract(self):
#         if self.index < len(self.tables):
#             table = self.tables[self.index]
#             self.index += 1
#             return table
#         else:
#             return None
