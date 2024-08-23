# run_data_builder_job.py
from pyhocon import ConfigFactory
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

from custom_iceberg_extractor import IcebergNessieExtractor

config = ConfigFactory.from_dict({
    'extractor.iceberg_nessie.nessie_url': 'http://localhost:19120',
    'extractor.iceberg_nessie.nessie_branch': 'main',
    'extractor.iceberg_nessie.catalog_name': 'my_catalog',
    'loader.filesystem_csv_neo4j.node_dir_path': '/var/tmp/amundsen/neo4j/nodes',
    'loader.filesystem_csv_neo4j.relation_dir_path': '/var/tmp/amundsen/neo4j/relations',
    'publisher.neo4j.neo4j_end_point_key': 'bolt://localhost:7687',
    'publisher.neo4j.neo4j_user': 'neo4j',
    'publisher.neo4j.neo4j_password': 'test',
})

task = DefaultTask(extractor=IcebergNessieExtractor(), loader=FsNeo4jCSVLoader(), transformer=NoopTransformer())

job = DefaultJob(conf=config, task=task, publisher=Neo4jCsvPublisher())
job.launch()
