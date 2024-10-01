import logging
import sys
import os
import uuid
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from databuilder.extractor.druid_metadata_extractor import DruidMetadataExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.extractor.csv_extractor import CsvTableLineageExtractor
from databuilder.extractor import sql_alchemy_extractor

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

# Configure Elasticsearch and Neo4j hosts
es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', 'localhost')
es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)
neo_host = os.getenv('CREDENTIALS_NEO4J_PROXY_HOST', 'localhost')
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)

# Command-line arguments for overriding host values
if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    neo_host = sys.argv[2]

# Initialize Elasticsearch client
es = Elasticsearch([
    {'host': es_host, 'port': es_port},
])

# Druid connection and configuration
druid_broker_host = "10.100.2.48"
where_clause_suffix = ""
conn_string = "druid+http://{host}:8888/druid/v2/sql/".format(host=druid_broker_host)

# Job configuration
job_config = ConfigFactory.from_dict({
    'extractor.druid_metadata.{}'.format(DruidMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY): where_clause_suffix,
    'extractor.druid_metadata.extractor.sqlalchemy.{}'.format(sql_alchemy_extractor.SQLAlchemyExtractor.CONN_STRING): conn_string,
    'loader.filesystem_csv_neo4j.node_dir_path': '/var/tmp/amundsen/neo4j/nodes',
    'loader.filesystem_csv_neo4j.relationship_dir_path': '/var/tmp/amundsen/neo4j/relations',
    'publisher.neo4j.neo4j_endpoint': f'bolt://{neo_host}:{neo_port}',
    'publisher.neo4j.neo4j_user': '',  # Add your Neo4j username
    'publisher.neo4j.neo4j_password': '',  # Add your Neo4j password
    'publisher.neo4j.node_files_directory': '/var/tmp/amundsen/neo4j/nodes',
    'publisher.neo4j.relation_files_directory': '/var/tmp/amundsen/neo4j/relations',
    'publisher.neo4j.job_publish_tag': 'druid_metadata_job',
    'extractor.search_data.entity_type': 'table',
    'extractor.search_data.extractor.neo4j.graph_url': f'bolt://{neo_host}:{neo_port}',
    'extractor.search_data.extractor.neo4j.model_class': 'databuilder.models.table_elasticsearch_document.TableESDocument',
    'extractor.search_data.extractor.neo4j.neo4j_auth_user': '',  # Add your Neo4j username
    'extractor.search_data.extractor.neo4j.neo4j_auth_pw': '',  # Add your Neo4j password
    'extractor.search_data.extractor.neo4j.neo4j_encrypted': False,
    'loader.filesystem.elasticsearch.file_path': '/var/tmp/amundsen/search_data.json',
    'loader.filesystem.elasticsearch.mode': 'w',
    'publisher.elasticsearch.file_path': '/var/tmp/amundsen/search_data.json',
    'publisher.elasticsearch.mode': 'r',
    'publisher.elasticsearch.client': es,
    'publisher.elasticsearch.new_index': f'table_{uuid.uuid4()}',
    'publisher.elasticsearch.doc_type': 'table',
    'publisher.elasticsearch.alias': 'table_search_index',
})

# Function to run table lineage job from a CSV file
def run_table_lineage_job(file_loc, job_name):
    tmp_folder = f'/var/tmp/amundsen/{job_name}'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    csv_extractor = CsvTableLineageExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=csv_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        'extractor.csvtablelineage.table_lineage_file_location': file_loc,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': f'bolt://{neo_host}:{neo_port}',
        'publisher.neo4j.neo4j_user': '',  # Add your Neo4j username
        'publisher.neo4j.neo4j_password': '',  # Add your Neo4j password
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_tag_lineage',  # Use a unique tag for the job
    })

    job = DefaultJob(conf=job_config, task=task, publisher=Neo4jCsvPublisher())
    job.launch()


logger.info(f"Job configuration: {job_config}")
logger.info("Creating job and task...")

# Create the task for Neo4j
task = DefaultTask(
    extractor=DruidMetadataExtractor(),
    loader=FsNeo4jCSVLoader(),
    transformer=NoopTransformer()
)

# Create the job for Neo4j
job_neo4j = DefaultJob(conf=job_config, task=task, publisher=Neo4jCsvPublisher())

logger.info("Launching the Neo4j job...")
job_neo4j.launch()
logger.info("Neo4j job completed.")


# Run the table lineage job
run_table_lineage_job('./sample_lineage.csv', 'table_lineage_job')


# Create the task for Elasticsearch
es_task = DefaultTask(
    extractor=Neo4jSearchDataExtractor(),
    loader=FSElasticsearchJSONLoader(),
    transformer=NoopTransformer()
)

# Create the job for Elasticsearch
job_es = DefaultJob(conf=job_config, task=es_task, publisher=ElasticsearchPublisher())

logger.info("Launching the Elasticsearch job...")
job_es.launch()
logger.info("Elasticsearch job completed.")
