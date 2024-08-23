
import logging
import sys
import os
import uuid
import pynessie
import pandas as pd
import boto3
import json
from urllib.parse import urlparse
import pyarrow.fs as fs
from pynessie.model import ContentKey
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.extractor import sql_alchemy_extractor
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask

from databuilder.extractor.base_extractor import Extractor


# Custom extractor for Nessie metadata
class NessieMetadataExtractor(Extractor):
    def __init__(self, tables):
        self.tables = tables
        self.index = 0

    def init(self, conf):
        pass

    def extract(self):
        if self.index < len(self.tables):
            table = self.tables[self.index]
            self.index += 1
            return table
        else:
            return None
        


nessie_client = pynessie.init() # this will look for the client config as per above
# nessie_client._base_url = "http://10.100.2.73:19120"

# Set the branch you want to work with
branch_name = "main"  # Change to your branch name

branch_keys = nessie_client.list_keys("main")




iceberg_tables = [entry for entry in branch_keys.entries if entry.kind == 'ICEBERG_TABLE']

metadata = []

for table in iceberg_tables:
    # Construct the content key from the elements
    table_path = ".".join(table.name.elements)
    content_key = ContentKey.from_path_string(table_path)
    
    # Retrieve the content using the constructed content_key
    content = nessie_client.get_content("main", content_key)
    

    metadata.append({
        "table_name": table_path,
        "metadata_location": content.metadata_location if hasattr(content, 'metadata_location') else None,
        "snapshot_id": content.snapshot_id if hasattr(content, 'snapshot_id') else None,
        "schema_id": content.schema_id if hasattr(content, 'schema_id') else None,
        "partition_spec_id": content.partition_spec_id if hasattr(content, 'partition_spec_id') else None
    })

    # print(f'Content for {table_path}: {content}')

print(metadata)


# # Convert metadata to a DataFrame for easier manipulation
df_metadata = pd.DataFrame(metadata)
print(df_metadata)




# Function to fetch schema from HDFS
def fetch_schema_from_hdfs(metadata_location):
    hdfs = fs.HadoopFileSystem('namenode', port=9002)
    with hdfs.open_input_file(metadata_location) as f:
        metadata_json = json.load(f)
    return metadata_json

# Function to fetch schema from S3
def fetch_schema_from_minio(metadata_location):
    parsed_url = urlparse(metadata_location)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')
    
    session = boto3.Session(
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    s3 = session.client(
        's3',
        endpoint_url="http://197.253.123.37:9000",
        use_ssl=False
    )
    response = s3.get_object(Bucket=bucket, Key=key)
    metadata_json = json.loads(response['Body'].read().decode('utf-8'))
    return metadata_json


# Function to fetch schema based on the URL schema
def fetch_schema(metadata_location):
    if metadata_location.startswith("hdfs://"):
        metadata_json = fetch_schema_from_hdfs(metadata_location)
    elif metadata_location.startswith("s3a://"):
        metadata_json = fetch_schema_from_minio(metadata_location)
    else:
        raise ValueError(f"Unsupported URL schema in metadata location: {metadata_location}")

    # Extract schema details
    schema = metadata_json["schemas"][0]["fields"]
    
    # Transform schema into Amundsen's ColumnMetadata format
    columns = [ColumnMetadata(name=field["name"], description="", col_type=field["type"], sort_order=index) 
               for index, field in enumerate(schema)]
    return columns

# Fetch schema information for each table
df_metadata['columns'] = df_metadata['metadata_location'].apply(fetch_schema)
print(df_metadata)


# Transform metadata
def transform_to_amundsen_format(df_metadata):
    tables = []
    for _, row in df_metadata.iterrows():
        table_name = row["table_name"]
        columns = row["columns"]
        table_metadata = TableMetadata(
            database="iceberg",  # Adjust according to your setup
            cluster="gold",      # Adjust according to your setup
            schema="nba",        # Adjust according to your setup
            name=table_name,
            description="Description of {}".format(table_name),
            columns=columns
        )
        tables.append(table_metadata)
    return tables

amundsen_tables = transform_to_amundsen_format(df_metadata)

print(amundsen_tables)


# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

# Elasticsearch and Neo4j configurations
es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', 'localhost')
es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)
neo_host = os.getenv('CREDENTIALS_NEO4J_PROXY_HOST', 'localhost')
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)


if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    neo_host = sys.argv[2]

# Initialize Elasticsearch client
es = Elasticsearch([
    {'host': es_host, 'port': es_port},
])

# Job config
job_config = ConfigFactory.from_dict({
    'loader.filesystem_csv_neo4j.node_dir_path': '/var/tmp/amundsen/neo4j/nodes',
    'loader.filesystem_csv_neo4j.relationship_dir_path': '/var/tmp/amundsen/neo4j/relations',
    'publisher.neo4j.neo4j_endpoint': f'bolt://{neo_host}:{neo_port}',
    'publisher.neo4j.neo4j_user': '',
    'publisher.neo4j.neo4j_password': '',
    'publisher.neo4j.node_files_directory': '/var/tmp/amundsen/neo4j/nodes',
    'publisher.neo4j.relation_files_directory': '/var/tmp/amundsen/neo4j/relations',
    'publisher.neo4j.job_publish_tag': 'nessie_metadata_job',
    'extractor.search_data.entity_type': 'table',
    'extractor.search_data.extractor.neo4j.graph_url': f'bolt://{neo_host}:{neo_port}',
    'extractor.search_data.extractor.neo4j.model_class': 'databuilder.models.table_elasticsearch_document.TableESDocument',
    'extractor.search_data.extractor.neo4j.neo4j_auth_user': '',
    'extractor.search_data.extractor.neo4j.neo4j_auth_pw': '',
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

logger.info(f"Job configuration: {job_config}")
logger.info("Creating job and task...")

extractor = NessieMetadataExtractor(tables=amundsen_tables)
# Neo4j task
task = DefaultTask(
    extractor=extractor,
    loader=FsNeo4jCSVLoader(),
    transformer=NoopTransformer()
)

# Neo4j job
job_neo4j = DefaultJob(conf=job_config, task=task, publisher=Neo4jCsvPublisher())
logger.info("Launching the Neo4j job...")

job_neo4j.launch()
logger.info("Neo4j job completed.")

# Create the task for Elasticsearch
es_task = DefaultTask(
    extractor=Neo4jSearchDataExtractor(),
    loader=FSElasticsearchJSONLoader(),
    transformer=NoopTransformer()
)

# Create the job for Elasticsearch
# job_es = DefaultJob(conf=job_config, task=es_task, publisher=ElasticsearchPublisher())

# logger.info("Launching the Elasticsearch job...")
# job_es.launch()
# logger.info("Elasticsearch job completed.")
