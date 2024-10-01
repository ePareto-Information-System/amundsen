from databuilder.extractor.druid_metadata_extractor import DruidMetadataExtractor
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
from pyhocon import ConfigFactory
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.extractor import sql_alchemy_extractor
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.transformer.base_transformer import NoopTransformer

import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

druid_broker_host = "10.100.2.48"
where_clause_suffix = ""

conn_string = "druid+http://{host}:8888/druid/v2/sql/".format(host=druid_broker_host)

job_config = ConfigFactory.from_dict({
    'extractor.druid_metadata.{}'.format(DruidMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY): where_clause_suffix,
    'extractor.druid_metadata.extractor.sqlalchemy.{}'.format(sql_alchemy_extractor.SQLAlchemyExtractor.CONN_STRING): conn_string,
    'loader.filesystem_csv_neo4j.node_dir_path': '/var/tmp/amundsen/neo4j/nodes',
    'loader.filesystem_csv_neo4j.relationship_dir_path': '/var/tmp/amundsen/neo4j/relations',
    'publisher.neo4j.neo4j_endpoint': 'bolt://localhost:7687',
    'publisher.neo4j.neo4j_user': '',
    'publisher.neo4j.neo4j_password': '',
    'publisher.neo4j.node_files_directory': '/var/tmp/amundsen/neo4j/nodes',
    'publisher.neo4j.relation_files_directory': '/var/tmp/amundsen/neo4j/relations',
    'publisher.neo4j.job_publish_tag': 'druid_metadata_job'
})

logger.info(f"Job configuration: {job_config}")
logger.info("Creating job and task...")

task = DefaultTask(
    extractor=DruidMetadataExtractor(),
    loader=FsNeo4jCSVLoader(),
    transformer=NoopTransformer()
)

job = DefaultJob(conf=job_config, task=task, publisher=Neo4jCsvPublisher())

logger.info("Launching the job...")
job.launch()
logger.info("Job completed.")
