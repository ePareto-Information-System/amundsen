import uuid
from pyhocon import ConfigFactory
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher

# Configuration settings
neo4j_endpoint = 'bolt://localhost:7687'
neo4j_user = ''
neo4j_password = ''
job_publish_tag = 'database'
tmp_es_file_path = '/var/tmp/amundsen_dashboard/elasticsearch_dashboard_upload/es_data.json'

elasticsearch_new_index_name = 'dashboard_search_index_{ds}_{hex_str}'.format(
    ds='2020-05-12',
    hex_str=uuid.uuid4().hex
)
elasticsearch_doc_type = 'dashboard'
elasticsearch_index_alias = 'dashboard_search_index'

# Correct Cypher Query without the placeholder
correct_cypher_query = """
MATCH (dashboard:Dashboard)
RETURN dashboard.id as id, dashboard.name as name, dashboard.dashboard_url as url, 
dashboard.created_timestamp as created_timestamp, dashboard.updated_timestamp as updated_timestamp
"""

# Job configuration dictionary
job_config = ConfigFactory.from_dict({
    'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.GRAPH_URL_CONFIG_KEY): neo4j_endpoint,
    'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.MODEL_CLASS_CONFIG_KEY): 
        'databuilder.models.dashboard_elasticsearch_document.DashboardESDocument',
    'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_USER): neo4j_user,
    'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_PW): neo4j_password,
    'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_ENCRYPTED): False,
    'extractor.search_data.{}'.format(Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY): correct_cypher_query,
    'extractor.search_data.job_publish_tag': job_publish_tag,
    'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY): tmp_es_file_path,
    'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
    'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY): tmp_es_file_path,
    'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
    'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY): 'http://localhost:9200',
    'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY): elasticsearch_new_index_name,
    'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY): elasticsearch_doc_type,
    'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY): 
        '{"mappings": {"dashboard": {"properties": {"name": {"type": "text"}, "uri": {"type": "keyword"}}}}}',
    'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY): elasticsearch_index_alias,
})

# Create and launch the job
job = DefaultJob(
    conf=job_config,
    task=DefaultTask(extractor=Neo4jSearchDataExtractor(), loader=FSElasticsearchJSONLoader()),
    publisher=ElasticsearchPublisher()
)
job.launch()
