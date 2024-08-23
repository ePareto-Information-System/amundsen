
import json
import logging
from http import HTTPStatus
from typing import Any, Iterable, Mapping, Union, no_type_check

from amundsen_common.entity.resource_type import ResourceType
from amundsen_common.models.feature import FeatureSchema
from amundsen_common.models.generation_code import GenerationCodeSchema
from amundsen_common.models.lineage import LineageSchema
from flasgger import swag_from
from flask import request
from flask_restful import Resource, reqparse

from metadata_service.api.badge import BadgeCommon
from metadata_service.api.tag import TagCommon
from metadata_service.exception import NotFoundException
from metadata_service.proxy import get_proxy_client

LOGGER = logging.getLogger(__name__)




class CustomLineageAPI(Resource):

    def __init__(self) -> None:
        self.client = get_proxy_client()
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('direction', type=str, required=False, default="both")
        self.parser.add_argument('depth', type=int, required=False, default=1)

    @swag_from('swagger_doc/custom_lineage/custom_lineage_get.yml')
    def get(self, id: str) -> Iterable[Union[Mapping, int, None]]:
        args = self.parser.parse_args()
        direction = args.get('direction')
        depth = args.get('depth')
        try:
            lineage = self.client.get_lineage(id=id,
                                              resource_type=ResourceType.Feature,
                                              direction=direction,
                                              depth=depth)
            schema = LineageSchema()
            return schema.dump(lineage), HTTPStatus.OK
        except NotFoundException:
            LOGGER.error(f'NotFoundException: feature_uri {id} lineage does not exist')
            return {'message': f'feature_uri {id} lineage does not exist'}, HTTPStatus.NOT_FOUND
        except Exception as e:
            LOGGER.error(f'Internal server error occurred when getting feature lineage: {e}')
            return {'message': f'Exception raised when getting lineage: {e}'}, HTTPStatus.INTERNAL_SERVER_ERROR




class CustomColumnLineageAPI(Resource):
    """
    Custom ColumnLineageAPI supports GET operation to get column lineage
    """
    def __init__(self) -> None:
        self.client = get_proxy_client()
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('direction', type=str, required=False, default="both")
        self.parser.add_argument('depth', type=int, required=False, default=1)
        super(CustomColumnLineageAPI, self).__init__()

    @swag_from('swagger_doc/custom_lineage/custom_column_lineage_get.yml')
    def get(self, table_uri: str, column_name: str) -> Iterable[Union[Mapping, int, None]]:
        args = self.parser.parse_args()
        direction = args.get('direction')
        depth = args.get('depth')

        try:
            lineage = self.client.get_lineage(id=f"{table_uri}/{column_name}",
                                              resource_type=ResourceType.Column,
                                              direction=direction,
                                              depth=depth)
            schema = LineageSchema()
            return schema.dump(lineage), HTTPStatus.OK
        except Exception as e:
            return {'message': f'Exception raised when getting lineage: {e}'}, HTTPStatus.NOT_FOUND



class CustomTableLineageAPI(Resource):
    def __init__(self) -> None:
        self.client = get_proxy_client()
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('direction', type=str, required=False, default="both")
        self.parser.add_argument('depth', type=int, required=False, default=1)
        self.parser.add_argument('table_id', type=str )
        super(CustomTableLineageAPI, self).__init__()

    @swag_from('swagger_doc/custom_lineage/custom_table_lineage_get.yml')
    def post(self) -> Iterable[Union[Mapping, int, None]]:
        args = self.parser.parse_args()
        print("args",args)
        id = args.get('table_id')
        direction = args.get('direction')
        depth = args.get('depth')
        try:
            lineage = self.client.get_lineage(id=id,
                                              resource_type=ResourceType.Table,
                                              direction=direction,
                                              depth=depth)
            schema = LineageSchema()
            return schema.dump(lineage), HTTPStatus.OK
        except Exception as e:
            return {'message': f'Exception raised when getting lineage: {e}'}, HTTPStatus.NOT_FOUND
 
    # create new table lineage  

    @swag_from('swagger_doc/custom_lineage/custom_table_lineage_put.yml')  
    def put(self) -> Iterable[Union[Mapping, int, None]]:
        try:
            # Get the request data
            data = request.get_json()
            print(request)
            # Extract the necessary information from the request data
            source_id = data.get('source_id')
            target_id = data.get('target_id')
            #relationship_type = data.get('relationship_type')
            
            # Create the lineage using the client
            lineage = self.client.create_lineage(source_table=source_id,
                                                 target_tables=target_id,
                                                 properties={})
                                                #  relationship_type=relationship_type)
            
            # Return the created lineage
            schema = LineageSchema()
            return schema.dump(lineage), HTTPStatus.CREATED
        except Exception as e:
            return {'message': f'Exception raised when creating lineage: {e}'}, HTTPStatus.INTERNAL_SERVER_ERROR

