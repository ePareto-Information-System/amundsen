
import json
import logging
from http import HTTPStatus
from typing import  Iterable, Mapping, Union

from amundsen_common.entity.resource_type import ResourceType
from amundsen_common.models.lineage import LineageSchema
from flasgger import swag_from
from flask import request
from flask_restful import Resource, reqparse
from metadata_service.exception import NotFoundException
from metadata_service.proxy import get_proxy_client

LOGGER = logging.getLogger(__name__)






class CustomColumnLineageAPI(Resource):
    """
    Custom ColumnLineageAPI supports GET operation to get column lineage
    """
    def __init__(self) -> None:
        self.client = get_proxy_client()
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('direction', type=str, required=False, default="both")
        self.parser.add_argument('depth', type=int, required=False, default=1)
        self.parser.add_argument('column_id', type=str, required=False)
        super(CustomColumnLineageAPI, self).__init__()

    @swag_from('swagger_doc/custom_lineage/custom_column_lineage_get.yml')
    def post(self) -> Iterable[Union[Mapping, int, None]]:
        args = self.parser.parse_args()
        id = args.get('column_id')
        direction = args.get('direction')
        depth = args.get('depth')

        try:
            lineage = self.client.get_lineage(id=id,
                                              resource_type=ResourceType.Column,
                                              direction=direction,
                                              depth=depth)
            schema = LineageSchema()
            return schema.dump(lineage), HTTPStatus.OK
        except Exception as e:
            return {'message': f'Exception raised when getting lineage: {e}'}, HTTPStatus.NOT_FOUND
        
    #create column lineage
    @swag_from('swagger_doc/custom_lineage/custom_column_lineage_put.yml')
    def put(self) -> Iterable[Union[Mapping, int, None]]:
        try:
            # Get the request data
            data = request.get_json()
            # Extract the necessary information from the request data
            source_id = data.get('source_id')
            target_id = data.get('target_id')
            lineage_type = data.get('lineage_type')
            
            # Create the lineage using the client
            lineage_response = self.client.create_lineage(source_key=source_id,
                                                 target_keys=target_id,
                                                    lineage_type=lineage_type,
                                                     resource_type=ResourceType.Column,
                                                 properties={})
            
            # Return the lineage response directly as JSON
            return lineage_response, HTTPStatus.CREATED
        except Exception as e:
            return {'message': f'Exception raised when creating lineage: {e}'}, HTTPStatus.INTERNAL_SERVER_ERROR
        
    #delete column lineage
    @swag_from('swagger_doc/custom_lineage/custom_column_lineage_delete.yml')
    def delete(self) -> Iterable[Union[Mapping, int, None]]:
        try:
            # Get the request data
            data = request.get_json()
            # Extract the necessary information from the request data
            source_id = data.get('source_id')
            target_id = data.get('target_id')
            lineage_type = data.get('lineage_type')
            
            # Delete the lineage using the client
            deleted_count = self.client.delete_lineage(source_key=source_id,
                                                   target_keys=target_id, lineage_type=lineage_type, resource_type=ResourceType.Column)
        
        # Return the response with deleted count
            return {'message': f"{deleted_count}"}, HTTPStatus.OK
        except Exception as e:
            return {'message': f'Exception raised when deleting lineage: {e}'}, HTTPStatus.INTERNAL_SERVER_ERROR



class CustomTableLineageAPI(Resource):
    def __init__(self) -> None:
        self.client = get_proxy_client()
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('direction', type=str, required=False, default="both")
        self.parser.add_argument('depth', type=int, required=False, default=1)
        self.parser.add_argument('table_id', type=str )
        super(CustomTableLineageAPI, self).__init__()


    #get lineage
    @swag_from('swagger_doc/custom_lineage/custom_table_lineage_get.yml')
    def post(self) -> Iterable[Union[Mapping, int, None]]:
        args = self.parser.parse_args()
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
            # Extract the necessary information from the request data
            source_id = data.get('source_id')
            target_id = data.get('target_id')
            lineage_type = data.get('lineage_type')
            
            # Create the lineage using the client
            lineage_response = self.client.create_lineage(source_key=source_id,
                                                 target_keys=target_id,
                                                    lineage_type=lineage_type,
                                                     resource_type=ResourceType.Table,
                                                 properties={})
            
            # Return the lineage response directly as JSON
            return lineage_response, HTTPStatus.CREATED
        except Exception as e:
            return {'message': f'Exception raised when creating lineage: {e}'}, HTTPStatus.INTERNAL_SERVER_ERROR
        
        #delete table lineage
    @swag_from('swagger_doc/custom_lineage/custom_table_lineage_delete.yml')
    def delete(self) -> Iterable[Union[Mapping, int, None]]:
        try:
            # Get the request data
            data = request.get_json()
            # Extract the necessary information from the request data
            source_id = data.get('source_id')
            target_id = data.get('target_id')
            lineage_type = data.get('lineage_type')
            
            # Delete the lineage using the client
            deleted_count = self.client.delete_lineage(source_key=source_id,
                                                   target_keys=target_id, lineage_type=lineage_type, resource_type=ResourceType.Table)
        
        # Return the response with deleted count
            return {'message': f"{deleted_count}"}, HTTPStatus.OK
        except Exception as e:
            return {'message': f'Exception raised when deleting lineage: {e}'}, HTTPStatus.INTERNAL_SERVER_ERROR


