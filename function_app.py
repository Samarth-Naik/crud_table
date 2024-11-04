import azure.functions as func
import logging
from azure.data.tables import TableServiceClient, TableEntity
import os
import json
 
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
table_storage_connection_string = os.environ["AzureWebJobsStorage"]
table_service_client = TableServiceClient.from_connection_string(table_storage_connection_string)
table_name = "Players"
 
@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing a request for CRUD operations on Table Storage.')
 
    table_client=table_service_client.get_table_client(table_name)
 
    req_method = req.method
 
    if req_method == "GET":
        partition_key=req.params.get('partition_key')
        row_key=req.params.get('row_key')
        if not partition_key or not row_key:
            return func.HttpResponse(
                "Please provide both 'partitionKey' and 'rowKey' as query parameters.",
                status_code=400
            )
        try:
            entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)
            return func.HttpResponse(
                json.dumps(entity),
                status_code=200,
                mimetype="application/json"
            )
        except Exception as e:
            logging.error(f"Error retrieving entity: {e}")
            return func.HttpResponse(f"Error retrieving entity: {str(e)}", status_code=500)
    elif req_method=="POST":
        try:
            new_entity=req.get_json()
            table_client.create_entity(entity=new_entity)
            return func.HttpResponse("Entity created successfully", status_code=201)
        except Exception as e:
            logging.error(f"Error creating new entity: {e}")
            return func.HttpResponse(f"Error creating entity: {str(e)}", status_code=500)
    elif req_method=="PUT":
        try:
            update_entity=req.get_json()
            table_client.update_entity(mode="merge",entity=update_entity)
            return func.HttpResponse("Entity updated successfully", status_code=200)
        except Exception as e:
            logging.error(f"Error updating entity: {e}")
            return func.HttpResponse(f"Error updating entity: {str(e)}", status_code=500)
    elif req_method=="DELETE":
        partition_key=req.params.get('partition_key')
        row_key=req.params.get('row_key')
        if not partition_key or not row_key:
            return func.HttpResponse(
                "Please provide both 'partitionKey' and 'rowKey' as query parameters.",
                status_code=400
            )
        try:
            table_client.delete_entity(partition_key=partition_key, row_key=row_key)
            return func.HttpResponse("Entity deleted successfully", status_code=200)
        except Exception as e:
            logging.error(f"Error deleting entity: {e}")
            return func.HttpResponse(f"Error deleting entity: {str(e)}", status_code=500)
    else:
        return func.HttpResponse(
            "Unsupported HTTP method. Please use GET, POST, PUT, or DELETE.",
            status_code=400
        )