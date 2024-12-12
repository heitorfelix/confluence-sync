from confluence_sync import ConfluenceSync
import json
import logging
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger function processed a request.')

    # Extract parameters from the query string
    space = req.params.get('space')
    ingestion_type = req.params.get('type')

    if not space or not ingestion_type:
        return func.HttpResponse(
            "Please pass both 'space' and 'type' parameters in the query string.",
            status_code=400
        )

    try:
        confluence_sync = ConfluenceSync(space=space)
        
        if ingestion_type == 'full':
            confluence_sync.process_page_full(confluence_sync.root_page_id, path='')
            return func.HttpResponse(f"Full sync completed for space: {space}.", status_code=200)
        elif ingestion_type == 'incremental':
            confluence_sync.process_page_incremental()
            return func.HttpResponse(f"Incremental sync completed for space: {space}.", status_code=200)
        else:
            return func.HttpResponse(
                "Invalid 'type' parameter. Please use 'full' or 'incremental'.",
                status_code=400
            )
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return func.HttpResponse(
            "An error occurred while processing the request.",
            status_code=500
        )