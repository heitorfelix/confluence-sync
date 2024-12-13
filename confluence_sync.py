import requests
from requests.auth import HTTPBasicAuth
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
from atlassian import Confluence
from datetime import datetime, timedelta
from typing import List, Tuple, Dict

load_dotenv()

class ConfluenceSync:
    def __init__(self, space: str):
        self.space = space
        self.base_url = os.getenv('CONFLUENCE_BASE_URL')
        self.confluence_base_url = os.getenv("CONFLUENCE_REST_API_URL")
        self.username = os.getenv("ATLASSIAN_USERNAME")
        self.token = os.getenv("ATLASSIAN_TOKEN")
        self.confluence = Confluence(url=self.base_url, username=self.username, password=self.token)
        self.azure_connection_string = os.getenv("STORAGE_CONNECTION_STRING")
        self.auth = HTTPBasicAuth(self.username, self.token)
        self.container_name = 'public' if self.space == 'SIA' else 'private'
        self.blob_service_client = BlobServiceClient.from_connection_string(self.azure_connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        self._create_container()
        self.root_page_id = self.get_root_page_id()

    def _create_container(self) -> None:
        """
        Creates the Azure Blob Storage container if it doesn't exist.
        """
        try:
            self.container_client.create_container()
        except Exception as e:
            print(f"Container already exists or could not be created: {e}")

    def upload_to_azure_blob(self, content: str, blob_name: str, metadata: Dict[str, str]) -> None:
        """
        Uploads content to Azure Blob Storage.

        Args:
            content (str): The content to upload.
            blob_name (str): The name of the blob.
            metadata (Dict[str, str]): Metadata to associate with the blob.

        Returns:
            None
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.upload_blob(content, overwrite=True)
            blob_client.set_blob_metadata(metadata)
            print(f"Uploaded {blob_name} to Azure Blob Storage.")
        except Exception as e:
            print(f"Failed to upload {blob_name}: {e}")

    def get_child_pages(self, parent_id: str) -> List[Dict]:
        """
        Retrieves the child pages of a given parent page.

        Args:
            parent_id (str): The ID of the parent page.

        Returns:
            List[Dict]: A list of child pages.
        """
        url = f"{self.confluence_base_url}/{parent_id}/child/page"
        response = requests.get(url, auth=self.auth)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()['results']

    def get_page_content(self, page_id: str) -> Tuple[str, str, str]:
        """
        Retrieves the content of a given page.

        Args:
            page_id (str): The ID of the page.

        Returns:
            Tuple[str, str, str]: The title, body content, and created date of the page.
        """
        url = f"{self.confluence_base_url}/{page_id}"
        params = {'expand': 'body.storage,version', 'orderby': 'history.lastModified desc', 'limit': 1}
        response = requests.get(url, auth=self.auth, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        title = data['title']
        body_content = data['body']['storage']['value']
        created_date = data['version']['when']
        return title, body_content, created_date

    def get_updated_page_content(self, page_id: str) -> Tuple[str, str, str]:
        """
        Retrieves the updated content of a given page.

        Args:
            page_id (str): The ID of the page.

        Returns:
            Tuple[str, str, str]: The title, body content, and created date of the page.
        """
        url = f"{self.confluence_base_url}/{page_id}"
        params = {'expand': 'body.storage,version', 'orderby': 'history.lastModified desc', 'limit': 1}
        response = requests.get(url, auth=self.auth, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        title = data['title']
        body_content = data['body']['storage']['value']
        created_date = data['version']['when']
        return title, body_content, created_date

    def get_root_page_id(self) -> str:
        """
        Retrieves the root page ID of the space.

        Returns:
            str: The root page ID.
        """
        space_page_id = self.confluence.get_all_pages_from_space(space=self.space, limit=1)[0]['id']
        ancestors = self.confluence.get_page_ancestors(space_page_id)
        if not ancestors:
            return space_page_id
        root_page_id = ancestors[0]['id']
        return root_page_id

    def process_page_full(self, page_id: str, path: str = '') -> None:
        """
        Processes a page and its child pages recursively, uploading their content to Azure Blob Storage.

        Args:
            page_id (str): The ID of the page.
            path (str): The path to the page.

        Returns:
            None
        """
        try:
            # Get page content
            title, content, created_date = self.get_page_content(page_id)
            # Define blob name based on path and title
            blob_name = os.path.join(path, f"{title}.html").replace("\\", "/")
            metadata = {'created_date': created_date}
            # Upload content to Azure Blob Storage
            self.upload_to_azure_blob(content, blob_name, metadata)
            # Get child pages and process them recursively
            child_pages = self.get_child_pages(page_id)
            for child in child_pages:
                self.process_page_full(child['id'], os.path.join(path, title))
        except Exception as e:
            print(f"An error occurred: {e}")

    def build_full_path(self, page_id: str) -> str:
        """
        Builds the full path of a page based on its ancestors.

        Args:
            page_id (str): The ID of the page.

        Returns:
            str: The full path of the page.
        """
        ancestors = self.confluence.get_page_ancestors(page_id)
        ancestor_titles = [ancestor['title'] for ancestor in ancestors]
        return '/'.join(ancestor_titles)

    def process_page_incremental(self) -> None:
        """
        Processes pages incrementally, uploading their content to Azure Blob Storage.

        Returns:
            None
        """
        try:
            # Calculate yesterday's date
            yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
            # CQL query to find pages modified yesterday
            cql = f"space = {self.space} AND type = page AND lastmodified >= {yesterday} AND lastmodified < {datetime.now().strftime('%Y-%m-%d')}"
            # Execute the query and extract page IDs
            results = self.confluence.cql(cql, expand="ancestors")
            page_ids = [result['content']['id'] for result in results['results']]
            for page_id in page_ids:
                # Get content and path
                title, body_content, created_date = self.get_page_content(page_id)
                # Build full path
                path = self.build_full_path(page_id)
                # Define blob name based on path and title
                blob_name = os.path.join(path, f"{title}.html").replace("\\", "/")
                metadata = {'created_date': created_date}
                # Save HTML to full path
                self.upload_to_azure_blob(body_content, blob_name, metadata)
        except Exception as e:
            print(f"An error occurred: {e}")

