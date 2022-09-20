#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from sqlite3 import paramstyle
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from urllib.parse import urlparse, parse_qs
import requests
from mollie.api.client import Client
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

# Basic full refresh stream
class MollieStream(HttpStream, ABC):

    url_base = "https://api.mollie.com/v2/"

class Methods(MollieStream):

    primary_key = "id"

    def __init__(self, config: Mapping[str, Any], **kwargs) -> None:
        super().__init__(**kwargs)
        

    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "methods/all"

        def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:   
            return response.json().get('_embedded', []).get('methods', [])


# Basic incremental stream
class IncrementalMollieStream(MollieStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}

class Payments(MollieStream):
    primary_key = "id"

    def __init__(self, config: Mapping[str, Any], **kwargs) -> None:
        super().__init__(**kwargs)
        
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "payments"    

    def request_params(
        self, stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params = super().request_params(stream_state, stream_slice, next_page_token)
        if next_page_token:
            params.update(next_page_token)
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if response.json().get('_links').get('next'):
            url = response.json().get('_links').get('next').get('href')
            parsed_url = urlparse(url)
            from_param = parse_qs(parsed_url.query)['from'][0]
            return {"from": from_param}
        else:
            return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return [response.json().get('_embedded', [])]
            
# Source
class SourceMollie(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        connection check to validate that the user-provided config can be used to connect to the underlying API

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            mollie_client = Client()
            mollie_client.set_api_key(config['api_key'])
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = TokenAuthenticator(token=config['api_key'])  
        return [Methods(config, authenticator=auth),
                 Payments(config, authenticator=auth)]