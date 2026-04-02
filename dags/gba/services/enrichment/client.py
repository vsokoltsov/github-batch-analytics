from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth


def get_github_client(github_token: str) -> RESTClient:
    return RESTClient(
        base_url="https://api.github.com",
        auth=BearerTokenAuth(token=github_token),
    )
