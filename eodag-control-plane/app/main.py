from contextlib import asynccontextmanager
from typing import Annotated

from eodag import EODataAccessGateway, setup_logging
from eodag.api.product.metadata_mapping import ONLINE_STATUS
from eodag.utils.exceptions import NoMatchingCollection
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, model_validator



class S3Response(BaseModel):
    endpoint_url: str
    path: str
    key: str | None = None
    secret: str | None = None
    token: str | None = None
    anon: bool = False
    requester_pays: bool = False

    @model_validator(mode="before")
    @classmethod
    def _flatten_client_kwargs(cls, data: dict) -> dict:
        """Promote client_kwargs.endpoint_url to the top level."""
        if isinstance(data, dict) and "client_kwargs" in data:
            data = {**data, **data.pop("client_kwargs")}
        return data


class HttpResponse(BaseModel):
    path: str
    headers: dict[str, str] = {}


# Union type: the Rust proxy disambiguates by the presence of `endpoint_url`.
EodagResponse = Annotated[S3Response | HttpResponse, Field(discriminator=None)]

@asynccontextmanager
async def lifespan(app: FastAPI):
    """EODAG control plane lifespan context manager."""
    setup_logging(2)  # INFO level
    app.state.dag = EODataAccessGateway()
    yield

app = FastAPI(title="EODAG control plane", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get(
    "/resolve/eodag", response_model=EodagResponse)
def resolve_with_eodag(
    provider: str,
    collection_id: str,
    item_id: str,
    asset_key: str,
) -> EodagResponse:
    """
    Resolve download instructions for a product asset via EODAG.

    Searches for `item_id` within `collection_id` on the given `provider`,
    checks that the product is online, then returns either S3 credentials
    or HTTP download details for the requested `asset_key`.
    """
    dag: EODataAccessGateway = app.state.dag

    try:
        dag.get_collection_from_alias(collection_id)
    except NoMatchingCollection as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        search_result = dag.search(id=item_id, collection=collection_id, provider=provider)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if len(search_result) == 0:
        raise HTTPException(status_code=404, detail=f"Item not found in collection {collection_id} from provider {provider}")
    
    product = search_result[0]

    if product.properties.get("order:status", ONLINE_STATUS) != ONLINE_STATUS:
        raise HTTPException(status_code=409, detail=f"Product {product.id} is not online yet")

    try:
        storage_options = product._get_storage_options(asset_key=asset_key)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return storage_options


@app.get("/resolve/stac", response_model=EodagResponse)
def resolve_from_stac(
    item_url: str,
    asset_key: str,
) -> EodagResponse:
    """
    Resolve download instructions for a STAC item asset.

    Fetches the STAC item at `item_url`, locates the requested `asset_key`,
    and returns either S3 credentials or HTTP download details that the
    Rust proxy can use to stream the data back to the client.
    """
    dag: EODataAccessGateway = app.state.dag

    search_result = dag.import_stac_items([item_url])
    
    if len(search_result) == 0:
        raise HTTPException(status_code=404, detail="STAC item not found")
    
    product = search_result[0]

    print(f"Product {product.id} found, provider: {product.provider}, collection: {product.collection_id}")

    try:
        storage_options = product._get_storage_options(asset_key=asset_key)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return storage_options

