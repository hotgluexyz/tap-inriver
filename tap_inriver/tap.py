"""inRiver Singer tap: Product → ProductItem → ItemSize (bulk batches)."""

from __future__ import annotations

from typing import Any, List

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th

from tap_inriver.client import InRiverClient
from tap_inriver.schema_builder import entity_stream_schema, entity_with_parent_id_schema
from tap_inriver.streams import (
    ItemSizeStream,
    ProductItemStream,
    ProductStream,
)


class TapInRiver(Tap):
    """Singer tap for inRiver PIM REST API."""

    name = "tap-inriver"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url_base",
            th.StringType,
            required=True,
            description="Environment base URL, e.g. https://api-test1a-use.productmarketingcloud.com",
        ),
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="REST API key (X-inRiver-APIKey)",
        ),
        th.Property(
            "start_date",
            th.StringType,
            required=False,
            description="ISO 8601 lower bound for LastModified on Product query",
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            required=False,
            description="How many products to process per batch (links + item fetchdata + child sync); default 25. Item/Size fetchdata is not split by this size.",
        ),
    ).to_dict()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._inriver_client = InRiverClient(
            base_url=self.config["api_url_base"],
            api_key=str(self.config["api_key"]),
        )

    def discover_streams(self) -> List[Stream]:
        client = self._inriver_client

        product_fts = client.get_json("/api/v1.0.1/model/entitytypes/Product/fieldtypes")
        if not isinstance(product_fts, list):
            product_fts = []
        product_schema = entity_stream_schema(product_fts)

        item_fts = client.get_json("/api/v1.0.1/model/entitytypes/Item/fieldtypes")
        if not isinstance(item_fts, list):
            item_fts = []
        size_fts = client.get_json("/api/v1.0.1/model/entitytypes/Size/fieldtypes")
        if not isinstance(size_fts, list):
            size_fts = []

        # ProductItem: product_id + Item entity; ItemSize: item_id + Size entity (no link columns)
        pi_schema = entity_with_parent_id_schema("product_id", item_fts)
        is_schema = entity_with_parent_id_schema("item_id", size_fts)

        return [
            ProductStream(
                self,
                schema=product_schema,
                fieldtypes=product_fts,
                item_fieldtypes=item_fts,
            ),
            ProductItemStream(self, schema=pi_schema),
            ItemSizeStream(self, schema=is_schema, size_fieldtypes=size_fts),
        ]


if __name__ == "__main__":
    TapInRiver.cli()
