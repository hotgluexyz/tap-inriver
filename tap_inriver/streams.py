"""Stream classes for inRiver: Product (bulk batches) → ProductItem → ItemSize."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Type

from hotglue_singer_sdk.exceptions import InvalidStreamSortException
from hotglue_singer_sdk.helpers._state import finalize_state_progress_markers, log_sort_error
from hotglue_singer_sdk.streams import Stream

from tap_inriver.client import InRiverClient
from tap_inriver.schema_builder import build_fieldtype_key_map, merge_fieldvalue_dict

LINK_PRODUCT_ITEM = "ProductItem"
LINK_ITEM_SIZE = "ItemSize"

# entities:fetchdata `objects` (same for Product, Item, and Size bulk requests)
FETCHDATA_OBJECTS = "EntitySummary,FieldValues,Media"


def _chunks(items: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _sync_without_begin_log(stream: Stream, context: Optional[dict]) -> None:
    """Same as SDK Stream.sync but without the generic 'Beginning ... sync' INFO line."""
    signpost = stream.get_replication_key_signpost(context)
    if signpost:
        stream._write_replication_key_signpost(context, signpost)
    if stream.selected:
        stream._write_schema_message()
    stream._sync_records(context)


def _write_record_count_log_no_extra_tags(
    stream: Stream, record_count: int, _context: Optional[dict]
) -> None:
    """Like SDK _write_record_count_log but never add partition context to METRIC tags."""
    counter_metric: Dict[str, Any] = {
        "type": "counter",
        "metric": "record_count",
        "value": record_count,
        "tags": {"stream": stream.name},
    }
    stream._write_metric_log(counter_metric, extra_tags=None)


def _parse_summary_row(summary: dict | None, entity_id_fallback: int | None) -> dict[str, Any]:
    if not summary:
        return {"entity_id": entity_id_fallback}
    return {
        "entity_id": summary.get("id", entity_id_fallback),
        "entity_type_id": summary.get("entityTypeId"),
        "display_name": summary.get("displayName"),
        "display_description": summary.get("displayDescription"),
        "modified_date": summary.get("modifiedDate"),
        "created_date": summary.get("createdDate"),
        "completeness": summary.get("completeness"),
        "field_set_id": summary.get("fieldSetId"),
        "segment_id": summary.get("segmentId"),
    }


def _flatten_entity_row(row: dict, key_map: dict[str, str]) -> dict[str, Any]:
    """One entities:fetchdata row → same shape as entity_stream_schema (summary + fieldValues + media)."""
    eid = row.get("entityId")
    base = _parse_summary_row(row.get("summary"), eid)
    fv = merge_fieldvalue_dict(row.get("fieldValues"), key_map)
    out = {**base, **fv}
    if "media" in row:
        out["media"] = row.get("media")
    return out


class ProductStream(Stream):
    """Root: one query for all Product IDs; process products in batches of batch_size (default 25); per batch: fetchdata products, GET ProductItem links, one fetchdata for all linked Items, emit products, sync children."""

    primary_keys = ["entity_id"]

    def __init__(
        self,
        tap: Any,
        *,
        schema: dict,
        fieldtypes: List[dict],
        item_fieldtypes: List[dict] | None = None,
    ) -> None:
        self._fieldtypes = fieldtypes
        self._key_map = build_fieldtype_key_map(fieldtypes)
        self._item_key_map = build_fieldtype_key_map(item_fieldtypes or [])
        # Filled per batch for ProductItem (links + item fetchdata); item_ids passed in child context.
        self._pending_product_item_links: List[dict] = []
        self._pending_items_by_id: Dict[int, dict[str, Any]] = {}
        super().__init__(tap=tap, name="Product", schema=schema)
        self.replication_key = "modified_date"

    def sync(self, context: Optional[dict] = None) -> None:
        """Skip SDK default 'Beginning ... sync' line; progress is logged in _sync_records."""
        _sync_without_begin_log(self, context)

    @property
    def client(self) -> InRiverClient:
        return self._tap._inriver_client  # type: ignore[attr-defined]

    def _query_product_ids(self, modified_since: str | None) -> List[int]:
        """Single POST /query; returns all Product entityIds the API returns (no client-side cap)."""
        criteria: List[dict] = [
            {"type": "EntityTypeId", "operator": "Equal", "value": "Product"},
        ]
        if modified_since:
            criteria.append(
                {
                    "type": "LastModified",
                    "operator": "GreaterThanOrEqual",
                    "value": modified_since,
                }
            )
        body: dict = {
            "systemCriteria": criteria,
            "dataCriteria": [],
            "linkCriterion": None,
            "dataCriteriaOperator": "And",
        }
        data = self.client.post_json("/api/v1.0.0/query", body)
        ids = data.get("entityIds") or []
        return [int(x) for x in ids]

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Not used; sync uses overridden _sync_records."""
        yield from ()

    def _sync_records(self, context: Optional[dict] = None) -> None:
        record_count = 0
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)

            start = None
            if self.replication_key:
                start = self.get_starting_replication_key_value(current_context)
            start_s = str(start) if start else None
            if not start_s:
                start_s = self.config.get("start_date")

            # All product IDs from one query; only *processing* is batched (batch_size).
            product_ids = self._query_product_ids(start_s)
            batch = max(1, int(self.config.get("batch_size", 25)))
            total_products = len(product_ids)
            total_batches = (total_products + batch - 1) // batch if total_products else 0
            self.logger.info(
                "Total Product records: %s, total batches to process: %s",
                total_products,
                total_batches,
            )

            for batch_num, chunk in enumerate(_chunks(product_ids, batch), start=1):
                self.logger.info(
                    "Processing batch %s of %s, batch product count %s",
                    batch_num,
                    total_batches,
                    len(chunk),
                )
                payload = {"entityIds": chunk, "objects": FETCHDATA_OBJECTS}
                rows = self.client.post_json("/api/v1.0.1/entities:fetchdata", payload)
                if not isinstance(rows, list):
                    rows = []
                rows_by_id = {r.get("entityId"): r for r in rows}

                all_pi_links: List[dict] = []
                item_ids: set[int] = set()
                for pid in chunk:
                    for link in self.client.get_links(int(pid), LINK_PRODUCT_ITEM, "outbound"):
                        all_pi_links.append(link)
                        tid = link.get("targetEntityId")
                        if tid is not None:
                            item_ids.add(int(tid))

                # One fetchdata for all Items linked from this product batch (no chunking).
                items_by_id: Dict[int, dict[str, Any]] = {}
                if item_ids:
                    item_rows = self.client.post_json(
                        "/api/v1.0.1/entities:fetchdata",
                        {"entityIds": sorted(item_ids), "objects": FETCHDATA_OBJECTS},
                    )
                    if isinstance(item_rows, list):
                        for row in item_rows:
                            eid = row.get("entityId")
                            if eid is None:
                                continue
                            items_by_id[int(eid)] = _flatten_entity_row(row, self._item_key_map)

                batch_product_records = 0
                for pid in chunk:
                    row = rows_by_id.get(pid)
                    if not row:
                        continue
                    summary = row.get("summary")
                    base = _parse_summary_row(summary, pid)
                    fv = merge_fieldvalue_dict(row.get("fieldValues"), self._key_map)
                    record = {**base, **fv}
                    if "media" in row:
                        record["media"] = row.get("media")
                    for key, val in (state_partition_context or {}).items():
                        if key not in record:
                            record[key] = val

                    self._check_max_record_limit(record_count)
                    if selected:
                        if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                            self._write_state_message()
                        self._write_record_message(record)
                        try:
                            self._increment_stream_state(record, context=current_context)
                        except InvalidStreamSortException as ex:
                            log_sort_error(
                                log_fn=self.logger.error,
                                ex=ex,
                                record_count=record_count + 1,
                                partition_record_count=partition_record_count + 1,
                                current_context=current_context,
                                state_partition_context=state_partition_context,
                                stream_name=self.name,
                            )
                            raise ex
                    record_count += 1
                    partition_record_count += 1
                    batch_product_records += 1

                if self.child_streams and any(
                    cs.selected or cs.has_selected_descendents for cs in self.child_streams
                ):
                    self._pending_product_item_links = all_pi_links
                    self._pending_items_by_id = items_by_id
                    try:
                        self._sync_children({"item_ids": sorted(item_ids)})
                    finally:
                        pi_stream = self._tap.streams.get("ProductItem")  # type: ignore[union-attr]
                        isz_stream = self._tap.streams.get("ItemSize")  # type: ignore[union-attr]
                        pi_emitted = int(
                            getattr(pi_stream, "_last_batch_product_item_emitted", 0) or 0
                        )
                        isz_links = int(getattr(isz_stream, "_last_batch_item_size_links", 0) or 0)
                        isz_emitted = int(
                            getattr(isz_stream, "_last_batch_item_size_emitted", 0) or 0
                        )
                        self.logger.info(
                            "Batch %s/%s: Product records %s; ProductItem links %s, emitted %s; "
                            "ItemSize links %s, emitted %s",
                            batch_num,
                            total_batches,
                            batch_product_records,
                            len(all_pi_links),
                            pi_emitted,
                            isz_links,
                            isz_emitted,
                        )
                        self._pending_product_item_links = []
                        self._pending_items_by_id = {}

            if current_context == state_partition_context:
                finalize_state_progress_markers(state)

        if not context:
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        self._write_state_message()


class ProductItemStream(Stream):
    """Child of ProductStream: Item rows with product_id (source product); then sync ItemSize child."""

    parent_stream_type: Optional[Type[Stream]] = None  # set after ProductStream class
    primary_keys = ["product_id", "entity_id"]

    def __init__(self, tap: Any, *, schema: dict) -> None:
        super().__init__(tap=tap, name="ProductItem", schema=schema)
        # No partition keys in STATE (parent merges item_ids from context before each sync).
        self.state_partitioning_keys = None

    @property
    def client(self) -> InRiverClient:
        return self._tap._inriver_client  # type: ignore[attr-defined]

    def sync(self, context: Optional[dict] = None) -> None:
        self.state_partitioning_keys = None
        self._last_batch_product_item_emitted = 0
        _sync_without_begin_log(self, context)

    def _write_record_count_log(self, record_count: int, context: Optional[dict] = None) -> None:
        _write_record_count_log_no_extra_tags(self, record_count, context)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        if context is None:
            self._last_batch_product_item_emitted = 0
            yield from ()
            return
        parent = self._tap.streams.get("Product")  # type: ignore[union-attr]
        if not parent:
            self._last_batch_product_item_emitted = 0
            yield from ()
            return
        items_by_id = getattr(parent, "_pending_items_by_id", None) or {}
        emitted = 0
        for link in getattr(parent, "_pending_product_item_links", None) or []:
            tid = link.get("targetEntityId")
            sid = link.get("sourceEntityId")
            if tid is None or sid is None:
                continue
            tid_i = int(tid)
            if tid_i not in items_by_id:
                continue
            emitted += 1
            yield {"product_id": int(sid), **items_by_id[tid_i]}
        self._last_batch_product_item_emitted = emitted

    def _sync_records(self, context: Optional[dict] = None) -> None:
        record_count = 0
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)

            for record in self.get_records(current_context):
                for key, val in (state_partition_context or {}).items():
                    if key not in record:
                        record[key] = val
                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex
                record_count += 1
                partition_record_count += 1

            if self.child_streams and any(
                cs.selected or cs.has_selected_descendents for cs in self.child_streams
            ):
                self._sync_children(
                    {"item_ids": list(current_context.get("item_ids") or [])}
                    if isinstance(current_context, dict)
                    else {}
                )

            if current_context == state_partition_context:
                finalize_state_progress_markers(state)

        if not context:
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        self._write_state_message()


ProductItemStream.parent_stream_type = ProductStream


class ItemSizeStream(Stream):
    """Child of ProductItemStream: Size rows with item_id (source item); one bulk fetchdata for sizes."""

    parent_stream_type: Optional[Type[Stream]] = None
    primary_keys = ["item_id", "entity_id"]

    def __init__(
        self,
        tap: Any,
        *,
        schema: dict,
        size_fieldtypes: List[dict] | None = None,
    ) -> None:
        self._size_key_map = build_fieldtype_key_map(size_fieldtypes or [])
        super().__init__(tap=tap, name="ItemSize", schema=schema)
        self.state_partitioning_keys = None

    @property
    def client(self) -> InRiverClient:
        return self._tap._inriver_client  # type: ignore[attr-defined]

    def sync(self, context: Optional[dict] = None) -> None:
        self.state_partitioning_keys = None
        self._last_batch_item_size_links = 0
        self._last_batch_item_size_emitted = 0
        _sync_without_begin_log(self, context)

    def _write_record_count_log(self, record_count: int, context: Optional[dict] = None) -> None:
        _write_record_count_log_no_extra_tags(self, record_count, context)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        if context is None:
            self._last_batch_item_size_links = 0
            self._last_batch_item_size_emitted = 0
            yield from ()
            return
        item_ids = context.get("item_ids") or []
        all_links: List[dict] = []
        size_ids: set[int] = set()
        for item_id in item_ids:
            for link in self.client.get_links(int(item_id), LINK_ITEM_SIZE, "outbound"):
                all_links.append(link)
                tid = link.get("targetEntityId")
                if tid is not None:
                    size_ids.add(int(tid))
        self._last_batch_item_size_links = len(all_links)
        sizes_by_id: Dict[int, dict[str, Any]] = {}
        if size_ids:
            size_rows = self.client.post_json(
                "/api/v1.0.1/entities:fetchdata",
                {"entityIds": sorted(size_ids), "objects": FETCHDATA_OBJECTS},
            )
            if isinstance(size_rows, list):
                for row in size_rows:
                    eid = row.get("entityId")
                    if eid is None:
                        continue
                    sizes_by_id[int(eid)] = _flatten_entity_row(row, self._size_key_map)
        emitted = 0
        for link in all_links:
            tid = link.get("targetEntityId")
            sid = link.get("sourceEntityId")
            if tid is None or sid is None:
                continue
            tid_i = int(tid)
            if tid_i not in sizes_by_id:
                continue
            emitted += 1
            yield {"item_id": int(sid), **sizes_by_id[tid_i]}
        self._last_batch_item_size_emitted = emitted


ItemSizeStream.parent_stream_type = ProductItemStream
