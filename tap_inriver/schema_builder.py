"""Build Singer JSON Schema from inRiver field type definitions."""

from __future__ import annotations

from typing import Any, List

from hotglue_singer_sdk import typing as th

from tap_inriver.client import field_type_id_to_snake


def _property_for_datatype(data_type: str | None, is_multi_value: bool) -> th.JSONTypeHelper:
    """Map inRiver dataType string to SDK typing helper."""
    dt = (data_type or "String").strip()
    if is_multi_value:
        inner: th.JSONTypeHelper
        if dt in ("String", "LocaleString", "Xml"):
            inner = th.StringType
        elif dt == "Boolean":
            inner = th.BooleanType
        elif dt in ("Integer",):
            inner = th.IntegerType
        elif dt in ("Double",):
            inner = th.NumberType
        elif dt == "DateTime":
            inner = th.DateTimeType
        else:
            inner = th.StringType
        return th.ArrayType(inner)

    if dt in ("String", "Xml", "File"):
        return th.StringType
    if dt == "LocaleString":
        return th.CustomType(
            {"type": "object", "additionalProperties": {"type": ["string", "null"]}}
        )
    if dt == "Boolean":
        return th.BooleanType
    if dt == "Integer":
        return th.IntegerType
    if dt == "Double":
        return th.NumberType
    if dt == "DateTime":
        return th.DateTimeType
    if dt.startswith("CVL"):
        return th.StringType
    return th.CustomType({"type": ["object", "array", "string", "number", "integer", "boolean", "null"]})


def fieldtypes_to_properties(fieldtypes: List[dict]) -> List[th.Property]:
    """Convert GET .../fieldtypes array to Property list (snake_case names)."""
    props: List[th.Property] = []
    for ft in fieldtypes:
        fid = ft.get("id") or ""
        snake = field_type_id_to_snake(fid) if fid else "unknown_field"
        dt = ft.get("dataType")
        multi = bool(ft.get("isMultiValue"))
        props.append(th.Property(snake, _property_for_datatype(dt, multi)))
    return props


def entity_stream_schema(fieldtypes: List[dict]) -> dict:
    """Schema for entity streams: entity_id, summary fields, then field types."""
    summary_props = [
        th.Property("entity_id", th.IntegerType, required=True),
        th.Property("entity_type_id", th.StringType, required=False),
        th.Property("display_name", th.StringType, required=False),
        th.Property("display_description", th.StringType, required=False),
        th.Property("modified_date", th.DateTimeType, required=False),
        th.Property("created_date", th.DateTimeType, required=False),
        th.Property("completeness", th.IntegerType, required=False),
        th.Property("field_set_id", th.StringType, required=False),
        th.Property("segment_id", th.IntegerType, required=False),
        th.Property("media", th.ArrayType(th.StringType), required=False),
    ]
    dynamic = fieldtypes_to_properties(fieldtypes)
    return th.PropertiesList(*summary_props, *dynamic).to_dict()


def entity_with_parent_id_schema(parent_field: str, target_entity_fieldtypes: List[dict]) -> dict:
    """Child streams: parent FK (product_id / item_id) plus target entity shape from `entity_stream_schema`."""
    parent_schema = th.PropertiesList(
        th.Property(parent_field, th.IntegerType, required=True),
    ).to_dict()
    ent_schema = entity_stream_schema(target_entity_fieldtypes)
    pp = parent_schema.get("properties") or {}
    ep = ent_schema.get("properties") or {}
    merged_props = {**pp, **ep}
    req = sorted(set(parent_schema.get("required", []) or []) | set(ent_schema.get("required", []) or []))
    out: dict[str, Any] = {"type": "object", "properties": merged_props}
    if req:
        out["required"] = list(req)
    return out


def merge_fieldvalue_dict(
    field_values: List[dict] | None,
    key_snake: dict[str, str],
) -> dict[str, Any]:
    """Flatten fieldValues array to snake_case keys using mapping fieldTypeId -> snake."""
    out: dict[str, Any] = {}
    for fv in field_values or []:
        ftid = fv.get("fieldTypeId")
        if not ftid:
            continue
        sk = key_snake.get(ftid) or field_type_id_to_snake(str(ftid))
        out[sk] = fv.get("value")
    return out


def build_fieldtype_key_map(fieldtypes: List[dict]) -> dict[str, str]:
    """fieldTypeId -> snake_case property name."""
    return {str(ft["id"]): field_type_id_to_snake(str(ft["id"])) for ft in fieldtypes if ft.get("id")}
