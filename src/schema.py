import re

from pydantic import BaseModel
from typing import Type, List, Optional
from pyarrow import Schema
import pyarrow as pa


class ArrowColumn(BaseModel):
    col_no: int
    col_name: str
    arrow_type: str
    col_group: str
    bit_width : Optional[int]
    precision : Optional[int]
    scale : Optional[int]
    tz: Optional[str]
    unit: Optional[str]


class ExtendedArrowSchema(BaseModel):
    columns: List[ArrowColumn]


def pa_schema_to_extended_arrow_schema(pa_schema: Type[Schema]) -> Type[ExtendedArrowSchema]:
    arrow_columns = []

    for i, col in enumerate(pa_schema):
        col_no = i + 1
        col_name = col.name
        arrow_type = str(col.type)
        bit_width = None
        precision = None
        scale = None
        tz = None
        unit = None
        if arrow_type[:3] == 'int' or arrow_type[:4] == 'uint':
            col_group = 'int'
            bit_width = col.type.bit_width
        elif arrow_type[:5] == 'float':
            col_group = 'float'
            bit_width = col.type.bit_width
        elif arrow_type[:7] == 'decimal':
            col_group = 'decimal'
            bit_width = col.type.bit_width
            precision = col.type.precision
            scale = col.type.scale
        elif arrow_type[:4] == 'date':
            col_group = 'date'
            bit_width = col.type.bit_width
        elif arrow_type[:9] == 'timestamp':
            col_group = 'timestamp'
            bit_width = col.type.bit_width
            tz = col.type.tz
            unit = col.type.unit
        elif arrow_type[:4] == 'time':
            col_group = 'time'
            bit_width = col.type.bit_width
            unit = col.type.unit
        elif arrow_type == 'duration':
            col_group = 'duration'
            bit_width = col.type.bit_width
            unit = col.type.unit
        else:
            col_group = arrow_type

        column = ArrowColumn(
            col_no=col_no,
            col_name=col_name,
            arrow_type=arrow_type,
            col_group=col_group,
            bit_width=bit_width,
            precision=precision,
            scale=scale,
            tz=tz,
            unit=unit
        )

        arrow_columns.append(column)

    return ExtendedArrowSchema(columns= arrow_columns)


def enlarge_pa_schema(pa_schema: Type[Schema]) -> Type[Schema]:
    new_pa_schema = []
    for col in pa_schema:
        if str(col.type)[:3] == 'int':
            new_pa_schema.append((col.name, pa.int64()))
        elif str(col.type)[:4] == 'uint':
            new_pa_schema.append((col.name, pa.uint64()))
        elif str(col.type)[:5] == 'float':
            new_pa_schema.append((col.name, pa.float64()))
        elif str(col.type)[:10] == 'decimal128':
            new_pa_schema.append((col.name, pa.decimal128(38, col.type.scale)))
        else:
            new_pa_schema.append(col)

    return pa.schema(new_pa_schema)


def schema_type_to_pa_field(field_name: str, field_type: str) -> pa.Field:
    if field_type[:3] == 'int'or field_type[:4] == 'uint':
        return pa.field(field_name, pa.int64())
    elif field_type[:5] == 'float':
        return pa.field(field_name, pa.float64())
    elif field_type[:7] == 'decimal':
        precision = int(re.search('\((.*)\,', field_type).group(1))
        scale = int(re.search('\,(.*)\)', field_type).group(1))
        return pa.field(field_name, pa.decimal256(precision, scale))
    elif field_type[:9] == 'timestamp':
        unit = re.search('\[(.*)\]', field_type).group(1)
        return pa.field(field_name, pa.timestamp(unit))
    elif field_type[:4] == 'time':
        unit = re.search('\[(.*)\]', field_type).group(1)
        return pa.field(field_name, pa.time(unit))
    elif field_type[:4] == 'date':
        return pa.field(field_name, pa.date64())
    elif field_type[:8] == 'duration':
        unit = re.search('\[(.*)\]', field_type).group(1)
        return pa.field(field_name, pa.duration(unit))
    elif field_type[:6] == 'binary':
        return pa.field(field_name, pa.binary())
    elif field_type[:6] == 'string':
        return pa.field(field_name, pa.string())


def schema_to_pa_schema(schema: dict) -> pa.Schema:
    fields = []
    for key, value in schema.items():
        fields.append(schema_type_to_pa_field(key, value))

    return pa.schema(fields)