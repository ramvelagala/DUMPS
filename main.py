"""Avro Schema Parsing and dump to JSON."""


import json
import pathlib
import avro.schema


file = pathlib.Path(__file__).parent / 'asset' / 'schema.avsc'
dir_path = pathlib.Path(__file__).parent / 'asset'


def read_avsc(filepath):
    """Read avro schema and return avro object.

    return avro schema
    """
    schema = avro.schema.parse(open(filepath / "_sk_result.avsc", "rb").read())
    return schema


data = read_avsc(dir_path)
print(json.dumps(data.to_json(), separators=(':', ',')))
