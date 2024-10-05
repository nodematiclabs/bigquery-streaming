from datetime import datetime, timedelta

import json
import string
import random

from google.cloud import bigquery_storage
from google.cloud.bigquery_storage import types, ProtoRows, ProtoSchema
from google.protobuf import descriptor_pb2

import occupancy_pb2

def stream_data_to_bigquery(project_id, dataset_id, table_id, rows):
    client = bigquery_storage.BigQueryWriteClient()
    parent = client.table_path(project_id, dataset_id, table_id)

    # Prepare the stream
    write_stream = types.WriteStream()
    write_stream.type = types.WriteStream.Type.COMMITTED
    stream = client.create_write_stream(
        parent=parent,
        write_stream=write_stream
    )

    # Prepare the protobuf data
    proto_rows = ProtoRows()
    for row in rows:
        proto_row = occupancy_pb2.Occupancy()
        proto_row.sensor_id = row['sensor_id']
        proto_row.occupants = row['occupants']
        proto_row.timestamp = row['timestamp']
        proto_rows.serialized_rows.append(proto_row.SerializeToString())

    # Prepare the protobuf schema
    proto_schema = ProtoSchema()
    descriptor = descriptor_pb2.DescriptorProto()
    occupancy_pb2.Occupancy.DESCRIPTOR.CopyToProto(descriptor)
    proto_schema.proto_descriptor = descriptor

    # Prepare the request
    request = types.AppendRowsRequest()
    request.write_stream = stream.name
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = proto_rows
    proto_data.writer_schema = proto_schema
    request.proto_rows = proto_data

    # Send the request
    client.append_rows(requests=iter([request]))

rows = [
    {
        "sensor_id": ''.join(random.choices(string.ascii_uppercase + string.digits, k=12)),
        "occupants": random.randint(0,9),
        "timestamp": (datetime(2024, 1, 1) + timedelta(days=random.randint(0,364))).isoformat()
    }
]
print(rows)
stream_data_to_bigquery("your-project", "iot", "occupancy", rows)