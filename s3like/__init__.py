import io
import json
import os
import uuid
import zipfile
import time
from collections import namedtuple

try:
    import boto3
except ImportError:
    boto3 = None

import requests
from marshmallow import Schema, fields, validate


__version__ = "1.4.0"


OBJ_STORAGE_ACCESS = os.environ.get("OBJ_STORAGE_ACCESS", None)
OBJ_STORAGE_SECRET = os.environ.get("OBJ_STORAGE_SECRET", None)
OBJ_STORAGE_ENDPOINT = os.environ.get("OBJ_STORAGE_ENDPOINT", None)
OBJ_STORAGE_EDGE = os.environ.get("OBJ_STORAGE_EDGE", None)
OBJ_STORAGE_BUCKET = os.environ.get("OBJ_STORAGE_BUCKET", None)


class Serializer:
    def __init__(self, ext):
        self.ext = ext

    def serialize(self, data):
        return data

    def deserialize(self, data):
        return data


class JSONSerializer(Serializer):
    def serialize(self, data):
        return json.dumps(data).encode()

    def deserialize(self, data):
        return json.loads(data.decode())


class TextSerializer(Serializer):
    def serialize(self, data):
        return data.encode()

    def deserialize(self, data):
        return data.decode()


def get_serializer(media_type):
    return {
        "bokeh": JSONSerializer("json"),
        "table": TextSerializer("html"),
        "CSV": TextSerializer("csv"),
        "PNG": Serializer("png"),
        "JPEG": Serializer("jpeg"),
        "MP3": Serializer("mp3"),
        "MP4": Serializer("mp4"),
        "HDF5": Serializer("h5"),
    }[media_type]


class Output:
    """Output mixin shared among LocalOutput and RemoteOutput"""

    title = fields.Str()
    media_type = fields.Str(
        validate=validate.OneOf(choices=["bokeh", "table", "CSV", "PNG", "JPEG", "MP3", "MP4", "HDF5"])
    )


class RemoteOutput(Output, Schema):
    filename = fields.Str()


class RemoteOutputCategory(Schema):
    outputs = fields.Nested(RemoteOutput, many=True)
    ziplocation = fields.Str()


class RemoteResult(Schema):
    """Serializer for load_from_S3like"""

    renderable = fields.Nested(RemoteOutputCategory, required=False)
    downloadable = fields.Nested(RemoteOutputCategory, required=False)


class LocalOutput(Output, Schema):
    # Data could be a string or dict. It depends on the media type.
    data = fields.Field()


class LocalResult(Schema):
    """Serializer for load_to_S3like"""

    renderable = fields.Nested(LocalOutput, many=True)
    downloadable = fields.Nested(LocalOutput, many=True)


def write_to_s3like(task_id, loc_result, do_upload=True):
    s = time.time()
    LocalResult().load(loc_result)
    session = boto3.session.Session()
    client = session.client(
        "s3",
        endpoint_url=OBJ_STORAGE_ENDPOINT,
        aws_access_key_id=OBJ_STORAGE_ACCESS,
        aws_secret_access_key=OBJ_STORAGE_SECRET,
    )
    rem_result = {}
    for category in ["renderable", "downloadable"]:
        buff = io.BytesIO()
        zipfileobj = zipfile.ZipFile(buff, mode="w")
        ziplocation = f"{task_id}_{category}.zip"
        rem_result[category] = {"ziplocation": ziplocation, "outputs": []}
        for output in loc_result[category]:
            serializer = get_serializer(output["media_type"])
            ser = serializer.serialize(output["data"])
            filename = output["title"]
            if not filename.endswith(f".{serializer.ext}"):
                filename += f".{serializer.ext}"
            zipfileobj.writestr(filename, ser)
            rem_result[category]["outputs"].append(
                {
                    "title": output["title"],
                    "media_type": output["media_type"],
                    "filename": filename,
                }
            )
        zipfileobj.close()
        buff.seek(0)
        if do_upload:
            client.upload_fileobj(
                buff, OBJ_STORAGE_BUCKET, ziplocation, ExtraArgs={"ACL": "public-read"}
            )
    f = time.time()
    print(f"Write finished in {f-s}s")
    return rem_result


def read_from_s3like(rem_result):
    s = time.time()
    RemoteResult().load(rem_result)
    read = {"renderable": [], "downloadable": []}
    endpoint = OBJ_STORAGE_EDGE.replace("https://", "")
    base_url = f"https://{OBJ_STORAGE_BUCKET}.{endpoint}"
    for category in rem_result:
        resp = requests.get(f'{base_url}/{rem_result[category]["ziplocation"]}')
        assert resp.status_code == 200

        buff = io.BytesIO(resp.content)
        zipfileobj = zipfile.ZipFile(buff)

        for rem_output in rem_result[category]["outputs"]:
            ser = get_serializer(rem_output["media_type"])
            rem_data = ser.deserialize(zipfileobj.read(rem_output["filename"]))
            read[category].append(
                {
                    "title": rem_output["title"],
                    "media_type": rem_output["media_type"],
                    "data": rem_data,
                }
            )
    f = time.time()
    print(f"Read finished in {f-s}s")
    return read
