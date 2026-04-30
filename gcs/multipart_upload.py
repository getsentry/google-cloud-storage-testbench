# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Helpers for GCS S3-compatible XML multipart uploads."""

import datetime
import hashlib
import uuid
import xml.etree.ElementTree as ET

import gcs.object
import testbench
import testbench.common
from gcs.upload import Upload

MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MiB


def _create_upload_id(bucket_name, object_name):
    return hashlib.sha256(
        ("%s/%s/o/%s" % (uuid.uuid4().hex, bucket_name, object_name)).encode("utf-8")
    ).hexdigest()


def init_xml_multipart(request, bucket, object_name):
    """Create an Upload representing an S3-style multipart upload.

    Captures metadata from request headers at initiate time; parts are
    accumulated later via xml_upload_part.
    """
    upload_id = _create_upload_id(bucket.name, object_name)
    metadata = {
        "bucket": bucket.name,
        "name": object_name,
        "metadata": {"x_emulator_upload": "xml_multipart"},
    }
    headers = request.headers
    if "content-type" in headers:
        metadata["contentType"] = headers["content-type"]
    if headers.get("content-encoding"):
        metadata["contentEncoding"] = headers["content-encoding"]
    if headers.get("content-disposition"):
        metadata["contentDisposition"] = headers["content-disposition"]
    if headers.get("content-language"):
        metadata["contentLanguage"] = headers["content-language"]
    elif headers.get("x-goog-content-language"):
        metadata["contentLanguage"] = headers["x-goog-content-language"]
    if headers.get("cache-control"):
        metadata["cacheControl"] = headers["cache-control"]
    if headers.get("x-goog-storage-class"):
        metadata["storageClass"] = headers["x-goog-storage-class"]

    # Collect x-goog-meta-* custom metadata
    custom = metadata["metadata"]
    for key, value in headers.items():
        lower = key.lower()
        if lower.startswith("x-goog-meta-"):
            custom[lower[len("x-goog-meta-") :]] = value

    fake_request = testbench.common.FakeRequest.init_xml(request)
    predefined_acl = headers.get("x-goog-acl")
    if predefined_acl:
        fake_request.args["predefinedAcl"] = predefined_acl
    preconditions = testbench.common.make_xml_preconditions(fake_request)

    upload = Upload(
        request=fake_request,
        metadata=metadata,
        bucket=bucket,
        location="",
        upload_id=upload_id,
        media=b"",
        complete=False,
        transfer=set(),
        parts={},
        kind="xml_multipart",
    )
    upload.preconditions = preconditions
    return upload


# --- ETag helpers ---


def compute_part_etag(data):
    return '"%s"' % hashlib.md5(data).hexdigest()


def compute_multipart_etag(part_etags_in_order):
    """Compute the S3-style composite ETag from per-part ETags.

    Each element of *part_etags_in_order* is a quoted hex-md5 string
    (e.g. '"abc123..."').  The composite ETag is the MD5 of the
    concatenated *binary* MD5 digests, suffixed with ``-N``.
    """
    binary = b""
    for etag in part_etags_in_order:
        hex_str = etag.strip('"')
        binary += bytes.fromhex(hex_str)
    composite = hashlib.md5(binary).hexdigest()
    return '"%s-%d"' % (composite, len(part_etags_in_order))


# --- XML response builders ---

_XML_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _xml_preamble():
    return '<?xml version="1.0" encoding="UTF-8"?>\n'


def _el(tag, text):
    e = ET.Element(tag)
    e.text = str(text)
    return e


def build_initiate_response_xml(bucket, key, upload_id):
    root = ET.Element("InitiateMultipartUploadResult", xmlns=_XML_NS)
    root.append(_el("Bucket", bucket))
    root.append(_el("Key", key))
    root.append(_el("UploadId", upload_id))
    return (_xml_preamble() + ET.tostring(root, encoding="unicode")).encode("utf-8")


def build_complete_response_xml(location, bucket, key, etag):
    root = ET.Element("CompleteMultipartUploadResult", xmlns=_XML_NS)
    root.append(_el("Location", location))
    root.append(_el("Bucket", bucket))
    root.append(_el("Key", key))
    root.append(_el("ETag", etag))
    return (_xml_preamble() + ET.tostring(root, encoding="unicode")).encode("utf-8")


def build_list_parts_response_xml(
    bucket,
    key,
    upload_id,
    parts_page,
    part_number_marker,
    next_marker,
    max_parts,
    is_truncated,
):
    """Build ListPartsResult XML.

    *parts_page* is a list of (part_number, last_modified_datetime, etag, size).
    """
    root = ET.Element("ListPartsResult", xmlns=_XML_NS)
    root.append(_el("Bucket", bucket))
    root.append(_el("Key", key))
    root.append(_el("UploadId", upload_id))
    root.append(_el("PartNumberMarker", part_number_marker))
    if next_marker is not None:
        root.append(_el("NextPartNumberMarker", next_marker))
    root.append(_el("MaxParts", max_parts))
    root.append(_el("IsTruncated", "true" if is_truncated else "false"))
    for part_number, last_modified, etag, size in parts_page:
        part_el = ET.SubElement(root, "Part")
        part_el.append(_el("PartNumber", part_number))
        part_el.append(
            _el("LastModified", last_modified.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
        )
        part_el.append(_el("ETag", etag))
        part_el.append(_el("Size", size))
    return (_xml_preamble() + ET.tostring(root, encoding="unicode")).encode("utf-8")


# --- XML request parsing ---


def parse_complete_request_xml(body):
    """Parse a CompleteMultipartUpload XML body.

    Returns [(part_number: int, etag: str), ...] in document order.
    Tolerates the optional S3 namespace.
    """
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        testbench.error.invalid("CompleteMultipartUpload XML", context=None)
    # Handle optional namespace
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"
    parts = []
    for part in root.findall(ns + "Part"):
        part_number_text = part.findtext(ns + "PartNumber")
        try:
            pn = int(part_number_text)
        except (TypeError, ValueError):
            testbench.error.invalid("CompleteMultipartUpload XML", context=None)
        etag = part.findtext(ns + "ETag")
        parts.append((pn, etag))
    return parts


# --- Validation ---


def validate_min_part_size(requested_parts, upload):
    """Raise 400 if any non-final part is smaller than MIN_PART_SIZE."""
    for i, (part_number, _etag) in enumerate(requested_parts):
        is_last = i == len(requested_parts) - 1
        if is_last:
            break
        part = upload.parts.get(part_number)
        if part is None:
            testbench.error.invalid("Part %d not uploaded" % part_number, context=None)
        if len(part["data"]) < MIN_PART_SIZE:
            testbench.error.generic(
                "EntityTooSmall: Part %d is %d bytes, minimum is %d"
                % (part_number, len(part["data"]), MIN_PART_SIZE),
                400,
                None,
                None,
            )


def validate_requested_parts(requested_parts, upload):
    """Raise 400 if the completion manifest does not match uploaded parts."""
    for part_number, requested_etag in requested_parts:
        part = upload.parts.get(part_number)
        if part is None:
            testbench.error.invalid("Part %d not uploaded" % part_number, context=None)
        actual_etag = part["etag"]
        if requested_etag != actual_etag:
            testbench.error.generic(
                "InvalidPart: Part %d ETag %s does not match uploaded part %s"
                % (part_number, requested_etag, actual_etag),
                400,
                None,
                None,
            )


def validate_requested_part_order(requested_parts):
    """Raise 400 if the completion manifest is not strictly ascending."""
    last_part_number = None
    for part_number, _requested_etag in requested_parts:
        if last_part_number is not None and part_number <= last_part_number:
            testbench.error.generic("InvalidPartOrder", 400, None, None)
        last_part_number = part_number


def validate_requested_parts_not_empty(requested_parts):
    """Raise 400 if the completion manifest does not include any parts."""
    if not requested_parts:
        testbench.error.generic(
            "InvalidRequest: CompleteMultipartUpload requires at least one part",
            400,
            None,
            None,
        )


# --- Finalization ---


def finalize_multipart(upload, requested_parts):
    """Assemble parts into a finalized Object.

    *requested_parts* comes from parse_complete_request_xml.
    Returns (blob, multipart_etag_str).
    """
    validate_requested_parts_not_empty(requested_parts)
    validate_requested_part_order(requested_parts)
    validate_min_part_size(requested_parts, upload)
    validate_requested_parts(requested_parts, upload)

    media = b""
    etags_in_order = []
    for part_number, _etag in requested_parts:
        part = upload.parts.get(part_number)
        media += part["data"]
        etags_in_order.append(part["etag"])

    multipart_etag = compute_multipart_etag(etags_in_order)

    metadata = dict(upload.metadata)
    metadata["metadata"] = dict(metadata.get("metadata", {}))
    metadata["metadata"]["x_emulator_upload"] = "xml_multipart"

    blob, _ = gcs.object.Object.init_dict(
        upload.request, metadata, media, upload.bucket, False
    )
    return blob, multipart_etag
