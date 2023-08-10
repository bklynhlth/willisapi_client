import os
import math
import pathlib
import requests
from http import HTTPStatus
import asyncio
import copy
import aiohttp
import json
from itertools import zip_longest
import time

from typing import List, Mapping

from willisapi_client.services.upload.utils import MB

# AWS Limit
MAX_NUMBER_OF_PARTS = 10000

PART_SIZE = 40 * MB

class UploadUtils:
    def _get_semaphore():
        return asyncio.Semaphore(os.cpu_count() - 2)
    
    @staticmethod
    def read_file(file_path: str):
        return open(file_path, 'rb')
    
    @staticmethod
    def close_file(file):
        file.close()

    @staticmethod
    def get_file_size_and_parts(file_path):
        total_bytes = os.stat(file_path).st_size
        number_of_parts = math.ceil(total_bytes / PART_SIZE)
        return number_of_parts
    
    def initiate_multi_part_upload(row, url, headers):
        data = dict(project_name=row.project_name, workflow_tags=row.workflow_tags, pt_id_external=row.pt_id_external, filename=pathlib.Path(row.file_path).name)
        if row.time_collected:
            data["time_collected"] = row.time_collected
        if row.data_type:
            data["data_type"] = row.data_type
        
        try:
            response = requests.post(f"{url}?type=initiate", json=data, headers=headers)
            res_json = response.json() 
        except Exception as ex:
            pass
        else:
            if "status_code" in res_json:
                if res_json["status_code"] == HTTPStatus.OK:
                    return (res_json["upload_id"], res_json["record_id"])
                if res_json["status_code"] == HTTPStatus.BAD_REQUEST or res_json["status_code"] == HTTPStatus.INTERNAL_SERVER_ERROR:
                    print("Something went wrong")
            else:
                if 'message' in res_json and res_json['message'] == 'Unauthorized':
                    print("Your Key is expired. Login again to generate a new key")                
            return (None, None)
    
    def fetch_pre_signed_part_urls(url: str, record_id: str, upload_id: str, file_path: str, headers):
        try:
            number_of_parts = UploadUtils.get_file_size_and_parts(file_path)
            url_to_get_presigned_url = f"{url}?type=presigned&record_id={record_id}&upload_id={upload_id}&number_of_parts={number_of_parts}"
            response = requests.post(url_to_get_presigned_url, headers=headers)
            res_json = response.json()
        except Exception as ex:
            pass
        else:
            if "status_code" in res_json:
                if res_json["status_code"] == HTTPStatus.OK:
                    return res_json["presigned_url"], number_of_parts
            else:
                if 'message' in res_json and res_json['message'] == 'Unauthorized':
                    print("Your Key is expired. Login again to generate a new key")                
            return None, number_of_parts

    async def _async_upload(session, part_no, presigned_url, data, PARTS):
        try:
            async with session.put(presigned_url, data=data) as response:
                ETag = response.headers["ETag"].strip('"')
                PARTS.append({"ETag": ETag, "PartNumber": part_no})
        except Exception as ex:
            pass

    async def async_upload(presigned_urls, file):
        PARTS = []
        tasks = []
        async with UploadUtils._get_semaphore():
            async with aiohttp.ClientSession(headers={}) as session:
                for urls in presigned_urls:
                    part_no, presigned_url = urls["PartNumber"], urls["PresignedURL"]
                    tasks.append(UploadUtils._async_upload(session, part_no, presigned_url, file.read(PART_SIZE), PARTS))
                await asyncio.gather(*tasks)
        SORTED_PARTS = sorted(PARTS, key=lambda x: x['PartNumber'])
        return SORTED_PARTS

    @staticmethod
    def upload(row, url, headers):
        (upload_id, record_id) = UploadUtils.initiate_multi_part_upload(row, url, headers)
        uploaded = False
        if upload_id and record_id:
            presigned_urls, number_of_parts = UploadUtils.fetch_pre_signed_part_urls(url, record_id, upload_id, row.file_path, headers)
            if presigned_urls:
                if len(presigned_urls) != number_of_parts:
                    parts = json.dumps([])
                    res = requests.post(f"{url}?type=complete&record_id={record_id}&upload_id={upload_id}&number_of_parts={number_of_parts}", json=parts, headers=headers)
                else:
                    file = UploadUtils.read_file(row.file_path)
                    parts = asyncio.run(UploadUtils.async_upload(presigned_urls, file))                
                    UploadUtils.close_file(file)
                    parts = json.dumps(parts)
                    res = requests.post(f"{url}?type=complete&record_id={record_id}&upload_id={upload_id}&number_of_parts={number_of_parts}", json=parts, headers=headers)
                    res_json = res.json()
                    if "filename" in res_json and res_json["filename"]:
                        uploaded = True
        return uploaded