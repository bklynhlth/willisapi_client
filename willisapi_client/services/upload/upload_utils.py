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
        return asyncio.Semaphore(os.cpu_count() - 1)
    
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
    
    async def _fetch_pre_signed_part_urls(session, url, part_no, presigned_urls):
        async with session.post(url) as response:
            res = await response.json()
            if "status_code" in res and res["status_code"] == HTTPStatus.OK:
                presigned_urls[part_no] = res['presigned_url']
            else:
                presigned_urls[part_no] = ""

    async def fetch_pre_signed_part_urls(url: str, record_id: str, upload_id: str, file_path: str, headers):
        number_of_parts = UploadUtils.get_file_size_and_parts(file_path)
        tasks = []
        presigned_urls = {}
        async with UploadUtils._get_semaphore():
            async with aiohttp.ClientSession(headers=headers) as session:
                for part_no in range(1, number_of_parts + 1):
                    url_to_get_presigned_url = f"{url}?type=presigned&record_id={record_id}&upload_id={upload_id}&part_no={part_no}"
                    tasks.append(UploadUtils._fetch_pre_signed_part_urls(session, url_to_get_presigned_url, part_no, presigned_urls))
                await asyncio.gather(*tasks)
        return presigned_urls

    async def _async_upload(session, part_no, presigned_url, data, PARTS):
        try:
            async with session.put(presigned_url, data=data) as response:
                ETag = response.headers["ETag"].strip('"')
                PARTS.append({"ETag": ETag, "PartNumber": part_no})
        except Exception as ex:
            pass

    async def async_upload(nested_list, file):
        PARTS = []
        for list in nested_list:
            tasks = []
            async with UploadUtils._get_semaphore():
                async with aiohttp.ClientSession(headers={}) as session:
                    for data in list:
                        part_no, presigned_url = data
                        tasks.append(UploadUtils._async_upload(session, part_no, presigned_url, file.read(PART_SIZE), PARTS))
                    await asyncio.gather(*tasks)
        SORTED_PARTS = sorted(PARTS, key=lambda x: x['PartNumber'])
        return SORTED_PARTS

    @staticmethod
    def upload(row, url, headers):
        upload_id, record_id = UploadUtils.initiate_multi_part_upload(row, url, headers)
        if upload_id and record_id:
            urls_dict = asyncio.run(UploadUtils.fetch_pre_signed_part_urls(url, record_id, upload_id, row.file_path, headers))
            urls_list = sorted(urls_dict.items())
            if '' in urls_dict.values():
                parts = json.dumps([])
                res = requests.post(f"{url}?type=complete&record_id={record_id}&upload_id={upload_id}&number_of_parts={len(urls_list)}", json=parts, headers=headers)
                return False
            else:
                nested_list = [urls_list[i:i + 10] for i in range(0, len(urls_list), 10)] 
                file = UploadUtils.read_file(row.file_path)
                parts = asyncio.run(UploadUtils.async_upload(nested_list, file))                
                UploadUtils.close_file(file)
                parts = json.dumps(parts)
                res = requests.post(f"{url}?type=complete&record_id={record_id}&upload_id={upload_id}&number_of_parts={len(urls_list)}", json=parts, headers=headers)
                res_json = res.json()
                if "filename" in res_json and res_json["filename"]:
                    return True
                return False
        else:
            return False
