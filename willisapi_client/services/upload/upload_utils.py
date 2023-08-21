import os
import math
import pathlib
import requests
from http import HTTPStatus
import asyncio
import aiohttp
import nest_asyncio
import json
from typing import Tuple
from datetime import datetime

from willisapi_client.services.upload.utils import MB
from willisapi_client.logging_setup import logger as logger

# AWS Limit
MAX_NUMBER_OF_PARTS = 10000

PART_SIZE = 40 * MB

class UploadUtils:
    def _get_semaphore() -> int:
        """
        This function returns the semaphone count for async upload

        Returns:
            int
        """
        total_cpu_count = os.cpu_count()
        semaphore_count = total_cpu_count - 2
        if semaphore_count <= 0:
            semaphore_count = total_cpu_count 
        return asyncio.Semaphore(semaphore_count)
    
    @staticmethod
    def read_file(file_path: str):
        """
        This function reads the video/audio file and returns it in a binary format
        """
        return open(file_path, 'rb')
    
    @staticmethod
    def close_file(file):
        """
        This function closes the video/audio file
        
        Parameter:
            opened file in binary format
        """
        file.close()

    @staticmethod
    def get_file_size_and_parts(file_path: str) -> int:
        """
        This function takes the file path and returns the number of presigned urls required to upload the file to Brooklyn health S3 bucket
        
        Parameter:
            file_path

        Returns:
            int
        """
        total_bytes = os.stat(file_path).st_size
        number_of_parts = math.ceil(total_bytes / PART_SIZE)
        return number_of_parts
    
    def initiate_multi_part_upload(index: int, row, url: str, headers: dict) -> Tuple[str, str]:
        """
        This is an internal function which makes a POST API call to brooklyn.health API server to initiate multi part upload

        Parameters:
            row: a dataframe row
            url: API URL
            headers: headers

        Returns:
            upload_id and record_id
        """
        data = dict(project_name=row.project_name, workflow_tags=row.workflow_tags, pt_id_external=row.pt_id_external, filename=pathlib.Path(row.file_path).name)
        if row.time_collected:
            data["time_collected"] = row.time_collected
        
        try:
            response = requests.post(f"{url}?type=initiate", json=data, headers=headers)
            res_json = response.json() 
        except Exception as ex:
            pass
        else:
            if "status_code" in res_json:
                if res_json["status_code"] == HTTPStatus.OK:
                    if index == 0:
                        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: key check passed')
                    return (res_json["upload_id"], res_json["record_id"])
                if res_json["status_code"] == HTTPStatus.BAD_REQUEST or res_json["status_code"] == HTTPStatus.INTERNAL_SERVER_ERROR:
                    logger.error("Something went wrong")
            else:
                if 'message' in res_json and res_json['message'] == 'Unauthorized':
                    logger.error("Your Key is expired. Login again to generate a new key")                
            return (None, None)
    
    def fetch_pre_signed_part_urls(url: str, record_id: str, upload_id: str, file_path: str, headers):
        """
        This is an internal function which makes a POST API call to brooklyn.health API server to get all the presigned URLs at once

        Parameters:
            url: API URL
            record_id: str
            upload_id: str
            file_path: str
            headers: headers

        Returns:
            list of presigned urls: list
            number of parts: int
        """
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
                    logger.error("Your Key is expired. Login again to generate a new key")                
            return None, number_of_parts

    async def _async_upload(session, part_no, presigned_url, data, PARTS):
        """
        This is an async function which makes a PUT call to presigned URL
        """
        try:
            async with session.put(presigned_url, data=data) as response:
                ETag = response.headers["ETag"].strip('"')
                PARTS.append({"ETag": ETag, "PartNumber": part_no})
        except Exception as ex:
            pass

    async def async_upload(presigned_urls, file):
        """
        This is a internal function responsible for scheduling all the coroutines
        """
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
    def upload(index, row, url, headers) -> bool:
        """
        This is an internal funciton which first call the initiate_multipart_upload function, then upload the file in async way
        and finally make a POST call to brooklyn.health API server to make the upload as complete/abort
        
        Returns:
            uploaded: boolean
        """
        (upload_id, record_id) = UploadUtils.initiate_multi_part_upload(index, row, url, headers)
        uploaded = False
        if upload_id and record_id:
            presigned_urls, number_of_parts = UploadUtils.fetch_pre_signed_part_urls(url, record_id, upload_id, row.file_path, headers)
            if presigned_urls:
                if len(presigned_urls) != number_of_parts:
                    parts = json.dumps([])
                    res = requests.post(f"{url}?type=complete&record_id={record_id}&upload_id={upload_id}&number_of_parts={number_of_parts}", json=parts, headers=headers)
                else:
                    file = UploadUtils.read_file(row.file_path)
                    parts = UploadUtils.start_upload(presigned_urls, file)           
                    UploadUtils.close_file(file)
                    parts = json.dumps(parts)
                    res = requests.post(f"{url}?type=complete&record_id={record_id}&upload_id={upload_id}&number_of_parts={number_of_parts}", json=parts, headers=headers)
                    res_json = res.json()
                    #TODO: use status_code instead of filename
                    if "filename" in res_json and res_json["filename"]:
                        uploaded = True
        return uploaded
    
    def start_upload(presigned_urls, file):
        """
        This is an internal function which executes the async function responsible for uploading file asynchronously

        Returns:
            parts: List of Etags response from AWS S3 
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # 'RuntimeError: There is no current event loop...'
            loop = None
        
        if loop and loop.is_running():
            nest_asyncio.apply()
            parts = asyncio.run(UploadUtils.async_upload(presigned_urls, file))
        else:
            parts = asyncio.run(UploadUtils.async_upload(presigned_urls, file)) 
        
        return parts

    def summary_logs(number_of_files_uploaded: int, number_of_files_failed: int):
        """
        A function which logs the summary for upload
        """
        logger.info("---------------------------------------")
        logger.info("upload summary")
        logger.info("---------------------------------------")
        logger.info(f"file upload successes: {number_of_files_uploaded}")
        logger.info(f"file upload failures: {number_of_files_failed}")
