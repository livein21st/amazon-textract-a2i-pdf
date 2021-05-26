# /*
#  * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  * SPDX-License-Identifier: MIT-0
#  *
#  * Permission is hereby granted, free of charge, to any person obtaining a copy of this
#  * software and associated documentation files (the "Software"), to deal in the Software
#  * without restriction, including without limitation the rights to use, copy, modify,
#  * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
#  * permit persons to whom the Software is furnished to do so.
#  *
#  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
#  * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
#  * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#  * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
#  * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
#  * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#  */


import json
import boto3
import botocore
import os
import logging
import logging

from botocore.utils import merge_dicts


def does_exist(bucket, key):
    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        return False
    else:
        return True


def write_data_to_bucket(payload, name, csv):
    dest = "wip/" + payload["id"] + "/csv/" + name.replace(".png", ".csv")
    s3 = boto3.resource('s3')
    s3.Object(payload["bucket"], dest).put(Body=csv)
    return dest


def get_data_from_bucket(bucket, key):
    client = boto3.client('s3')
    response = client.get_object(
        Bucket=bucket,
        Key=key
    )
    return json.load(response["Body"])


# Create JSON


def create_json(base_image_keys, payload):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    merge_list = []
    single_dict = {}

    try:
        for base_key in base_image_keys:
            if does_exist(payload["bucket"], base_key + "/ai/output.json") and does_exist(payload["bucket"], base_key + "/human/output.json"):
                temp_ai_data = get_data_from_bucket(
                    payload["bucket"], base_key + "/ai/output.json")
                temp_human_data = get_data_from_bucket(
                    payload["bucket"], base_key + "/human/output.json")
                temp_ai_data.extend(temp_human_data)
                for dict in temp_ai_data:
                    if dict not in merge_list:
                        merge_list.append(dict)

        logger.info("INTERNAL_LOGGING: ai_human_output:" +
                    json.dumps(merge_list))

        for dict in merge_list:
            single_dict.update(dict)
        jsonOutput = json.dumps(single_dict)
        logger.info("INTERNAL_LOGGING: json" + jsonOutput)

        return jsonOutput

    except:
        logger.info(
            "INTERNAL_ERROR: Error on create_json()")
        raise


def create_csv(kv_list, give_type):
    csv_output = ""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    for item in kv_list:
        csv_output += item["key"].replace(",", "") + "," + \
            item["value"].replace(",", "") + "," + give_type + "\n"
    return csv_output


def curate_data(base_image_keys, payload):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    csv_data = ""

    try:
        for base_key in base_image_keys:
            page_number = base_key[base_key.rfind("/")+1:]
            page_number = "page " + \
                str(int(page_number[:page_number.find(".")]) + 1)

            csv_data += page_number + ",-,-" + "\n"

            if does_exist(payload["bucket"], base_key + "/ai/output.json"):
                temp_data = get_data_from_bucket(
                    payload["bucket"], base_key + "/ai/output.json")
                logger.info(
                    "INTERNAL_LOGGING: ai_output:" + json.dumps(temp_data))
                csv_data += create_csv(temp_data, "ai")

            if does_exist(payload["bucket"], base_key + "/human/output.json"):
                temp_data = get_data_from_bucket(
                    payload["bucket"], base_key + "/human/output.json")
                logger.info(
                    "INTERNAL_LOGGING: human_output:" + json.dumps(temp_data))
                csv_data += create_csv(temp_data, "human")

        return csv_data
    except:
        logger.info(
            "INTERNAL_ERROR: Error on curate_data()")
        raise


def get_base_image_keys(bucket, keys):
    temp = []
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        for key in keys:
            if "/human/output.json" in key:
                temp.append(key[:key.rfind("/human/output.json")])
            if "/ai/output.json" in key:
                temp.append(key[:key.rfind("/ai/output.json")])
        logger.info("INTERNAL_LOGGING: base_image_keys:" +
                    json.dumps(list(dict.fromkeys(temp)), indent=3, default=str))
        return list(dict.fromkeys(temp))
    except:
        logger.info(
            "INTERNAL_ERROR: Error when running get_base_image_keys()")
        raise


def get_all_possible_files(event):
    files = []
    payload = {}

    payload["bucket"] = event["bucket"]
    payload["id"] = event["id"]
    payload["key"] = event["key"]

    for item in event["image_keys"]:
        if item == "single_image":
            base_key = "wip/" + payload["id"] + "/0.png"
        else:
            base_key = "wip/" + payload["id"] + "/" + item + ".png"

        possible_ai_output_key = base_key + "/ai/output.json"
        possible_human_output_key = base_key + "/human/output.json"

        s3 = boto3.resource('s3')

        try:
            s3.Object(event["bucket"], possible_ai_output_key).load()
            files.append(possible_ai_output_key)
        except botocore.exceptions.ClientError as e:
            pass

        try:
            s3.Object(event["bucket"], possible_human_output_key).load()
            files.append(possible_human_output_key)
        except botocore.exceptions.ClientError as e:
            pass

    return files, payload


def gather_and_combine_data(event):
    keys, payload = get_all_possible_files(event)
    base_image_keys = get_base_image_keys(payload["bucket"], keys)
    base_image_keys.sort()
    jsonData = create_json(base_image_keys, payload)
    #csv_data = curate_data(base_image_keys, payload)

    return jsonData, payload
