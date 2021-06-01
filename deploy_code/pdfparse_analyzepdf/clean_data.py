# # /*
# #  * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# #  * SPDX-License-Identifier: MIT-0
# #  *
# #  * Permission is hereby granted, free of charge, to any person obtaining a copy of this
# #  * software and associated documentation files (the "Software"), to deal in the Software
# #  * without restriction, including without limitation the rights to use, copy, modify,
# #  * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# #  * permit persons to whom the Software is furnished to do so.
# #  *
# #  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# #  * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# #  * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# #  * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# #  * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# #  * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# #  */

import json
import logging


def get_child(relation, line, word):
    text = ""
    for id in relation["Ids"]:
        if id in line:
            text = line[id]
        else:
            text += " " + word[id]
    if text[0] == " ":
        text = text[1:]
    return text


def extract_value(value, kv, line, word):
    for val in value:
        try:
            text = ""
            for relation in kv[val]["Relationships"]:
                if relation["Type"] == "CHILD":
                    text = get_child(relation, line, word)
            return text
        except:
            return "UNKNOWN"


def line_up_ids(kv, line, word):
    kv_list = []
    master_values = []
    for cur in kv:
        if kv[cur]["EntityTypes"][0] == "KEY":
            value = []
            text = ""
            for relation in kv[cur]["Relationships"]:
                if relation["Type"] == "VALUE":
                    for id in relation["Ids"]:
                        value.append(id)
                if relation["Type"] == "CHILD":
                    text = get_child(relation, line, word)
            kv_list.append({
                # key: value
                text: extract_value(value, kv, line, word),
            })
    return kv_list


def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}

                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '
    return text


def generate_table_csv(table_result, blocks_map, table_index):

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    rows = get_rows_columns_map(table_result, blocks_map)
    logger.info("INTERNAL_LOGGING: rows:" +
                json.dumps(rows, indent=3, default=str))

    # get cells.

    json_object = {}
    for row_index, cols in rows.items():
        for col_index, text in cols.items():
            if len(cols) >= 3:
                json_object = rows
    logger.info("INTERNAL_LOGGING: JsonString:" +
                json.dumps(json_object, indent=3, default=str))
    return json_object


def get_table(data):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    blocks = data["Blocks"]
    blocks_map = {}
    table_blocks = []
    try:
        for block in blocks:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                table_blocks.append(block)
        if len(table_blocks) <= 0:
            return "<b> TABLE NOT FOUND </b>"

        table_data = {}
        for index, table in enumerate(table_blocks):
            table_data = generate_table_csv(table, blocks_map, index + 1)
        return table_data

    except:
        logger.info(
            "INTERNAL_ERROR: Ran into error while extracting table")
        raise


def get_key_value_set(data):
    dict_key_value = {}
    for block in data["Blocks"]:
        if block["BlockType"] == "KEY_VALUE_SET" and "Relationships" in block:
            dict_key_value[block["Id"]] = {
                "Relationships": block["Relationships"],
                "EntityTypes": block["EntityTypes"]
            }
    return dict_key_value


def get_word_and_line(data):
    dict_word = {}
    dict_line = {}
    for block in data["Blocks"]:
        if block["BlockType"] == "WORD":
            dict_word[block["Id"]] = block["Text"]
        if block["BlockType"] == "LINE":
            dict_line[block["Id"]] = block["Text"]
    return dict_word, dict_line


def extract_data(event):
    data = event
    dict_word, dict_line = get_word_and_line(data)
    dict_key_value = get_key_value_set(data)
    table = get_table(data)
    kv_list = line_up_ids(dict_key_value, dict_line, dict_word)

    return kv_list, table
