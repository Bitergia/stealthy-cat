#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Quan Zhou <quan@bitergia.com>
#


import argparse
import configparser
import json
import sys

from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch
import pandas as pd


VERSION = "version 0.1"


def write_file(filename, data):
    with open(filename, 'w+') as f:
        json.dump(data, f)

def read_arguments():
    desc="Checks the author data in Elasticsearch databases"
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=desc)

    parser.add_argument("-v", "--version",
                        action="version",
                        version=VERSION,
                        help="show program's version number and exit")
    parser.add_argument("config_file",
                        action="store",
                        help="config file")

    args = parser.parse_args()

    return args

def check_authors(conf):
    authors_pandas = []

    client = Elasticsearch('https://'+conf['user']+':'+conf['password']+'@'+conf['es'])
    s = Search(using=client, index=conf['backends'])
    for hit in s.scan():
        authors_pandas.append({"a_name": hit.author_name, "a_id": hit.author_uuid})

    df = pd.DataFrame(authors_pandas)

    return df

def get_conf(conf_name):
    Config = configparser.ConfigParser()
    Config.read(conf_name)

    conf = {}
    for project in Config.sections():
        if project != "notification":
            indexes = Config[project]['indexes'].replace(", ", ",")
            conf['backends'] = indexes.split(',')
            conf['es'] = Config[project]['es']
            conf['user'] = Config[project]['user']
            conf['password'] = Config[project]['password']

    return conf

def main():
    success = 0

    args = read_arguments()

    conf_name = args.config_file

    conf = get_conf(conf_name)

    authors_all = check_authors(conf)

    authors = authors_all.dropna()

    message = "Check author"
    message += "\nTotal : "+str(len(authors_all))
    message += "\nUnique author_name: "+str(len(authors.a_name.unique()))+"\nUnique author_uuid: "+str(len(authors.a_id.unique()))
    message += "\nauthor_name contend '@': "+str(authors[authors.a_name.str.contains("@")].a_name.count())
    #message += "\nauthor_uuid = '-': "+str(authors[authors.a_id.str.contains("-")].a_id.count())
    message += "\n\nSee the json files for more information"
    print(message)

    if len(authors.a_name.unique()) != len(authors.a_id.unique()):
        success = -1

    if len(authors[authors.a_name.str.contains("@")]) > 1:
        success = -1

    if len(authors_all[authors_all.isnull().any(axis=1)]) > 0:
        success = -1

    no_duplicates = authors_all.drop_duplicates()
    vc_name = no_duplicates.a_name.value_counts(dropna=False)
    name_duplicated = json.loads(vc_name[vc_name > 1].to_json())
    for name in name_duplicated:
        id_list =  no_duplicates[no_duplicates.a_name==name].a_id.unique().tolist()
        name_duplicated[name] = id_list
    write_file("name_duplicated.json", name_duplicated)

    vc_id = no_duplicates.a_id.value_counts(dropna=False)
    id_duplicated = json.loads(vc_id[vc_id > 1].to_json())
    for id in id_duplicated:
        name_list =  no_duplicates[no_duplicates.a_id==id].a_name.unique().tolist()
        id_duplicated[id] = name_list
    if "null" in id_duplicated:
        if len(id_duplicated["null"]) == 0:
            del id_duplicated["null"]
    write_file("id_duplicated.json", id_duplicated)

    authors_a = authors[authors.a_name.str.contains("@")].to_json()
    write_file("authors_a.json", json.loads(authors_a))

    authors_nan = authors_all[authors_all.isnull().any(axis=1)]
    id_nan_counts = authors_nan.a_id.value_counts().to_json()
    name_nan_counts = authors_nan.a_name.value_counts().to_json()
    authors_nan = {"Not_name": json.loads(id_nan_counts), "Not_id": json.loads(name_nan_counts)}
    write_file("authors_nan.json", authors_nan)

    sys.exit(success)

if __name__ == "__main__":
    main()
