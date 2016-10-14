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


import json
import sys

def openFile(fileName):
    f = open(fileName)
    data = f.read()
    json_data = json.loads(data)

    return json_data

def createLog(datas):
    message = "{0:70}{1:30}{2:20}\n".format("Organizations","UUIDs","Number of identities")
    for data in datas:
        value = [data['org'], data['uuid'], data['n_identities']]
        message += "{0:<50}{1:^50}{2:^20}\n".format(*value)

    return message

if __name__ == "__main__":
    data = openFile('data.json')

    if len(data) == 0:
        success = 0
        print("The all number of identities are less than 25")
    else:
        success = -1
        log = createLog(data)
        print(log)

    sys.exit(success)
