#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for basic usage of the remote database
#
# Copyright 2020 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific language governing permissions
# and limitations under the License.
#--------------------------------------------------------------------------------------------------

import tkrzw_rpc

# Prepares the database.
dbm = tkrzw_rpc.RemoteDBM()
dbm.Connect("localhost:1978")

# Sets records.
# If the operation fails, a runtime exception is raised.
# Keys and values are implicitly converted into bytes.
dbm["first"] = "hop"
dbm["second"] = "step"
dbm["third"] = "jump"

# Retrieves record values.
# If the operation fails, a runtime exception is raised.
# Retrieved values are strings if keys are strings.
print(dbm["first"])
print(dbm["second"])
print(dbm["third"])
try:
    print(dbm["fourth"])
except tkrzw_rpc.StatusException as e:
    print(repr(e))

# Checks and deletes a record.
if "first" in dbm:
    del dbm["first"]

# Traverses records.
# Retrieved keys and values are always bytes so we decode them.
for key, value in dbm:
    print(key.decode(), value.decode())

# Closes the connection.
dbm.Disconnect()

# END OF FILE
