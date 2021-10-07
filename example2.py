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
# The timeout is in seconds.
dbm = tkrzw_rpc.RemoteDBM()
status = dbm.Connect("localhost:1978", 10)
if not status.IsOK():
    raise tkrzw_rpc.StatusException(status)

# Sets the index of the database to operate.
# The default value 0 means the first database on the server.
# 1 means the second one and 2 means the third one, if any.
dbm.SetDBMIndex(0).OrDie()

# Sets records.
# The method OrDie raises a runtime error on failure.
dbm.Set(1, "hop").OrDie()
dbm.Set(2, "step").OrDie()
dbm.Set(3, "jump").OrDie()

# Retrieves records without checking errors.
# On failure, the return value is None.
print(dbm.GetStr(1))
print(dbm.GetStr(2))
print(dbm.GetStr(3))
print(dbm.GetStr(4))

# To know the status of retrieval, give a status object to Get.
# You can compare a status object and a status code directly.
status = tkrzw_rpc.Status()
value = dbm.GetStr(1, status)
print("status: " + str(status))
if status == tkrzw_rpc.Status.SUCCESS:
    print("value: " + value)

# Rebuilds the database.
# Optional parameters compatible with the database type can be given.
dbm.Rebuild().OrDie()

# Traverses records with an iterator.
iter = dbm.MakeIterator()
iter.First()
while True:
    status = tkrzw_rpc.Status()
    record = iter.GetStr(status)
    if not status.IsOK():
        break
    print(record[0], record[1])
    iter.Next()

# Closes the connection.
dbm.Disconnect().OrDie()

# END OF FILE
