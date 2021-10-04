#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Test cases
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

import math
import os
import random
import re
import shutil
import sys
import tempfile
import threading
import time
import unittest

from tkrzw_rpc import *

# Unit testing framework.
class TestTkrzw(unittest.TestCase):

  # Prepares resources.
  def setUp(self):
    tmp_prefix = "tkrzw-python-"
    self.test_dir = tempfile.mkdtemp(prefix=tmp_prefix)

  # Cleanups resources.
  def tearDown(self):
    shutil.rmtree(self.test_dir)

  # Status tests.
  def testStatus(self):
    self.assertEqual("INFEASIBLE_ERROR", Status.CodeName(Status.INFEASIBLE_ERROR))
    status = Status()
    self.assertEqual("SUCCESS", str(status))
    self.assertEqual("<tkrzw_rpc.Status: SUCCESS>", repr(status))
    self.assertTrue(status.IsOK())
    self.assertEqual(Status.SUCCESS, status)
    self.assertNotEqual(Status.NOT_FOUND_ERROR, status)
    self.assertEqual(Status(Status.SUCCESS), status)
    self.assertNotEqual(Status(Status.NOT_FOUND_ERROR), status)    
    status = Status(Status.NOT_FOUND_ERROR, "foo")
    self.assertEqual("NOT_FOUND_ERROR: foo", str(status))
    self.assertEqual("<tkrzw_rpc.Status: NOT_FOUND_ERROR: foo>", repr(status))
    self.assertFalse(status.IsOK())
    status.Set(Status.SYSTEM_ERROR, "bar")
    self.assertEqual(Status.SYSTEM_ERROR, status.GetCode())
    self.assertEqual("bar", status.GetMessage())
    self.assertFalse(status.IsOK())
    status.Join(Status(Status.DUPLICATION_ERROR, "baz"))
    self.assertEqual(Status.SYSTEM_ERROR, status.GetCode())
    status.Set(Status.SUCCESS)
    try:
      status.OrDie()
    except:
      self.fail("exception")
    status.Join(Status(Status.DUPLICATION_ERROR, "baz"))
    self.assertEqual("DUPLICATION_ERROR: baz", str(status))
    try:
      status.OrDie()
    except StatusException as e:
      self.assertEqual("<tkrzw_rpc.StatusException: DUPLICATION_ERROR: baz>", repr(e))
      self.assertEqual("DUPLICATION_ERROR: baz", str(e))
      self.assertEqual("DUPLICATION_ERROR: baz", str(e.status))
    else:
      self.fail("no exception")

  # Basic tests.
  def testBasic(self):
    dbm = RemoteDBM()
    self.assertEqual(0, repr(dbm).find("<tkrzw_rpc.RemoteDBM"))
    self.assertEqual(0, str(dbm).find("RemoteDBM"))
    self.assertEqual(0, len(dbm))
    self.assertEqual(Status.SUCCESS, dbm.Connect("localhost:1978"))
    self.assertEqual(Status.SUCCESS, dbm.SetDBMIndex(-1))
    attrs = dbm.Inspect()
    self.assertTrue(len(attrs["version"]) > 3)
    self.assertTrue(len(attrs["num_dbms"]) > 0)
    self.assertEqual(Status.SUCCESS, dbm.SetDBMIndex(0))
    attrs = dbm.Inspect()
    self.assertTrue(len(attrs["class"]) > 0)
    self.assertTrue(len(attrs["num_records"]) > 0)
    status = Status(Status.UNKNOWN_ERROR)
    self.assertEqual("hello", dbm.Echo("hello", status))
    self.assertEqual(Status.SUCCESS, status)
    self.assertEqual(Status.SUCCESS, dbm.Clear())
    self.assertEqual(Status.SUCCESS, dbm.Set("one", "ichi", False))
    self.assertEqual(Status.DUPLICATION_ERROR, dbm.Set("one", "first", False))
    self.assertEqual(Status.SUCCESS, dbm.Set("one", "first", True))
    self.assertEqual(b"first", dbm.Get("one"))
    self.assertEqual(None, dbm.Get("two", status))
    self.assertEqual(Status.NOT_FOUND_ERROR, status)
    self.assertEqual("first", dbm.GetStr("one", status))
    self.assertEqual(Status.SUCCESS, status)
    self.assertEqual(None, dbm.GetStr("two", status))
    self.assertEqual(Status.NOT_FOUND_ERROR, status)
    self.assertEqual(Status.SUCCESS, dbm.Append("two", "second", ":"))
    self.assertEqual("second", dbm.GetStr("two"))
    self.assertEqual(Status.SUCCESS, dbm.Append(b"two", b"second", b":"))
    self.assertEqual("second:second", dbm.GetStr("two"))
    self.assertEqual(Status.SUCCESS, dbm.SetMulti(True, one="FIRST", two="SECOND"))
    records = dbm.GetMulti("one", "two", "three")
    self.assertEqual(2, len(records))
    self.assertEqual(b"FIRST", records[b"one"])
    self.assertEqual(b"SECOND", records[b"two"])
    records = dbm.GetMultiStr("one", "two", "three")
    self.assertEqual(2, len(records))
    self.assertEqual("FIRST", records["one"])
    self.assertEqual("SECOND", records["two"])
    self.assertEqual(Status.SUCCESS, dbm.RemoveMulti("one", "two"))
    self.assertEqual(Status.NOT_FOUND_ERROR, dbm.RemoveMulti("one"))
    self.assertEqual(Status.SUCCESS, dbm.AppendMulti(":", one="first", two="second"))
    self.assertEqual(Status.SUCCESS, dbm.AppendMulti(":", one="1", two="2"))
    records = dbm.GetMultiStr("one", "two")
    self.assertEqual("first:1", records["one"])
    self.assertEqual("second:2", records["two"])
    self.assertEqual(Status.SUCCESS, dbm.CompareExchange("one", "first:1", None))
    self.assertEqual(None, dbm.GetStr("one"))
    self.assertEqual(Status.SUCCESS, dbm.CompareExchange("one", None, "hello"))
    self.assertEqual("hello", dbm.GetStr("one"))
    self.assertEqual(Status.INFEASIBLE_ERROR, dbm.CompareExchange("one", None, "hello"))
    self.assertEqual(Status.SUCCESS, dbm.CompareExchangeMulti(    
      [["one", "hello"], ["two", "second:2"]], [["one", None], ["two", None]]))
    self.assertEqual(None, dbm.GetStr("one"))
    self.assertEqual(None, dbm.GetStr("two"))
    self.assertEqual(Status.SUCCESS, dbm.CompareExchangeMulti(    
      [["one", None], ["two", None]], [["one", "first"], ["two", "second"]]))
    status = Status(Status.UNKNOWN_ERROR)
    self.assertEqual(105, dbm.Increment("num", 5, 100, status))
    self.assertEqual(Status.SUCCESS, status)
    self.assertEqual(110, dbm.Increment("num", 5))
    self.assertEqual(3, dbm.Count())
    self.assertEqual(3, len(dbm))
    self.assertTrue(dbm.GetFileSize() >= 0)
    self.assertEqual(Status.SUCCESS, dbm.Rebuild())
    self.assertFalse(dbm.ShouldBeRebuilt())
    self.assertEqual(Status.SUCCESS, dbm.Synchronize(False))
    for i in range(10):
      self.assertEqual(Status.SUCCESS, dbm.Set(i, i))
    keys = dbm.Search("regex", "[23]$", 5)
    self.assertEqual(2, len(keys))
    self.assertTrue("2" in keys)
    self.assertTrue("3" in keys)
    self.assertEqual(Status.SUCCESS, dbm.Clear())
    dbm["japan"] = "tokyo"
    self.assertEqual("tokyo", dbm["japan"])
    del dbm["japan"]
    try:
      self.assertEqual("tokyo", dbm["japan"])
    except StatusException as e:
      self.assertEqual(Status.NOT_FOUND_ERROR, e.GetStatus())
    else:
      self.assertFalse("no exception")
    try:
      del dbm["japan"]
    except StatusException as e:
      self.assertEqual(Status.NOT_FOUND_ERROR, e.GetStatus())
    else:
      self.assertFalse("no exception")
    self.assertEqual(0, len(dbm))
    for i in range(1, 11):
      dbm[i] = i * i
      self.assertEqual(str(i * i).encode("utf-8"), dbm[i])
    count = 0
    for key, value in dbm:
      self.assertEqual(int(key) ** 2, int(value))
      count += 1
    self.assertEqual(len(dbm), count)
    self.assertEqual(Status.SUCCESS, dbm.Disconnect())

  # Iterator tests.
  def testIterator(self):
    dbm = RemoteDBM()
    self.assertEqual(Status.SUCCESS, dbm.Connect("localhost:1978"))
    self.assertEqual(Status.SUCCESS, dbm.Clear())
    for i in range(10):
      self.assertEqual(Status.SUCCESS, dbm.Set(i, i * i))
    it = dbm.MakeIterator()
    self.assertEqual(0, repr(it).find("<tkrzw_rpc.Iterator"))
    self.assertEqual(0, str(it).find("Iterator"))
    self.assertEqual(Status.SUCCESS, it.First())
    count = 0
    while True:
      status = Status(Status.UNKNOWN_ERROR)
      record = it.Get(status)
      if record:
        self.assertEqual(Status.SUCCESS, status)
      else:
        self.assertEqual(Status.NOT_FOUND_ERROR, status)
        break
      self.assertEqual(int(record[0]) ** 2, int(record[1]))
      status.Set(Status.UNKNOWN_ERROR)
      self.assertEqual(record[0], it.GetKey(status))
      self.assertEqual(Status.SUCCESS, status)
      self.assertEqual(record[1], it.GetValue(status))
      self.assertEqual(Status.SUCCESS, status)
      record = it.GetStr(status)
      self.assertTrue(record)
      self.assertEqual(int(record[0]) ** 2, int(record[1]))
      self.assertEqual(Status.SUCCESS, status)
      status.Set(Status.UNKNOWN_ERROR)
      self.assertEqual(record[0], it.GetKeyStr(status))
      self.assertEqual(Status.SUCCESS, status)
      status.Set(Status.UNKNOWN_ERROR)
      self.assertEqual(record[1], it.GetValueStr(status))
      self.assertEqual(Status.SUCCESS, status)
      self.assertEqual(Status.SUCCESS, it.Next())
      count += 1
    self.assertEqual(dbm.Count(), count)
    for i in range(count):
      self.assertEqual(Status.SUCCESS, it.Jump(i))
      record = it.Get(status)
      self.assertEqual(i, int(record[0]))
      self.assertEqual(i * i, int(record[1]))
    status = it.Last()
    if status == Status.SUCCESS:
      count = 0
      while True:
        status = Status()
        record = it.Get(status)
        if record:
          self.assertEqual(Status.SUCCESS, status)
        else:
          self.assertEqual(Status.NOT_FOUND_ERROR, status)
          break
        self.assertEqual(int(record[0]) ** 2, int(record[1]))
        self.assertEqual(Status.SUCCESS, it.Previous())
        count += 1
      self.assertEqual(dbm.Count(), count)
      self.assertEqual(Status.SUCCESS, it.JumpLower("0"))
      self.assertEqual(None, it.GetKeyStr(status))
      self.assertEqual(Status.NOT_FOUND_ERROR, status)
      self.assertEqual(Status.SUCCESS, it.JumpLower("0", True))
      self.assertEqual("0", it.GetKeyStr(status))
      self.assertEqual(Status.SUCCESS, status)
      self.assertEqual(Status.SUCCESS, it.Next())
      self.assertEqual("1", it.GetKeyStr())
      self.assertEqual(Status.SUCCESS, it.JumpUpper("9"))
      self.assertEqual(None, it.GetKeyStr(status))
      self.assertEqual(Status.NOT_FOUND_ERROR, status)
      self.assertEqual(Status.SUCCESS, it.JumpLower("9", True))
      self.assertEqual("9", it.GetKeyStr(status))
      self.assertEqual(Status.SUCCESS, status)
      self.assertEqual(Status.SUCCESS, it.Previous())
      self.assertEqual("8", it.GetKeyStr())
      self.assertEqual(Status.SUCCESS, it.Set("eight"))
      self.assertEqual("eight", it.GetValueStr())
      self.assertEqual(Status.SUCCESS, it.Remove())
      self.assertEqual("9", it.GetKeyStr())
      self.assertEqual(Status.SUCCESS, it.Remove())
      self.assertEqual(Status.NOT_FOUND_ERROR, it.Remove())
      self.assertEqual(8, dbm.Count())
    else:
      self.assertEqual(Status.NOT_IMPLEMENTED_ERROR, status)
    self.assertEqual(Status.SUCCESS, dbm.Disconnect())

  # Thread tests.
  def testThread(self):
    dbm = RemoteDBM()
    self.assertEqual(Status.SUCCESS, dbm.Connect("localhost:1978"))
    self.assertEqual(Status.SUCCESS, dbm.Clear())
    rnd_state = random.Random()
    num_records = 5000
    num_threads = 5
    records = {}
    class Task(threading.Thread):
      def __init__(self, test, thid):
        threading.Thread.__init__(self)
        self.thid = thid
        self.test = test
      def run(self):
        for i in range(0, num_records):
          key_num = rnd_state.randint(1, num_records)
          key_num = key_num - key_num % num_threads + self.thid;
          key = str(key_num)
          value = str(key_num * key_num)
          if rnd_state.randint(0, num_records) == 0:
            self.test.assertEqual(Status.SUCCESS, dbm.Rebuild())
          elif rnd_state.randint(0, 10) == 0:
            iter = dbm.MakeIterator()
            iter.Jump(key)
            status = Status()
            record = iter.Get(status)
            if status == Status.SUCCESS:
              self.test.assertEqual(2, len(record))
              self.test.assertEqual(key, record[0].decode())
              self.test.assertEqual(value, record[1].decode())
              status = iter.Next()
              self.test.assertTrue(status == Status.SUCCESS or status == Status.NOT_FOUND_ERROR)
          elif rnd_state.randint(0, 4) == 0:
            status = Status()
            rec_value = dbm.Get(key, status)
            if status == Status.SUCCESS:
              self.test.assertEqual(value, rec_value.decode())
            else:
              self.test.assertEqual(Status.NOT_FOUND_ERROR, status)
          elif rnd_state.randint(0, 4) == 0:
            status = dbm.Remove(key)
            if status == Status.SUCCESS:
              del records[key]
            else:
              self.test.assertEqual(Status.NOT_FOUND_ERROR, status)
          else:
            overwrite = rnd_state.randint(0, 2) == 0
            status = dbm.Set(key, value, overwrite)
            if status == Status.SUCCESS:
              records[key] = value
            else:
              self.test.assertEqual(Status.DUPLICATION_ERROR, status)
          if rnd_state.randint(0, 10) == 0:
            time.sleep(0.00001)
    threads = []
    for thid in range(0, num_threads):
        threads.append(Task(self, thid))
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    it_records = {}
    for key, value in dbm:
      it_records[key.decode()] = value.decode()
    self.assertEqual(records, it_records)
    self.assertEqual(Status.SUCCESS, dbm.Disconnect())
    

# Main routine.
def main(argv):
  test_names = argv
  if test_names:
    test_suite = unittest.TestSuite()
    for test_name in test_names:
      test_suite.addTest(TestTkrzw(test_name))
  else:
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestTkrzw)
  unittest.TextTestRunner(verbosity=2).run(test_suite)
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))


# END OF FILE
