#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Python client library of Tkrzw-RPC
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

import grpc
import pathlib
import sys
import threading
import time

sys.path.append(str(pathlib.Path(__file__).parent))

from . import tkrzw_rpc_pb2
from . import tkrzw_rpc_pb2_grpc


def _StrGRPCError(error):
  code_name = str(error.code())
  delim_pos = code_name.find(".")
  if delim_pos >= 0:
    code_name = code_name[delim_pos + 1:]
  details = error.details()
  if details:
    return code_name + ": " + details
  return code_name


def _MakeStatusFromProto(proto_status):
  return Status(proto_status.code, proto_status.message)


def _SetStatusFromProto(status, proto_status):
  status.Set(proto_status.code, proto_status.message)


def _MakeBytes(obj):
  if isinstance(obj, bytes):
    return obj
  if not isinstance(obj, str):
    obj = str(obj)
  return obj.encode('utf-8')


class Status:
  """
  Status of operations.
  """

  SUCCESS = 0
  """Success."""
  UNKNOWN_ERROR = 1
  """Generic error whose cause is unknown."""
  SYSTEM_ERROR = 2
  """Generic error from underlying systems."""
  NOT_IMPLEMENTED_ERROR = 3
  """Error that the feature is not implemented."""
  PRECONDITION_ERROR = 4
  """Error that a precondition is not met."""
  INVALID_ARGUMENT_ERROR = 5
  """Error that a given argument is invalid."""
  CANCELED_ERROR = 6
  """Error that the operation is canceled."""
  NOT_FOUND_ERROR = 7
  """Error that a specific resource is not found."""
  PERMISSION_ERROR = 8
  """Error that the operation is not permitted."""
  INFEASIBLE_ERROR = 9
  """Error that the operation is infeasible."""
  DUPLICATION_ERROR = 10
  """Error that a specific resource is duplicated."""
  BROKEN_DATA_ERROR = 11
  """Error that internal data are broken."""
  NETWORK_ERROR = 12
  """Error caused by networking failure."""
  APPLICATION_ERROR = 13
  """Generic error caused by the application logic."""

  def __init__(self, code=SUCCESS, message=""):
    """
    Sets the code and the message.

    :param code: The status code.  This can be omitted and then SUCCESS is set.
    :param message: An arbitrary status message.  This can be omitted and the an empty string is set.
    """
    self.code = code
    self.message = message

  def __repr__(self):
    """
    Returns a string representation of the object.

    :return: The string representation of the object.
    """
    if self.message:
      return "<tkrzw_rpc.Status: " + self.CodeName(self.code) + ": " + self.message + ">"
    return "<tkrzw_rpc.Status: " + self.CodeName(self.code) + ">"

  def __str__(self):
    """
    Returns a string representation of the content.

    :return: The string representation of the content.
    """
    if self.message:
      return self.CodeName(self.code) + ": " + self.message
    return self.CodeName(self.code)

  def __eq__(self, rhs):
    """
    Returns true if the given object is equivalent to this object.
    
    :return: True if the given object is equivalent to this object.

    This supports comparison between a status object and a status code number.
    """
    if isinstance(rhs, type(self)):
      return self.code == rhs.code
    if isinstance(rhs, int):
      return self.code == rhs
    return False

  def Set(self, code=SUCCESS, message=""):
    """
    Sets the code and the message.

    :param code: The status code.  This can be omitted and then SUCCESS is set.
    :param message: An arbitrary status message.  This can be omitted and the an empty string is set.
    """
    self.code = code
    self.message = message

  def Join(self, rht):
    """
    Assigns the internal state from another status object only if the current state is success.

    :param rhs: The status object.
    """
    if self.code == self.SUCCESS:
      self.code = rht.code
      self.message = rht.message

  def GetCode(self):
    """
    Gets the status code.

    :return: The status code.
    """
    return self.code

  def GetMessage(self):
    """
    Gets the status message.

    :return: The status message.
    """
    return self.message

  def IsOK(self):
    """
    Returns true if the status is success.

    :return: True if the status is success, or False on failure.
    """
    return self.code == self.SUCCESS

  def OrDie(self):
    """
    Raises an exception if the status is not success.

    :raise StatusException: An exception containing the status object.
    """
    if self.code != self.SUCCESS:
      raise StatusException(self)

  @classmethod
  def CodeName(cls, code):
    """
    Gets the string name of a status code.

    :param: code The status code.
    :return: The name of the status code.
    """
    if code == cls.SUCCESS: return "SUCCESS"
    if code == cls.UNKNOWN_ERROR: return "UNKNOWN_ERROR"
    if code == cls.SYSTEM_ERROR: return "SYSTEM_ERROR"
    if code == cls.NOT_IMPLEMENTED_ERROR: return "NOT_IMPLEMENTED_ERROR"
    if code == cls.PRECONDITION_ERROR: return "PRECONDITION_ERROR"
    if code == cls.INVALID_ARGUMENT_ERROR: return "INVALID_ARGUMENT_ERROR"
    if code == cls.CANCELED_ERROR: return "CANCELED_ERROR"
    if code == cls.NOT_FOUND_ERROR: return "NOT_FOUND_ERROR"
    if code == cls.PERMISSION_ERROR: return "PERMISSION_ERROR"
    if code == cls.INFEASIBLE_ERROR: return "INFEASIBLE_ERROR"
    if code == cls.DUPLICATION_ERROR: return "DUPLICATION_ERROR"
    if code == cls.BROKEN_DATA_ERROR: return "BROKEN_DATA_ERROR"
    if code == cls.NETWORK_ERROR: return "NETWORK_ERROR"
    if code == cls.APPLICATION_ERROR: return "APPLICATION_ERROR"
    return "unknown"


class StatusException(RuntimeError):
  """
  Exception to convey the status of operations.
  """

  def __init__(self, status):
    """
    Sets the status.

    :param status: The status object.
    """
    self.status = status

  def __repr__(self):
    """
    Returns A string representation of the object.

    :return: The string representation of the object.
    """
    return "<tkrzw_rpc.StatusException: " + str(self.status) + ">"

  def __str__(self):
    """
    Returns A string representation of the content.

    :return: The string representation of the content.
    """
    return str(self.status)

  def GetStatus(self):
    """
    Gets the status object

    :return: The status object.
    """
    return self.status


class RemoteDBM:
  """
  Remote database manager.

  All operations are thread-safe; Multiple threads can access the same database concurrently.  The SetDBMIndex affects all threads so it should be called before the object is shared.  This class implements the iterable protocol so an instance is usable with "for-in" loop.
  """

  def __init__(self):
    """
    Does nothing especially.
    """
    self.channel = None
    self.stub = None
    self.timeout = None
    self.dbm_index = 0

  def __repr__(self):
    """
    Returns A string representation of the object.

    :return: The string representation of the object.
    """
    expr = "connected" if self.channel else "not connected"
    return "<tkrzw_rpc.RemoteDBM: " + hex(id(self)) + ": " + expr + ">"

  def __str__(self):
    """
    Returns A string representation of the content.

    :return: The string representation of the content.
    """
    expr = "connected" if self.channel else "not connected"
    return "RemoteDBM: " + hex(id(self)) + ": " + expr

  def __len__(self):
    """
    Gets the number of records, to enable the len operator.

    :return: The number of records on success, or 0 on failure.
    """
    if not self.channel:
      return 0
    request = tkrzw_rpc_pb2.CountRequest()
    request.dbm_index = self.dbm_index
    try:
      response = self.stub.Count(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return 0
    return response.count

  def __getitem__(self, key):
    """
    Gets the value of a record, to enable the [] operator.

    :param key: The key of the record.
    :return: The value of the matching record.  An exception is raised for missing records.  If the given key is a string, the returned value is also a string.  Otherwise, the return value is bytes.
    :raise StatusException: An exception containing the status object.
    """
    if not self.channel:
      raise StatusException(Status(Status.PRECONDITION_ERROR, "not opened connection"))
    request = tkrzw_rpc_pb2.GetRequest()
    request.dbm_index = self.dbm_index
    request.key = _MakeBytes(key)
    try:
      response = self.stub.Get(request, timeout=self.timeout)
    except grpc.RpcError as error:
      raise StatusException(Status(Status.NETWORK_ERROR, _StrGRPCError(error)))
    if response.status.code != Status.SUCCESS:
      raise StatusException(_MakeStatusFromProto(response.status))
    if isinstance(key, str):
      return response.value.decode("utf-8")
    return response.value

  def __setitem__(self, key, value):
    """
    Sets a record of a key and a value, to enable the []= operator.

    :param key: The key of the record.
    :param value: The value of the record.
    :raise StatusException: An exception containing the status object.
    """
    if not self.channel:
      raise StatusException(Status(Status.PRECONDITION_ERROR, "not opened connection"))
    request = tkrzw_rpc_pb2.SetRequest()
    request.dbm_index = self.dbm_index
    request.key = _MakeBytes(key)
    request.value = _MakeBytes(value)
    request.overwrite = True
    try:
      response = self.stub.Set(request, timeout=self.timeout)
    except grpc.RpcError as error:
      raise StatusException(Status(Status.NETWORK_ERROR, _StrGRPCError(error)))
    if response.status.code != Status.SUCCESS:
      raise StatusException(_MakeStatusFromProto(response.status))

  def __delitem__(self, key):
    """
    Removes a record of a key, to enable the del [] operator.

    :param key: The key of the record.
    :raise StatusException: An exception containing the status object.
    """
    if not self.channel:
      raise StatusException(Status(Status.PRECONDITION_ERROR, "not opened connection"))
    request = tkrzw_rpc_pb2.RemoveRequest()
    request.dbm_index = self.dbm_index
    request.key = _MakeBytes(key)
    try:
      response = self.stub.Remove(request, timeout=self.timeout)
    except grpc.RpcError as error:
      raise StatusException(Status(Status.NETWORK_ERROR, _StrGRPCError(error)))
    if response.status.code != Status.SUCCESS:
      raise StatusException(_MakeStatusFromProto(response.status))

  def __iter__(self):
    """
    Makes an iterator and initialize it, to comply to the iterator protocol.

    :return: The iterator for each record.
    """
    it = self.MakeIterator()
    it.First()
    return it

  def Connect(self, address, timeout=None):
    """
    Connects to the server.

    :param address: The address or the host name of the server and its port number.  For IPv4 address, it's like "127.0.0.1:1978".  For IPv6, it's like "[::1]:1978".  For UNIX domain sockets, it's like "unix:/path/to/file".
    :param timeout: The timeout in seconds for connection and each operation.  Negative means unlimited.
    :return: The result status.
    """
    if self.channel:
      return Status(Status.PRECONDITION_ERROR, "opened connection")
    timeout = timeout if timeout and timeout >= 0 else 1 << 30
    deadline = time.time() + timeout
    try:
      self.channel = grpc.insecure_channel(address)
      last_conn = grpc.ChannelConnectivity.CONNECTING
      def checker(conn):
        nonlocal last_conn
        last_conn = conn
      self.channel.subscribe(checker, True)
      ready_future = grpc.channel_ready_future(self.channel)
      max_failures = 3
      num_failures = 0
      while True:
        if time.time() > deadline:
          self.channel.close()
          self.channel = None
          return Status(Status.NETWORK_ERROR, "connection timeout")
        try:
          ready_future.result(0.1)
          break
        except grpc.FutureTimeoutError:
          pass
        if last_conn == grpc.ChannelConnectivity.TRANSIENT_FAILURE:
          num_failures += 1
        if last_conn == grpc.ChannelConnectivity.SHUTDOWN:
          num_failures = max_failures
        if num_failures >= max_failures:
          self.channel.close()
          self.channel = None
          return Status(Status.NETWORK_ERROR, "connection failed")
      self.stub = tkrzw_rpc_pb2_grpc.DBMServiceStub(self.channel)
    except grpc.RpcError as error:
      self.channel = None
      self.stub = None
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    self.timeout = timeout
    return Status(Status.SUCCESS)

  def Disconnect(self):
    """
    Disconnects the connection to the server.

    :return The result status.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    status = Status(Status.SUCCESS)
    try:
      self.channel.close()
    except grpc.RpcError as error:
      status = Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    self.channel = None
    self.stub = None
    return status
  
  def SetDBMIndex(self, dbm_index):
    """
    Sets the index of the DBM to access.

    :param dbm_index: The index of the DBM to access.
    :return: The result status.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    self.dbm_index = dbm_index
    return Status(Status.SUCCESS)

  def Echo(self, message, status=None):
    """
    Sends a message and gets back the echo message.

    :param: message The message to send.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The string value of the echoed message or None on failure.
    """
    if not self.channel:
      if status:
        status.Set(Status.PRECONDITION_ERROR, "not opened connection")
      return None
    request = tkrzw_rpc_pb2.EchoRequest()
    request.message = message
    try:
      response = self.stub.Echo(request, timeout=self.timeout)
    except grpc.RpcError as error:
      if status:
        status.Set(Status.NETWORK_ERROR, _StrGRPCError(error))
      return None
    if status:
      status.Set(Status.SUCCESS)
    return response.echo

  def Inspect(self):
    """
    Inspects the database.

    :return: A map of property names and their values.

    If the DBM index is negative, basic metadata of all DBMs are obtained.
    """
    result = {}
    if not self.channel:
      return result
    request = tkrzw_rpc_pb2.InspectRequest()
    request.dbm_index = self.dbm_index
    try:
      response = self.stub.Inspect(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return result
    for record in response.records:
      result[record.first] = record.second
    return result

  def Get(self, key, status=None):
    """
    Gets the value of a record of a key.

    :param key: The key of the record.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The bytes value of the matching record or None on failure.
    """
    if not self.channel:
      if status:
        status.Set(Status.PRECONDITION_ERROR, "not opened connection")
      return None
    request = tkrzw_rpc_pb2.GetRequest()
    request.dbm_index = self.dbm_index
    request.key = _MakeBytes(key)
    try:
      response = self.stub.Get(request, timeout=self.timeout)
    except grpc.RpcError as error:
      if status:
        status.Set(Status.NETWORK_ERROR, _StrGRPCError(error))
      return None
    if status:
      _SetStatusFromProto(status, response.status)
    if response.status.code == Status.SUCCESS:
      return response.value
    return None

  def GetStr(self, key, status=None):
    """
    Gets the value of a record of a key, as a string.

    :param key: The key of the record.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The string value of the matching record or None on failure.
    """
    value = self.Get(key, status)
    return None if value == None else value.decode("utf-8")

  def GetMulti(self, *keys):
    """
    Gets the values of multiple records of keys.

    :param keys: The keys of records to retrieve.
    :return: A map of retrieved records.  Keys which don't match existing records are ignored.
    """
    result = {}
    if not self.channel:
      return result
    request = tkrzw_rpc_pb2.GetMultiRequest()
    request.dbm_index = self.dbm_index
    for key in keys:
      request.keys.append(_MakeBytes(key))
    try:
      response = self.stub.GetMulti(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return result
    for record in response.records:
      result[record.first] = record.second
    return result

  def GetMultiStr(self, *keys):
    """
    Gets the values of multiple records of keys, as strings.

    :param keys: The keys of records to retrieve.
    :return: A map of retrieved records.  Keys which don't match existing records are ignored.
    """
    result = {}
    if not self.channel:
      return result
    request = tkrzw_rpc_pb2.GetMultiRequest()
    request.dbm_index = self.dbm_index
    for key in keys:
      request.keys.append(_MakeBytes(key))
    try:
      response = self.stub.GetMulti(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return result
    for record in response.records:
      result[record.first.decode("utf-8")] = record.second.decode("utf-8")
    return result

  def Set(self, key, value, overwrite=True):
    """
    Sets a record of a key and a value.

    :param key: The key of the record.
    :param value: The value of the record.
    :param overwrite: Whether to overwrite the existing value.  It can be omitted and then false is set.
    :return: The result status.  If overwriting is abandoned, DUPLICATION_ERROR is returned.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.SetRequest()
    request.dbm_index = self.dbm_index
    request.key = _MakeBytes(key)
    request.value = _MakeBytes(value)
    request.overwrite = bool(overwrite)
    try:
      response = self.stub.Set(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def SetMulti(self, overwrite=True, **records):
    """
    Sets multiple records of the keyword arguments.

    :param overwrite: Whether to overwrite the existing value if there's a record with the same key.  If true, the existing value is overwritten by the new value.  If false, the operation is given up and an error status is returned.
    :param records: Records to store, specified as keyword parameters.
    :return: The result status.  If there are records avoiding overwriting, DUPLICATION_ERROR is returned.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.SetMultiRequest()
    request.dbm_index = self.dbm_index
    for key, value in records.items():
      record = request.records.add()
      record.first = _MakeBytes(key)
      record.second = _MakeBytes(value)
    request.overwrite = bool(overwrite)
    try:
      response = self.stub.SetMulti(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Remove(self, key):
    """
    Removes a record of a key.

    :param key: The key of the record.
    :return: The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.RemoveRequest()
    request.dbm_index = self.dbm_index
    request.key = _MakeBytes(key)
    try:
      response = self.stub.Remove(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def RemoveMulti(self, *keys):
    """
    Removes records of keys.

    :param key: The keys of the records.
    :return: The result status.  If there are missing records, NOT_FOUND_ERROR is returned.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.RemoveMultiRequest()
    request.dbm_index = self.dbm_index
    for key in keys:
      request.keys.append(_MakeBytes(key))
    try:
      response = self.stub.RemoveMulti(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Append(self, key, value, delim=""):
    """
    Appends data at the end of a record of a key.

    :param key: The key of the record.
    :param value: The value to append.
    :param delim: The delimiter to put after the existing record.
    :return: The result status.

    If there's no existing record, the value is set without the delimiter.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.AppendRequest()
    request.dbm_index = self.dbm_index
    request.key = _MakeBytes(key)
    request.value = _MakeBytes(value)
    request.delim = _MakeBytes(delim)
    try:
      response = self.stub.Append(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def AppendMulti(self, delim="", **records):
    """
    Appends data to multiple records of the keyword arguments.

    :param delim: The delimiter to put after the existing record.
    :param records: Records to append.  Existing records with the same keys are overwritten.
    :return: The result status.

    If there's no existing record, the value is set without the delimiter.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.AppendMultiRequest()
    request.dbm_index = self.dbm_index
    for key, value in records.items():
      record = request.records.add()
      record.first = _MakeBytes(key)
      record.second = _MakeBytes(value)
    request.delim = _MakeBytes(delim)
    try:
      response = self.stub.AppendMulti(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def CompareExchange(self, key, expected, desired):
    """
    Compares the value of a record and exchanges if the condition meets.

    :param key: The key of the record.
    :param expected: The expected value.  If it is None, no existing record is expected.
    :param desired: The desired value.  If it is None, the record is to be removed.
    :return: The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.CompareExchangeRequest()
    request.dbm_index = self.dbm_index
    request.key = _MakeBytes(key)
    if expected != None:
      request.expected_existence = True
      request.expected_value = _MakeBytes(expected)
    if desired != None:
      request.desired_existence = True
      request.desired_value = _MakeBytes(desired)
    try:
      response = self.stub.CompareExchange(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Increment(self, key, inc=1, init=0, status=None):
    """
    Increments the numeric value of a record.

    :param key: The key of the record.
    :param inc: The incremental value.  If it is Utility.INT64MIN, the current value is not changed and a new record is not created.
    :param init: The initial value.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The current value, or None on failure.

    The record value is stored as an 8-byte big-endian integer.  Negative is also supported.
    """
    if not self.channel:
      if status:
        status.Set(Status.PRECONDITION_ERROR, "not opened connection")
      return None
    request = tkrzw_rpc_pb2.IncrementRequest()
    request.dbm_index = self.dbm_index
    request.key = _MakeBytes(key)
    request.increment = inc
    request.initial = init
    try:
      response = self.stub.Increment(request, timeout=self.timeout)
    except grpc.RpcError as error:
      if status:
        status.Set(Status.NETWORK_ERROR, _StrGRPCError(error))
      return None
    if status:
      _SetStatusFromProto(status, response.status)
    if response.status.code == Status.SUCCESS:
      return response.current
    return None

  def CompareExchangeMulti(self, expected, desired):
    """
    Compares the values of records and exchanges if the condition meets.

    :param expected: A sequence of pairs of the record keys and their expected values.  If the value is None, no existing record is expected.
    :param desired: A sequence of pairs of the record keys and their desired values.  If the value is None, the record is to be removed.
    :return: The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.CompareExchangeMultiRequest()
    request.dbm_index = self.dbm_index
    for key, value in expected:
      record = request.expected.add()
      record.key = _MakeBytes(key)
      if value != None:
        record.existence = True
        record.value = None if value == None else _MakeBytes(value)
    for key, value in desired:
      record = request.desired.add()
      record.key = _MakeBytes(key)
      if value != None:
        record.existence = True
        record.value = None if value == None else _MakeBytes(value)
    try:
      response = self.stub.CompareExchangeMulti(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Rekey(old_key, new_key, overwrite=True, copying=False):
    """
    Changes the key of a record.

    :param old_key: The old key of the record.
    :param new_key: The new key of the record.
    :param overwrite: Whether to overwrite the existing record of the new key.
    :param copying: Whether to retain the record of the old key.

    :return: The result status.  If there's no matching record to the old key, NOT_FOUND_ERROR is returned.  If the overwrite flag is false and there is an existing record of the new key, DUPLICATION ERROR is returned.

    This method is done atomically.  The other threads observe that the record has either the old key or the new key.  No intermediate states are observed.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.RekeyRequest()
    request.dbm_index = self.dbm_index
    request.old_key = _MakeBytes(old_key)
    request.new_key = _MakeBytes(new_key)
    request.overwrite = bool(overwrite)
    request.copying = bool(copying)
    try:
      response = self.stub.Rekey(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def PopFirst(self, retry_wait=None, status=None):
    """
    Gets the first record and removes it.

    :param retry_wait: The maximum wait time in seconds before retrying.  If it is None, no retry is done.  If it is positive, retry is done and wait for the notifications of the next update for the time at most.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: A tuple of the bytes key and the bytes value of the first record.  On failure, None is returned.
    """
    if not self.channel:
      if status:
        status.Set(Status.PRECONDITION_ERROR, "not opened connection")
      return None
    request = tkrzw_rpc_pb2.PopFirstRequest()
    request.dbm_index = self.dbm_index
    request.retry_wait = retry_wait if retry_wait else 0
    try:
      response = self.stub.PopFirst(request, timeout=self.timeout)
    except grpc.RpcError as error:
      if status:
        status.Set(Status.NETWORK_ERROR, _StrGRPCError(error))
      return None
    if status:
      _SetStatusFromProto(status, response.status)
    if response.status.code == Status.SUCCESS:
      return response.key, response.value
    return None

  def PopFirstStr(self, retry_wait=None, status=None):
    """
    Gets the first record as strings and removes it.

    :param retry_wait: The maximum wait time in seconds before retrying.  If it is None, no retry is done.  If it is positive, retry is done and wait for the notifications of the next update for the time at most.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: A tuple of the string key and the string value of the first record.  On failure, None is returned.
    """
    record = self.PopFirst(retry_wait, status)
    return None if record == None else (record[0].decode("latin-1"), record[1].decode("utf-8"))

  def PushLast(self, value, wtime=None, notify=False):
    """
    Adds a record with a key of the current timestamp.

    :param value: The value of the record.
    :param wtime: The current wall time used to generate the key.  If it is None, the system clock is used.
    :param notify: If true, notification signal is sent.
    :return: The result status.

    The key is generated as an 8-bite big-endian binary string of the timestamp.  If there is an existing record matching the generated key, the key is regenerated and the attempt is repeated until it succeeds.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.PushLastRequest()
    request.dbm_index = self.dbm_index
    request.value = _MakeBytes(value)
    request.wtime = -1 if wtime == None else wtime
    request.notify = notify
    try:
      response = self.stub.PushLast(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Count(self):
    """
    Gets the number of records.

    :return: The number of records on success, or None on failure.
    """
    if not self.channel:
      return None
    request = tkrzw_rpc_pb2.CountRequest()
    request.dbm_index = self.dbm_index
    try:
      response = self.stub.Count(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return None
    return response.count

  def GetFileSize(self):
    """
    Gets the current file size of the database.

    :return: The current file size of the database, or None on failure.
    """
    if not self.channel:
      return None
    request = tkrzw_rpc_pb2.GetFileSizeRequest()
    request.dbm_index = self.dbm_index
    try:
      response = self.stub.GetFileSize(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return None
    return response.file_size

  def Clear(self):
    """
    Removes all records.

    :return: The result status.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.ClearRequest()
    request.dbm_index = self.dbm_index
    try:
      response = self.stub.Clear(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Rebuild(self, **params):
    """
    Rebuilds the entire database.

    :param params: Optional keyword parameters.
    :return: The result status.

    The optional parameters are the same as the Open method of the local DBM class and the database configurations of the server command.  Omitted tuning parameters are kept the same or implicitly optimized.

    In addition, HashDBM, TreeDBM, and SkipDBM supports the following parameters.
      - skip_broken_records (bool): If true, the operation continues even if there are broken records which can be skipped.
      - sync_hard (bool): If true, physical synchronization with the hardware is done before finishing the rebuilt file.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.RebuildRequest()
    request.dbm_index = self.dbm_index
    for name, value in params.items():
      param = request.params.add()
      param.first = _MakeBytes(name)
      param.second = _MakeBytes(value)
    try:
      response = self.stub.Rebuild(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def ShouldBeRebuilt(self):
    """
    Checks whether the database should be rebuilt.

    :return: True to be optimized or false with no necessity.
    """
    if not self.channel:
      return False
    request = tkrzw_rpc_pb2.ShouldBeRebuiltRequest()
    request.dbm_index = self.dbm_index
    try:
      response = self.stub.ShouldBeRebuilt(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return False
    return response.tobe

  def Synchronize(self, hard, **params):
    """
    Synchronizes the content of the database to the file system.

    :param hard: True to do physical synchronization with the hardware or false to do only logical synchronization with the file system.
    :param params: Optional keyword parameters.
    :return: The result status.

    The "reducer" parameter specifies the reducer for SkipDBM.  "ReduceToFirst", "ReduceToSecond", "ReduceToLast", etc are supported.  If the parameter "make_backup" exists, a backup file is created in the same directory as the database file.  The backup file name has a date suffix in GMT, like ".backup.20210831213749".  If the value of "make_backup" not empty, it is the value is used as the suffix.
    """
    if not self.channel:
      return Status(Status.PRECONDITION_ERROR, "not opened connection")
    request = tkrzw_rpc_pb2.SynchronizeRequest()
    request.dbm_index = self.dbm_index
    request.hard = hard
    for name, value in params.items():
      param = request.params.add()
      param.first = _MakeBytes(name)
      param.second = _MakeBytes(value)
    try:
      response = self.stub.Synchronize(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Search(self, mode, pattern, capacity=0):
    """
    Searches the database and get keys which match a pattern.

    :param mode: The search mode.  "contain" extracts keys containing the pattern.  "begin" extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.  "regex" extracts keys partially matches the pattern of a regular expression.  "edit" extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts keys whose edit distance to the binary pattern is the least.
    :param pattern: The pattern for matching.
    :param capacity: The maximum records to obtain.  0 means unlimited.
    :return: A list of string keys matching the condition.
    """
    result = []
    if not self.channel:
      return result
    request = tkrzw_rpc_pb2.SearchRequest()
    request.dbm_index = self.dbm_index
    request.mode = mode
    request.pattern = _MakeBytes(pattern)
    request.capacity = capacity
    try:
      response = self.stub.Search(request, timeout=self.timeout)
    except grpc.RpcError as error:
      return result
    if response.status.code == Status.SUCCESS:
      for key in response.matched:
        result.append(key.decode("utf-8"))
    return result

  def MakeIterator(self):
    """
    Makes an iterator for each record.

    :return: The iterator for each record.
    """
    return Iterator(self)


class Iterator:
  """
  Iterator for each record.
  """

  def __init__(self, dbm):
    """
    Initializes the iterator.

    :param dbm: The database to scan.
    """
    if not dbm.channel:
      raise StatusException(Status(Status.PRECONDITION_ERROR, "not opened connection"))
    self.dbm = dbm
    class RequestIterator():
      def __init__(self):
        self.request = None
        self.event = threading.Event()
      def __next__(self):
        self.event.wait()
        self.event.clear()
        if self.request:
          return self.request
        raise StopIteration
    self.req_it = RequestIterator()
    try:
      self.res_it = dbm.stub.Iterate(self.req_it)
    except grpc.RpcError as error:
      self.dbm = None
      self.req_it = None

  def __del__(self):
    """
    Destructs the iterator.
    """
    self.req_it.request = None
    self.req_it.event.set()

  def __repr__(self):
    """
    Returns A string representation of the object.

    :return: The string representation of the object.
    """
    return "<tkrzw_rpc.Iterator: " + hex(id(self)) + ">"

  def __str__(self):
    """
    Returns A string representation of the content.

    :return: The string representation of the content.
    """
    return "Iterator: " + hex(id(self))

  def __next__(self):
    """
    Moves the iterator to the next record, to comply to the iterator protocol.

    :return: A tuple of The key and the value of the current record.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_GET
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      raise StatusException(Status(Status.NETWORK_ERROR, _StrGRPCError(error)))
    if response.status.code == Status.SUCCESS:
      self.Next()
      return (response.key, response.value)
    if response.status.code == Status.NOT_FOUND_ERROR:
      raise StopIteration
    raise StatusException(_MakeStatusFromProto(response.status))

  def First(self):
    """
    Initializes the iterator to indicate the first record.

    :return: The result status.

    Even if there's no record, the operation doesn't fail.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_FIRST
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Last(self):
    """
    Initializes the iterator to indicate the last record.

    :return: The result status.

    Even if there's no record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_LAST
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Jump(self, key):
    """
    Initializes the iterator to indicate a specific record.

    :param key: The key of the record to look for.
    :return: The result status.

    Ordered databases can support "lower bound" jump; If there's no record with the same key, the iterator refers to the first record whose key is greater than the given key.  The operation fails with unordered databases if there's no record with the same key.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_JUMP
    request.key = _MakeBytes(key)
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def JumpLower(self, key, inclusive=False):
    """
    Initializes the iterator to indicate the last record whose key is lower than a given key.

    :param key: The key to compare with.
    :param inclusive: If true, the considtion is inclusive: equal to or lower than the key.
    :return: The result status.

    Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_JUMP_LOWER
    request.key = _MakeBytes(key)
    request.jump_inclusive = inclusive
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def JumpUpper(self, key, inclusive=False):
    """
    Initializes the iterator to indicate the first record whose key is upper than a given key.

    :param key: The key to compare with.
    :param inclusive: If true, the considtion is inclusive: equal to or upper than the key.
    :return: The result status.

    Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_JUMP_UPPER
    request.key = _MakeBytes(key)
    request.jump_inclusive = inclusive
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Next(self):
    """
    Moves the iterator to the next record.

    :return: The result status.

    If the current record is missing, the operation fails.  Even if there's no next record, the operation doesn't fail.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_NEXT
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Previous(self):
    """
    Moves the iterator to the previous record.

    :return: The result status.

    If the current record is missing, the operation fails.  Even if there's no previous record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_PREVIOUS
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Get(self, status=None):
    """
    Gets the key and the value of the current record of the iterator.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: A tuple of the bytes key and the bytes value of the current record.  On failure, None is returned.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_GET
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      if status:
        status.Set(Status.NETWORK_ERROR, _StrGRPCError(error))
      return None
    if status:
      _SetStatusFromProto(status, response.status)
    if response.status.code == Status.SUCCESS:
      return (response.key, response.value)
    return None

  def GetStr(self, status=None):
    """
    Gets the key and the value of the current record of the iterator, as strings.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: A tuple of the string key and the string value of the current record.  On failure, None is returned.
    """
    record = self.Get(status)
    if record:
      return (record[0].decode("utf-8"), record[1].decode("utf-8"))
    return None

  def GetKey(self, status=None):
    """
    Gets the key of the current record.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The bytes key of the current record or None on failure.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_GET
    request.omit_value = True
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      if status:
        status.Set(Status.NETWORK_ERROR, _StrGRPCError(error))
      return None
    if status:
      _SetStatusFromProto(status, response.status)
    if response.status.code == Status.SUCCESS:
      return response.key
    return None

  def GetKeyStr(self, status=None):
    """
    Gets the key of the current record, as a string.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The string key of the current record or None on failure.
    """
    key = self.GetKey(status)
    if key:
      return key.decode("utf-8")
    return None

  def GetValue(self, status=None):
    """
    Gets the value of the current record.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The bytes value of the current record or None on failure.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_GET
    request.omit_key = True
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      if status:
        status.Set(Status.NETWORK_ERROR, _StrGRPCError(error))
      return None
    if status:
      _SetStatusFromProto(status, response.status)
    if response.status.code == Status.SUCCESS:
      return response.value
    return None

  def GetValueStr(self, status=None):
    """
    Gets the value of the current record, as a string.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The string value of the current record or None on failure.
    """
    value = self.GetValue(status)
    if value:
      return value.decode("utf-8")
    return None

  def Set(self, value):
    """
    Sets the value of the current record.

    :param value: The value of the record.
    :return: The result status.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_SET
    request.value = _MakeBytes(value)
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Remove(self):
    """
    Removes the current record.

    :return: The result status.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_REMOVE
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      return Status(Status.NETWORK_ERROR, _StrGRPCError(error))
    return _MakeStatusFromProto(response.status)

  def Step(self, status=None):
    """
    Gets the current record and moves the iterator to the next record.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: A tuple of the bytes key and the bytes value of the current record.  On failure, None is returned.
    """
    request = tkrzw_rpc_pb2.IterateRequest()
    request.dbm_index = self.dbm.dbm_index
    request.operation = tkrzw_rpc_pb2.IterateRequest.OP_STEP
    try:
      self.req_it.request = request
      self.req_it.event.set()
      response = self.res_it.__next__()
      self.req_it.request = None
    except grpc.RpcError as error:
      if status:
        status.Set(Status.NETWORK_ERROR, _StrGRPCError(error))
      return None
    if status:
      _SetStatusFromProto(status, response.status)
    if response.status.code == Status.SUCCESS:
      return (response.key, response.value)
    return None

  def StepStr(self, status=None):
    """
    Gets the current record and moves the iterator to the next record, as strings.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: A tuple of the string key and the string value of the current record.  On failure, None is returned.
    """
    record = self.Step(status)
    if record:
      return (record[0].decode("utf-8"), record[1].decode("utf-8"))
    return None


# END OF FILE
