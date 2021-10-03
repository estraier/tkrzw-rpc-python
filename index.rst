Module and Classes
==================

.. autosummary::
   :nosignatures:

   tkrzw_rpc
   tkrzw_rpc.Status
   tkrzw_rpc.StatusException
   tkrzw_rpc.RemoteDBM
   tkrzw_rpc.Iterator

Introduction
============

The core package of Tkrzw-RPC provides a server program which manages databases of Tkrzw.  This package provides a Python client library to access the service via gRPC protocol.  Tkrzw is a library to mange key-value storages in various algorithms.  With Tkrzw, the application can handle database files efficiently in process without any network overhead.  However, it means that multiple processes cannot open the same database file simultaneously.  Tkrzw-RPC solves the issue by using a server program which manages database files and allowing other processes access the contents via RPC.

The class ":class:`RemoteDBM`" has a similar API to the local DBM API, which represents an associative array aka a dictionary in Python.  Read the `homepage <https://dbmx.net/tkrzw-rpc/>`_ for details.

Symbols of the module "tkrzw_rpc" should be imported in each source file of application programs.::

 import tkrzw_rpc

An instance of the class ":class:`RemoteDBM`" is used in order to handle a database.  You can store, delete, and retrieve records with the instance.  The result status of each operation is represented by an object of the class ":class:`Status`".  Iterator to access access each record is implemented by the class ":class:`Iterator`".

Installation
============

This package is independent of the core library of Tkrzw.  You don't have to install the core library.  Meanwhile, you have to install the library of gRPC for Python as described in the official document.  Python 3.6 or later is required to use this package.

Enter the directory of the extracted package then perform installation.  If your system has the another command except for the "python3" command, edit the Makefile beforehand.::

 make
 sudo make install

Example
=======

Before running these examples, you have to run a database server by the following command.  It runs the server at the port 1978 on the local machine.::

 tkrzw_server 

The following code is a typical example to use a database.  A RemoteDBM object can be used like a dictionary object.  As RemoteDBM implements the generic iterator protocol, you can access each record with the "for" loop.::

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

 # Traverses records.
 # Retrieved keys and values are always bytes so we decode them.
 for key, value in dbm:
     print(key.decode(), value.decode())

 # Closes the connection.
 dbm.Disconnect()

The following code is a more complex example.  Resources of DBM and Iterator are bound to their objects so when the refenrece count becomes zero, resources are released.  Even if the database is not closed, the destructor closes it implicitly.  The method "OrDie" throws an exception on failure so it is useful for checking errors.::

 import tkrzw_rpc

 # Prepares the database.
 # The timeout is in seconds.
 dbm = tkrzw_rpc.RemoteDBM()
 status = dbm.Connect("localhost:1978", timeout=10)
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

Indices and tables
==================

.. toctree::
   :maxdepth: 4
   :caption: Contents:

   tkrzw_rpc

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
