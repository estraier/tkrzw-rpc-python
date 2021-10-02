Module and Classes
==================

.. autosummary::
   :nosignatures:

   tkrzw_rpc
   tkrzw_rpc.Status
   tkrzw_rpc.RemoteDBM

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

The following code is a typical example to use a database.  A DBM object can be used like a dictionary object.  As DBM implements the generic iterator protocol, you can access each record with the "for" loop.::

 import tkrzw_rpc

 # Prepares the database.
 dbm = tkrzw.DBM()
 dbm.Open("casket.tkh", True, truncate=True, num_buckets=100)

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
 except tkrzw.StatusException as e:
     print(repr(e))

 # Traverses records.
 # Retrieved keys and values are always bytes so we decode them.
 for key, value in dbm:
     print(key.decode(), value.decode())

 # Closes the database.
 dbm.Close()

Indices and tables
==================

.. toctree::
   :maxdepth: 4
   :caption: Contents:

   tkrzw_rpc

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
