#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Building configurations
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

from distutils.core import *

package_name = 'Tkrzw-RPC'
package_version = '0.1'
package_description = 'Python client library of Tkrzw-RPC'
package_author = 'Mikio Hirabayashi'
package_author_email = 'hirarin@gmail.com'
package_url = 'http://dbmx.net/tkrzw-rpc/'
package_name = 'tkrzw_rpc'

setup(name = package_name,
      version = package_version,
      description = package_description,
      author = package_author,
      author_email = package_author_email,
      url = package_url,
      packages=[package_name])

# END OF FILE
