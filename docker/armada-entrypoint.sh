#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Copy ARMADA_GANG* env vars to SPARK_EXECUTOR_ATTRIBUTE_* so they get sent
# to the driver during executor registration. In dynamic client mode the
# driver runs outside the cluster and lacks these vars; forwarding them as
# executor attributes lets the driver discover the gang node selector at
# runtime and use it for subsequent executor allocations.
for var in $(env | grep '^ARMADA_GANG' | cut -d= -f1); do
  export "SPARK_EXECUTOR_ATTRIBUTE_$var=${!var}"
done

exec /opt/entrypoint.sh "$@"
