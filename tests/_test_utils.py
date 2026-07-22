# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Minimal stand-in for Airflow's ``tests_common.test_utils.config.conf_vars``."""

from __future__ import annotations

import os
from contextlib import contextmanager


@contextmanager
def conf_vars(overrides: dict[tuple[str, str], str | None]):
    from airflow.configuration import conf

    original: dict[tuple[str, str], str | None] = {}
    original_env: dict[str, str] = {}
    for (section, key), value in overrides.items():
        env_var = conf._env_var_name(section, key)
        if env_var in os.environ:
            original_env[env_var] = os.environ.pop(env_var)
        if conf.has_option(section, key):
            original[(section, key)] = conf.get(section, key)
        else:
            original[(section, key)] = None
        if value is None:
            conf.remove_option(section, key)
        else:
            if not conf.has_section(section):
                conf.add_section(section)
            conf.set(section, key, value)
    try:
        yield
    finally:
        for (section, key), value in original.items():
            if value is None:
                conf.remove_option(section, key)
            else:
                conf.set(section, key, value)
        os.environ.update(original_env)
