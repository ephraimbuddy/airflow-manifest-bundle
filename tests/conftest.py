from __future__ import annotations

import os
import tempfile

# Isolate the test run from any real Airflow deployment. Must happen before the first
# airflow import anywhere in the test session; pytest loads conftest.py first.
if "AIRFLOW_HOME" not in os.environ:
    os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="airflow-manifest-bundle-tests-")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
