"""Shared test helpers: ``conf_vars`` stand-in and CLI output parsing."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager


def published_payload(cli_output: str) -> dict:
    """
    Extract the publisher command's JSON payload from captured stdout.

    DagBundlesManager may log to stdout before the command prints its payload — on
    Airflow 3.0 including single-quoted dicts that defeat a find-the-first-brace
    heuristic. The payload is the last block whose line is exactly ``{``.
    """
    lines = cli_output.splitlines()
    starts = [index for index, line in enumerate(lines) if line == "{"]
    # Fail with the captured output: a missing payload means the command itself
    # failed, and that output is what diagnoses it.
    assert starts, f"no JSON payload found in CLI output:\n{cli_output}"
    return json.loads("\n".join(lines[starts[-1] :]))


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
