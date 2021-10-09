#!/usr/bin/env bash
set -eu
flake8 *.py
mypy *.py
