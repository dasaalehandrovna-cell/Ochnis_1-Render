#!/usr/bin/env python3
"""OCHNIS 13.5 thin compatibility entrypoint.

The historical source catalogs are flattened before deployment into runtime_flat.py.
Runtime startup therefore performs no AST source-catalog scan and no dynamic source reconstruction.
"""
from __future__ import annotations

import os
from runtime_config import install_internal_runtime_config

install_internal_runtime_config('front')

import runtime_flat as _runtime

OCHNIS_RELEASE = 'очнись_13.5'
main = _runtime.main
app = _runtime.app
bot = _runtime.bot


def __getattr__(name):
    return getattr(_runtime, name)


if __name__ == '__main__' and str(os.getenv('BOT_DEFER_MAIN_R54','0') or '0').strip().casefold() not in {'1','true','yes','on'}:
    main()
