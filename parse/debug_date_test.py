#!/usr/bin/env python3

from test_phase3_normalization import test_date_normalizer

try:
    result = test_date_normalizer()
except Exception as e:
    import traceback
    traceback.print_exc()