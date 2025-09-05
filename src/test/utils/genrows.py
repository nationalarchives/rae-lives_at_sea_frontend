#!/usr/bin/env python3
import sys

def service_entry(identifier, last, row = 1):
  while(row <= last):
    result = f'''
          {{
            "row_number": {row},
            "ship": "{identifier}{row}_ship",
            "rating": "{identifier}{row}_rating",
            "fromday": 0,
            "frommonth": 0,
            "fromyear": 0,
            "today": 0,
            "tomonth": 0,
            "toyear": 0
          }}''';
    row += 1;
    yield result;

print(','.join(x for x in service_entry(sys.argv[1], int(sys.argv[2]))));
