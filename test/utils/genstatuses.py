#!/usr/bin/env python3
import sys

def status_entry(last, entry = 1):
  while(entry <= last):
    result = f'''
    {{
      "gen_item": null,
      "source_reference": null,
      "person_id": {9999999900 + entry},
      "tr1": null,
      "tr2": null,
      "complete1": false,
      "complete2": false,
      "reconciled": null,
      "notww1": null
    }}''';
    entry += 1;
    yield result;

print(','.join(x for x in status_entry(int(sys.argv[1]))));
