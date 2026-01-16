#!/usr/bin/env python3

import re
import os
import os.path
import sys
import json
import logging
from enum import Enum, unique
from difflib import ndiff
from datetime import datetime
from collections import defaultdict

@unique
class Side(Enum):
  #Following are "member" declarations
  #Iterating over the enumeration will return these in definition order
  #LHS of a member is its "name", rhs is its "value"
  SYNC  = 'sync' 
  ASYNC = 'async'

@unique
class Action(Enum):
  RCVD = 'rcvd'
  SENT = 'sent'
  DONE = 'done' #IIUC, navigating away with no reference to what happens to data here

@unique
class Type(Enum):
  NAME    = 'name'
  SERVICE = 'service'
  AWAY    = 'all' #Created to pair with Action.DONE. TODO: Is this a good enough solution?

def tokenize(s):
#  print(s)

  #sometimes I insert extra _ to keep field length the same for easier eyeball parsing
  #do this first as it makes a copy of the input string, allowing me to leave the input unmodified
  snap_id = re.sub(r'_+', r'_', s)
#  print(snap_id)

  if snap_id[-5:] != '.json': raise Exception(f'Suffix of {s} is unexpectedly not .json')
  snap_id = snap_id[:-5]
#  print(snap_id)

  parts = snap_id.split('_')
  if parts[-2] == Action.DONE.value: #special case, has no action
    parts.insert(-1, Type.AWAY.value)  #so we insert this special value for the type, which otherwise would be missing
  if len(parts) != 9:    raise Exception(f'Parts of {s} has unexpected length {len(parts)}')
  if parts[0] != 'LatS': raise Exception(f'First part of {s} is unexpectedly {parts[0]} rather than LatS')
#  print(parts)
#  print(s)

  #I rely on these type conversions for validation
  return {
    'seq':    int(parts[1]), #edit within the current session
    'stamp':  datetime.fromisoformat(':'.join(parts[2:5])), #timestamp when the edit occurred
    'pid':    int(parts[5]), #identifier of the record (as in the primary key, not the service or item number)
    'action': Action(parts[6]), #action being recorded (data just received, or data about to be sent)
    'type':   Type(parts[7]), #type of data (name, services, other services, other data)
    'side':   Side(parts[8]), #source of data (synchronous (should be local state) or asynchronous (should be server state) cache)
  }

def describe(path):
  name = os.path.basename(path)
  tokens = tokenize(name)
  tokens['path'] = os.path.abspath(path)
  tokens['basename'] = name
  return tokens

def suck(topdirs):
  result = []
  count = 0
  print('\n\n', file = sys.stderr)
  for dircount, indir in enumerate(topdirs, start = 1):
    print('\033[3A', end = '', file = sys.stderr)
    print('\033[K', end = '', file = sys.stderr)
    print(f'Directory {dircount} / {len(sys.argv) - 1} ({indir})', file = sys.stderr)
    for root, dirs, files in os.walk(indir):
      count += len(files)
      print('\n', file = sys.stderr)
      for fcount, file in enumerate(files, start = 1):
        print('\033[2A', end = '', file = sys.stderr)
        print('\033[K', end = '', file = sys.stderr)
        print(f'  Within which, file {fcount} / {len(files)} ({file})', file = sys.stderr)
        print(file = sys.stderr)
        result.append(describe(os.path.join(root, file)))
        print('\033[A', end = '', file = sys.stderr)
        print('\033[K', end = '', file = sys.stderr)
        print(f'Total completed: {len(result)} / {count} known files', file = sys.stderr)
  return result 

#Pair of descriptions for same stamp and seq should be identical except for:
#  Side, which must be the other kind
#  path, which is necessarily different if the description is not a second read of the same file
#  basename, which is necessarily different if the Side is different
def check_legal_pairing(a, b):
  #confirm that side is different
  if a['side'] == b['side']:
    raise Exception(f'''Multiple descriptions for stamp {a["stamp"]}, seq {a["seq"]} with same Side:
           {a["path"]}
           {b["path"]}''')

  #make shallow copies and edit for partial comparison
  cmp_A = a.copy()
  cmp_B = b.copy()
  for cmp in cmp_A, cmp_B:
    for ignore in ('side', 'path', 'basename'):
      del cmp[ignore]

  #confirm that everything except for expected exceptions is the same
  if cmp_A != cmp_B:
    raise Exception(f'''Description for stamp {a["stamp"]}, seq {a["seq"]} at path
           {a["path"]}
           fails to match non-Side attributes of other description of same stamp, seq at
           {b["path"]}\n''' + '\n'.join(ndiff(json.dumps({k: str(v) for k, v in cmp_A.items()}, indent = 2).splitlines(),
                                              json.dumps({k: str(v) for k, v in cmp_B.items()}, indent = 2).splitlines())))


#Future proofing
if len(Side) != 2:
  raise Exception(f'''Is this still safe if there are not exactly two Sides?
           See particularly the expectation that there are a pair of descriptions with different Side for a given stamp and sequence.''')



#At least some of the above may be reusable elsewhere
#The following addressing immediate need of reconstructing bucket contents from a particular audit trail produed over Christmas holiday 2025-6

logging.basicConfig(format='%(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

log.info('Parsing...')
descriptions = suck(sys.argv[1:])
log.info('Indexing and verifiying...')

#index descriptions by pid
PIDS = defaultdict(list)
for description in descriptions:
  PIDS[description['pid']].append(description)

#index descriptions by stamp and sequence
#each sequence within each stamp should have one sync and one async snapshot
STAMPS = defaultdict(lambda: defaultdict(list))
for description in descriptions:
    current_stamp = description['stamp']
    current_seq   = description['seq']
    if current_stamp in STAMPS:
      existing_stamp = STAMPS[current_stamp]
      if current_seq in existing_stamp:
        existing_seq = existing_stamp[current_seq]
        if len(existing_seq) != 1:
          raise Exception(f'''Description for stamp {current_stamp}, seq {current_seq} recurs in:
           {',\n             '.join([x["path"] for x in existing_seq])} and
           {description["path"]}.
           This is not two times and therefore cannot be due to the expected sync/async pairing.''')

        #can now assume that existing_seq contains exactly one element (until we append to it)
        check_legal_pairing(existing_seq[0], description)
        existing_seq.append(description) #can only get here if check_legal_pairing did not throw an exception
      else: existing_stamp[current_seq].append(description)
    else:
      STAMPS[current_stamp][current_seq].append(description)

#At this point, every entry in STAMPS, SEQS should be a list of 2 snapshots, one sync and one async
multiseq_count = 0
multiseq_cases = []
for stamp, seqs in STAMPS.items():
  if len(seqs) != 1:
    multiseq_count += 1
    for snaps in seqs.values():
      multiseq_cases.extend([x['path'] for x in snaps])
  for seq, snaps in seqs.items():
    if len(snaps) != 2:
      raise Exception(f'Stamp {stamp}, seq {seq} does not have the expected 2 (sync and async) snapshots')

log.info(f'Indexed {len(PIDS)} unique pids')
log.info(f'Indexed {len(STAMPS)} unique stamps, of which {multiseq_count} had multiple sequences.')
if(multiseq_count):
  log.info('\n    '.join(['  Multiseq paths: '] + multiseq_cases))


log.info('Sorting...')
#sort contents of pid index by stamp, then seq (by doing the sort operations in 'wrong' order)
#works because of sort stability (re https://docs.python.org/3/howto/sorting.html#sort-stability-and-complex-sorts)
for desc_list in PIDS.values():
  desc_list.sort(key = lambda x: x['seq'])
  desc_list.sort(key = lambda x: x['stamp'])

sync_sents = []
for pid, snapshots in PIDS.items():
  sent = list(filter(lambda x: x['action'] is Action.SENT and x['side'] is Side.SYNC, snapshots))
  if len(sent):
    sync_sents.extend([f'{pid}:{x["stamp"].strftime("%Y-%m-%d_%H-%M-%S-%f")}:{x["path"]}' for x in sent])

log.info(f'Found {len(sync_sents)} sync-side sends')
print('\n'.join(sync_sents))
