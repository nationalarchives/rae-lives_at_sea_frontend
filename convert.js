//console.log(import.meta);
//import.meta.env = {
//  VITE_API_ROOT: 'https://ofktct1tij.execute-api.eu-west-2.amazonaws.com/development/',
//};
//console.log(import.meta);
//console.log(import.meta.env);

import { mainPersonQF, translateToAPI, translateFromAPI } from './src/queries.js';
import { normalize, PERSON_FIELD_TYPES, SERVICE_FIELD_TYPES } from './src/data_utils.js';
import fs from 'node:fs';
import readline from 'node:readline';
import process from 'process';

const LAST_SEEN = {};

async function getAsyncData(pid, type) {
  if(LAST_SEEN.hasOwnProperty(pid)) {
    console.log('  Retrieved personid ' + pid + ' from previous sight');
  }
  else {
    const cache_name = '../cache/api^' + pid + '.json';
    if(fs.existsSync(cache_name)) {
      console.log('  Retrieved personid ' + pid + ' from cache');
      LAST_SEEN[pid] = translateFromAPI(JSON.parse(fs.readFileSync(cache_name)));
    }
    else {
      process.stdout.write('  '); //so that retrieval messages from queries.js have correct indentation
      LAST_SEEN[pid] = await mainPersonQF({queryKey:
        [, {
          sailorType: 'rating',
          nameId: pid,
        }]
      });
    }
  }
  return structuredClone(LAST_SEEN[pid]);
}

const rl = readline.createInterface({
  input: process.stdin,
});

for await (const line of rl) {
  const parts = line.split(':');
  if(parts.length != 4) process.exit(1);
  const [ pid, type, stamp, path ] = parts;
  console.log('Processing', pid, type, stamp, path);
  const async_data = await getAsyncData(pid);
  const sync_data = JSON.parse(fs.readFileSync(path));
  if(type == 'name')         async_data.name = normalize(sync_data.name, PERSON_FIELD_TYPES);
  else if(type == 'service') async_data.services = sync_data.service;
  else throw new Error('Unexpected Type ' + type);
  for(const table of async_data.services.services) {
    for(const row of table.records) {
      normalize(row, SERVICE_FIELD_TYPES);
    }
  }

  if(JSON.stringify(async_data) === JSON.stringify(LAST_SEEN[pid])) {
    console.log('  Updated (NOP)');
  //switched off because later validation with the validation tools will see the NOP audits
  //  console.log('  Skipped NOP update');
  //  continue;
  }
  else {
    console.log('  Updated ' + type);
  }
  LAST_SEEN[pid] = structuredClone(async_data);
  fs.writeFileSync('output/' + pid + '_' + stamp + '.json', JSON.stringify(translateToAPI(async_data)), { flag: 'wx' , flush: true });
}
