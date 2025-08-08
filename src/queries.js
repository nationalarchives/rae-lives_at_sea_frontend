import { createStore, useStore } from 'zustand';
import { init_data, status_encode, normalize, PERSON_FIELD_TYPES, SERVICE_FIELD_TYPES, OTHER_SERVICE_FIELD_TYPES, OTHER_DATA_FIELD_TYPES } from './data_utils';
import { useQuery, useQueryClient, useMutation } from '@tanstack/react-query';
import { isEqual } from 'lodash';
import { fetchAuthSession } from 'aws-amplify/auth';

//following https://stackoverflow.com/a/1479341
//exporting only for testing purposes -- seems harmless, but perhaps could be avoided with mocks or spies
export const RECORDS = (function() { //TODO: Is this a global singleton?
  //private
  const map = new Map();
  function _key(sailorType, nameId, selection) { return `${sailorType}:${nameId}:${selection}` };

  //public
  return {
    has:    function(sailorType, nameId, selection) { return map.has       (_key(sailorType, nameId, selection)) },
    get:    function(sailorType, nameId, selection) { return map.get       (_key(sailorType, nameId, selection)) },
    delete: function(sailorType, nameId, selection) { return map.delete    (_key(sailorType, nameId, selection)) },
    set:    function(sailorType, nameId, selection, value) { return map.set(_key(sailorType, nameId, selection), value) },
    clear:  function() { return map.clear(); }, //only used in testing at time of writing
  }
})();

function getRecord(sailorType, nameId, selection, query) {
  if(!RECORDS.has(sailorType, nameId, selection)) {
    if(query.status === 'success') {
      RECORDS.set(sailorType, nameId, selection,
                  createStore((set) => ({
                    [selection]: structuredClone(query.data),
                    update: (value) => set({[selection]: value}),
                  })));
    }
  }

  // Two possibilities at this point:
  // 1. The record contains some data. In that case, this sync state has priority over whatever might be on the server.
  // 2. The records does not contain data. In that case:
  // 2a.  There is a successful query in the query cache. Load the sync state with this current async state.
  // 2b.  There is not a successful query in the query cache. Leave the sync state empty.
  //        Note that this means that is a query is pending, the sync state record will remain empty. This should at least be safe:
  //        user will see all data disappear, or null dereferences will cause exceptions, so we should not end up writing bad state
  //        back to the server.

  return [
    RECORDS.get(sailorType, nameId, selection) || createStore(() => ({[selection]: null})),
    query.status,
  ];
}

function fetchData(params) {
  const api = import.meta.env.VITE_API_ROOT + params;
  return new Promise((resolve, reject) => {
    const fetchData = async() => {
      const response = await fetch(api);
      if(!response.ok) {
        let msg = '' + response.status;
        if(response.statusText) msg += `: ${response.statusText}`;
        reject(new Error(msg, { cause: response }));
      }
      else {
        resolve(response.json());
      }
    }
    fetchData();
  });
}

function postData(params, body) {
  const api = import.meta.env.VITE_API_ROOT + params;
  return new Promise((resolve, reject) => {
    const fetchData = async() => {
      const session = await fetchAuthSession();
      const token = session.tokens?.idToken?.toString();
      const response = await fetch(api, {
        method: 'POST',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
          'Authorization': token || '',
        },
        body: JSON.stringify(body),
      });
      if(!response.ok) {
        let msg = '' + response.status;
        if(response.statusText) msg += `: ${response.statusText}`;
        reject(new Error(msg, { cause: response }));
      }
      else {
        resolve(response);
      }
    }
    fetchData();
  });
}

//following https://stackoverflow.com/a/78205295
//preserves order, optionally requires all expected properties to exist in propMap
//SHALLOW copy permits quick operation.
export function rename_properties(obj, propMap, strict = true) {
  const newObj = {};
  const mismatch = strict ? (k, v) => { console.log(`Unexpected property ${k}`); throw Error(`Unexpected property ${k}`); }
                          : (k, v) => { obj[k] = v; }
  for(const [k, v] of Object.entries(obj)) {
    if(k in propMap) newObj[propMap[k]] = v;
    else mismatch(k, v);
  }
  return newObj;
}

function translateFromAPI(apiData) {
  function translateServiceHeader(x) {
    return rename_properties(x, {
      md5_hash: 'md5_hash',
      user_id: 'userid',
      step: 'step',
      complete: 'complete',
      rows: 'records',
    });
  }
  function translateMainRecords(x) {
    const translatedRecords = [];
    if(x.rows !== null) {
      for(const y of x.rows) {
        translatedRecords.push(rename_properties(y, {
          row_number: 'rowid',
          ship: 'ship',
          rating: 'rating',
          officer: 'officer',
          fromday: 'fromday',
          frommonth: 'frommonth',
          fromyear: 'fromyear',
          today: 'today',
          tomonth: 'tomonth',
          toyear: 'toyear',
          source_id: 'source_id',
        }));
      }
    }
    return translatedRecords;
  }
  function translateOtherServiceRecords(x, source_data) {
    const translatedRecords = [];
    if(x.rows !== null) {
      for(const y of x.rows) {
        const translation = rename_properties(y, {
          row_number: 'row',
          ship: 'ship',
          rating: 'rating',
          officer: 'officer',
          fromday: 'fromday',
          frommonth: 'frommonth',
          fromyear: 'fromyear',
          today: 'today',
          tomonth: 'tomonth',
          toyear: 'toyear',
          source_id: 'sourceid',
        });

        //TODO: check with Mark! Am I translating the sources correctly?
        if(source_data[translation.sourceid]) { //TODO: This situation shouldn't arise, but does with the current implementation
                                                //      as I cannot yet enter sources. Should handle this as an error (or just
                                                //      let it put the query into an error state, as it will do without this if
                                                //      condition), but leave like this for now while developing.
          let last_part = source_data[translation.sourceid];
          const source_parts = [last_part];
          while(last_part.source_collection_id === 0) {
            last_part = source_data[last_part.parent_id];
            source_parts.push(last_part);
          }
          if(last_part.source_collection_id === 0) { //we were NOT able to find a "source collection"
          }
          else {
            translation.reference = last_part.collection_name;
            translation.piece = source_parts.at(-2).part_reference || null;
            translation.subref = source_parts.at(-3).part_reference || null;
          }
        }

        translatedRecords.push(translation);
      }
    }
    return translatedRecords;
  }

  //within this function, it is safe to refer to (as opposed to have to copy) data in the object that we already have from the API
  const appData = rename_properties(apiData, {
    source_lookup: 'source_lookup',
    person: 'name', //rename this key for app format
    source: 'source',
    service: 'services', //rename this key (alarmingly subtly) for app format
    versions: 'versions',
  });

  //FIXME: Get the catalogue info in a robust fashion
  //Order is not so important for these, they will get deleted before POST
  [ appData.name.series, appData.name.piece, appData.name.item ] = appData.source[0].source_reference.split('^').slice(1,4);

  //services need a lot of extra translation
  if(Object.hasOwn(apiData.service, 'MAIN')) {
    if(apiData.service.MAIN.length !== 1 && apiData.service.MAIN.length !== 2) {
      throw Error(`Unexpected nunber of service history transcriptions: ${appData.services.length}`);
    }
    const services = [];
    for(const x of apiData.service.MAIN) {
      const currentServices = translateServiceHeader(x);
      currentServices.records = translateMainRecords(x);
      services.push(currentServices);
    }
    appData.services = {
      reconciled: apiData.service.MAIN.every((x) => x.step === 'RECONCILE'),
      services:   services,
    };
  }
  else {
    throw Error('No service data');
  }
  if(Object.hasOwn(apiData.service, 'OTHER')) {
    if(apiData.service.OTHER.length !== 1) {
      throw Error('Unexpected other service length');
    }
    /* The full returned data is not currently used in the front end, just the records.
       So just read the records, but keep this comment as a record of how to read it 'properly'.
    appData.service_other = translateServiceHeader(apiData.service.OTHER[0]);
    appData.service_other.records = translateOtherServiceRecords(apiData.service.OTHER[0], apiData.source_lookup);
    */
    appData.service_other_header = apiData.service.OTHER[0]; //not used by app, no need to translate
    appData.service_other = translateOtherServiceRecords(apiData.service.OTHER[0], apiData.source_lookup);
  }

  return appData;
}

function translateToAPI(appData) {
  //expect to have been passed data that is safe to manipulate (i.e. cloned from, not referring to, cache)
  const apiData = rename_properties(appData, {
    source_lookup: 'source_lookup',
    name: 'person', //rename this key from app format
    source: 'source',
    services: 'service', //rename this key (alarmingly subtly) from app format
    versions: 'versions',
    service_other_header: 'service_other_header', //temporary, removed later in function
    service_other: 'service_other', //temporary, removed later in function
  });

  delete apiData.person.series;
  delete apiData.person.piece;
  delete apiData.person.item;

  if(appData.services) {
    apiData.service = {
      MAIN: [],
    };
    for(const [index, x] of appData.services.services.entries()) {
      const currentServices = rename_properties(x, {
        md5_hash: 'md5_hash',
        userid: 'user_id',
        step: 'step',
        complete: 'complete',
        records: 'rows',
      });

      //handle status
      if(appData.services.reconciled) {
        currentServices.step = 'RECONCILE';
      }
      else {
        currentServices.step = 'TRANSCRIBE' + (index + 1);
      }
      if(currentServices.rows.length === 0) {
        currentServices.rows = null;
      }
      else {
        currentServices.rows = [];
        for(const y of x.records) {
          currentServices.rows.push(rename_properties(y, {
            rowid: 'row_number',
            ship: 'ship',
            rating: 'rating',
            officer: 'officer',
            fromday: 'fromday',
            frommonth: 'frommonth',
            fromyear: 'fromyear',
            today: 'today',
            tomonth: 'tomonth',
            toyear: 'toyear',
            source_id: 'source_id',
          }));
        }
      }
      apiData.service.MAIN.push(currentServices);
    }
  }
  else {
    throw Error('No service data');
  }
  if(appData.service_other) {
    apiData.service.OTHER = [appData.service_other_header]; //not used by app, wasn't translated
    apiData.service.OTHER[0].rows = [];
    for(const x of appData.service_other) {
      delete x.reference;
      delete x.piece;
      delete x.subref;
      //TODO: Convert reference, piece, subref into a source_id (or arrange the interface such that it is always stored in this form)
      apiData.service.OTHER[0].rows.push(rename_properties(x, {
        row: 'row_number',
        ship: 'ship',
        rating: 'rating',
        officer: 'officer',
        fromday: 'fromday',
        frommonth: 'frommonth',
        fromyear: 'fromyear',
        today: 'today',
        tomonth: 'tomonth',
        toyear: 'toyear',
        sourceid: 'source_id',
      }));
    }
  }
  delete apiData.service_other_header;
  delete apiData.service_other;
  return apiData;
}

function mainPersonQF({queryKey}) {
  const [, {sailorType, nameId}] = queryKey;
  if(typeof(nameId) === 'undefined') {
    return new Promise((resolve, reject) => reject(new Error()));
  }
  else if(nameId === 0) {
    if(sailorType === 'officer') { //TODO: Remove this when the API provides the information
      return new Promise((resolve) => resolve(init_data(sailorType)));
    }
    else if(sailorType === 'rating') {
      const promises = [];
      for(const schema of ['name', 'service']) {
        promises.push(fetchData('schema/' + schema));
      }
      const promise = Promise.all(promises);
      return promise.then((schemata) => {
        const [name, services] = schemata;
        for(const key in name) {
          if(name[key] === 0) { name[key] = '';}
        }
        return {
          name: name,
          services: [structuredClone(services[0]), services[0]], //TODO: This is a workaround until the UI understands records with a single service history
          status: {"status_code": 0, "description": "Not Started"}, //TODO: Update if the API starts providing this information (but does it need to?)
          other_data: [], //TODO: Update if the API starts providing this information (but does it need to?)
          service_other: [], //TODO: Update if the API starts providing this information (but does it need to?)
      }});
    }
    else {
      return new Promise((resolve, reject) => reject(new Error('Bad sailor type' + sailorType)));
    }
  }
  else {
    if(sailorType === 'rating') {
      return new Promise((resolve, reject) => {
        fetchData('person/lastpost?personid=' + nameId).then(
          (bucketData) => {
            import.meta.env.MODE !== 'test' && console.log(`Retrieved personid ${nameId} from bucket`);
            if(bucketData.person.person_id !== Number(nameId)) throw new Error('Person id mismatch');
            if('L@SRecordsReturned' in bucketData && bucketData['L@SRecordsReturned'] === 1) {
              delete bucketData['L@SRecordsReturned']; //this is wrapped by the API, we don't want it internally (or we'll end up writing back to the bucket, then the API might try to add it again, horrors may ensue
              resolve(bucketData); //if it worked, just return the JSON
            }
            else { // 200, so we found something in the bucket -- but it did not have the expected identifying key/value, so blow up
              reject(new Error('Bad record count in response'), { cause: bucketData });
            }
          },
          (err) => {
            if(err.cause.status === 404) {
              err.cause.json().then(
                (jsonbody) => {
                  if('L@SRecordsReturned' in jsonbody && jsonbody['L@SRecordsReturned'] === 0) { //failed lastpost lookup, try the database
                    resolve(fetchData('person?personid=' + nameId).then((dbData) => {
                      import.meta.env.MODE !== 'test' && console.log(`Retrieved personid ${nameId} from database`);
                      if(dbData.person.person_id !== Number(nameId)) throw new Error('Person id mismatch');
                      return dbData;
                    }));
                  }
                  else { //Bad 404 -- contains JSON, but not the expected key/value
                    reject(err);
                  }
                },
                () => { //Bad 404 -- JSON not decodable (e.g. because it is text)
                  reject(err);
                }
              );
            }
            else { //Bad error -- not a 404
              reject(err);
            }
          }
        );
      }).then((apiData)=>translateFromAPI(apiData));
    }
    else if(sailorType === 'officer') {
      return Promise.reject(new Error('Officers not yet implemented'));
    }
    else {
      return new Promise((resolve, reject) => reject(new Error('Bad sailor type' + sailorType)));
    }
  }
}

function pieceQF({queryKey}) {
  const [, piece] = queryKey;
  return new Promise((resolve, reject) => {
    fetchData('status/piece_summary/last_piecesummary?piece_number=' + piece).then(
    (bucketData) => {
      import.meta.env.MODE !== 'test' && console.log(`Retrieved piece summary ${piece} from bucket`);
      resolve(bucketData); //if it worked, just return the JSON
    },
    (err) => {
      if(err.cause.status === 404) { //failed last_piecesummary lookup, try the database
        resolve(fetchData('status/piece_summary?piece_number=' + piece).then((dbData) => {
          import.meta.env.MODE !== 'test' && console.log(`Retrieved piece summary ${piece} from database`);
          return dbData;
        }));
      }
      else {
        reject(err);
      }
    }
  )});
}

function piecesQF() {
  return new Promise((resolve) => {
    resolve([ /* Could be an API call, but hard coded to the known values for now */
      5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,513,514,515,516,517,518,519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,696,697,698,699,700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,761,762,763,764,765,766,767,768,769,770,771,772,773,774,775,776,777,778,779,780,781,782,783,784,785,786,787,788,789,790,791,792,793,794,795,796,797,798,799,800,801,802,803,804,805,806,807,808,809,810,811,812,813,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,867,868,869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893,894,895,896,897,898,899,900,901,902,903,904,905,906,907,908,909,910,911,912,913,914,915,916,917,918,919,920,921,922,923,924,925,926,927,928,929,930,931,932,933,934,935,936,937,938,939,940,941,942,943,944,945,946,947,948,949,950,951,952,953,954,955,956,957,958,959,960,961,962,963,964,965,966,967,968,969,970,971,988,989,990,991,992,993,994,995,996,997,998,999,1000,1001,1002,1003,1004,1005,1006,1007,1008,1009,1010,1011,1018,1019,1020,1021,1022,1023,1024,1025,1026,1027,1028,1029,1030,1031,1032,1033,1034,1035,1036,1037,1038,1039,1040,1041,1042,1043,1044,1045,1046,1047,1048,1049,1050,1051,1052,1053,1054,1055,1056,1057,1058,1059,1060,1061,1062,1063,1064,1065,1066,1067,1068,1069,1070,1071,1072,1073,1074,1075,1076,1077,1078,1079,1080,1081,1082,1083,1084,1085,1086,1087,1094,
    ]);
  });
}

//TODO: Temporary hack: dump current state to bucket and force re-fetch
async function updatePieceBucket(queryClient, mainData) {
  const queryOpts = pieceQuery('' + mainData.name.piece);
  const pieceData = structuredClone(await queryClient.ensureQueryData(queryOpts));
  let found = false;
  for(const record of pieceData.records) {
    if(Number(record.gen_item) === Number(mainData.name.item)) {
      const tables = mainData.services.services;
      record.complete1  = tables[0].complete;
      record.tr1        = tables[0].userid;
      record.reconciled = mainData.services.reconciled;
      record.notww1     = mainData.name.notww1;
      if(tables.length === 1) {
        record.complete2 = record.complete1;
        record.tr2       = record.tr1;
      }
      else {
        record.complete2  = tables[1].complete;
        record.tr2        = tables[1].userid;
      }
      found = true;
      break;
    }
  }
  if(found) {
    return postData(`status/piece_summary/last_piecesummary?piece_number=${mainData.name.piece}`, pieceData).then(()=>{
      return queryClient.invalidateQueries({queryKey: queryOpts.queryKey});
    });
  }
  else {
    import.meta.env.MODE !== 'test' && console.warn(`No matching piece state found for person with item ${mainData.name.item} in piece ${mainData.name.piece}`);
  }
}
export function initPieceBucket(queryClient) {
  queryClient.getQueryCache().subscribe((e) => {
    if(e.type === 'updated' && e.action.type === 'success' && e.query.queryKey[0] === 'mainPersonData') {
      updatePieceBucket(queryClient, e.query.state.data);
    }
  });
}
//END TEMPORARY HACK

function mainPersonMutate(queryClient, sailorType, nameId, data) {
  const key = mainPersonQuery(sailorType, nameId).queryKey;
  const newClientData = structuredClone(queryClient.getQueryData(key));
  newClientData.name = normalize(structuredClone(data), PERSON_FIELD_TYPES);
  return postData('person', translateToAPI(newClientData));
}

function serviceRecordsMutate(queryClient, sailorType, nameId, data) {
  const key = mainPersonQuery(sailorType, nameId).queryKey;
  const newClientData = structuredClone(queryClient.getQueryData(key));
  newClientData.services = structuredClone(data);
  for(const table of newClientData.services.services) {
    for(const row of table.records) {
      normalize(row, SERVICE_FIELD_TYPES);
    }
  }
  return postData('person', translateToAPI(newClientData));
}

function otherDataMutate(queryClient, sailorType, nameId, data) {
  const key = mainPersonQuery(sailorType, nameId).queryKey;
  const newClientData = structuredClone(queryClient.getQueryData(key));
  newClientData.other_data = normalize(structuredClone(data), OTHER_DATA_FIELD_TYPES);
  return postData('person', translateToAPI(newClientData));
}

function otherServicesMutate(queryClient, sailorType, nameId, data) {
  const key = mainPersonQuery(sailorType, nameId).queryKey;
  const newClientData = structuredClone(queryClient.getQueryData(key));
  newClientData.service_other = normalize(structuredClone(data), OTHER_SERVICE_FIELD_TYPES);
  return postData('person', translateToAPI(newClientData));
}

/* Inefficient -- looks up the same data twice.
   But this is a simple way to ensure that it ends up in the
   correct place in the cache.
 */
export const refToPersonIdQuery = (piece, item) => ({
  queryKey: ['refToPersonId', {department: 'ADM', series: '188', piece: piece, item: item}],
  queryFn: ({queryKey}) => fetchData(`person?sourcereference=ADM&sourcereference=188&sourcereference=${piece}&sourcereference=${item}`),
  select: (x) => x.person.person_id,
  staleTime: Infinity,
});

const mainPersonQuery = (sailorType, nameId) => ({
  queryKey: ['mainPersonData', {sailorType: sailorType, nameId: Number(nameId)}],
  queryFn: mainPersonQF,
  select: (x) => x.name,
  staleTime: Infinity,
});

const serviceRecordsQuery = (sailorType, nameId) => ({
  queryKey: ['mainPersonData', {sailorType: sailorType, nameId: Number(nameId)}],
  queryFn: mainPersonQF,
  select: (x) => { return x.services },
  staleTime: Infinity,
});

const otherServicesQuery = (sailorType, nameId) => ({
  queryKey: ['mainPersonData', {sailorType: sailorType, nameId: Number(nameId)}],
  queryFn: mainPersonQF,
  select: (x) => ( x.service_other ),
  staleTime: Infinity,
});

const otherDataQuery = (sailorType, nameId) => ({
  queryKey: ['mainPersonData', {sailorType: sailorType, nameId: Number(nameId)}],
  queryFn: mainPersonQF,
  select: (x) => ( x.other_data ),
  staleTime: Infinity
});

export const pieceQuery = (piece) => ({
  queryKey: ['piece', piece],
  queryFn: pieceQF,
  staleTime: Infinity
});

export const piecesQuery = {
  queryKey: ['pieces', 188],
  queryFn: piecesQF,
  staleTime: Infinity
};

function simpleTableQueryFn({queryKey}) {
  const [, table] = queryKey;
  return Promise.reject(new Error('simpleTableQueryFn not yet implemented'));
}

export const simpleTableQuery = (table) => ({
    queryKey: ['simpleEditor', table],
    queryFn: simpleTableQueryFn,
    staleTime: Infinity,
});

export function useRecord(sailorType, nameId, selection) {
  const queries = {
    name: mainPersonQuery,
    service: serviceRecordsQuery,
    service_other: otherServicesQuery,
    data_other: otherDataQuery,
  }
  const mutations = {
    name: mainPersonMutate,
    service: serviceRecordsMutate,
    service_other: otherServicesMutate,
    data_other: otherDataMutate,
  }

  const key = queries[selection](sailorType, nameId).queryKey;
  const query = useQuery(queries[selection](sailorType, nameId));
  const queryClient = useQueryClient();

  /*
  //Could use this to respond to cache events, but seems better to do it in mutate functions.
  //Have kept this one here as an example of responding to the cache being cleared, but I'm
  //not sure of the timing implications and this is a case that only arises inside the test framework.
  //Assumes presence of a 'clear' function in RECORDS which just calls map.clear()
  queryClient.getQueryCache().subscribe((e)=>{ //TODO: Race conditions here?
    e.type === 'removed' && RECORDS.clear(); //clear the map if the query cache gets reset
  });
  */

  const [record, queryStatus] = getRecord(sailorType, nameId, selection, query);
  const mutation = useMutation({
    mutationFn: (newData) => {
      return mutations[selection](queryClient, sailorType, nameId, newData);
    },
    scope: { id: 'ATOMIC' }, //mutations in the same scope run in series. this ensures that updates are completed before data is gathered for the next mutation.
    //onMutate: () => {console.log('   About to mutate', selection); }, //Perhaps worth noting that this callback fires before mutations get scope-paused -- but this is pure observation, I do not know whether this is a defined behaviour
    onSuccess: () => {
      return queryClient.invalidateQueries({queryKey: key});
      //lengthy study of the not-quite-good-enough useQuery docs, combined with experimentation,
      //indicates that invalidateQueries returns a promise that resolves when the refetch consequent
      //upon this invalidation completes. the mutation remains unsettled until the promise returned
      //from this callback resolves. therefore, at the point that this mutation resolves (and therefore
      //another mutation in the same scope can begin), the cache is up to date. This is sufficiently
      //correct for the next mutation.
    }
  });

  return {
    data: useStore(record, (state)=>state[selection]),
    setData: useStore(record, (state)=>state.update),
    mutation: mutation,
    queryData: query.data,
    status: queryStatus,
  };
}

//This is UI functionality, shouldn't really be here. But it's always going to be needed along with this stuff. And life is short.
export function failedMutationDialog(dialogs, mutation) {
  return (error, variables) => {
    dialogs.confirm(`Save failed due to Error ${error.message}. Try again or contact your developer with detailed error message.`,
                  {
                    title: 'Failed save',
                    cancelText: 'Details',
                  }).then((ok)=>{
                    //mutation.reset(); //feels dirty to do this while it is handling the error but should be fine
                                      //so long as this is the last thing that we do with this mutation
                                      //operation -- which it very much should be
                    if(!ok) {
                      dialogs.alert(`${error.message} ${JSON.stringify(variables)} ${error.stack}`, { title: 'Copy this text for your developer', });
                    }
                  });
  };
}
