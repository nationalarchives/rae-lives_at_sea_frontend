import { useBlocker, matchRoutes } from 'react-router';
import { useRecord } from './queries';
import { createContext } from 'react';
import { isEqual } from 'lodash';
import { normalize, PERSON_FIELD_TYPES, SERVICE_FIELD_TYPES } from './data_utils';
import { useParams } from 'react-router';
import { snapshot } from './snapshot';

class Dirty {
  any() {
    return Object.values(this).some((x)=>x);
  }
}

class DirtySailor extends Dirty {
  constructor(name, service, service_other, data_other) {
    super();
    this.name = name;
    this.service = service;
    this.service_other = service_other;
    this.data_other = data_other;
  }
}


export const DirtySailorContext = createContext(new DirtySailor(false, false, false, false));

export function useDirtySailor(sailorType, nameId) {
  const {data: nameRecord, queryData: nameQuery } = useRecord(sailorType, nameId, 'name');
  const {data: servicesRecord, queryData: servicesQuery } = useRecord(sailorType, nameId, 'service');
  const {data: otherServicesRecord, queryData: otherServicesQuery } = useRecord(sailorType, nameId, 'service_other');
  const {data: otherDataRecord, queryData: otherDataQuery} = useRecord(sailorType, nameId, 'data_other');

  const clonedNameRecord = structuredClone(nameRecord);
  if(clonedNameRecord) {
    normalize(clonedNameRecord, PERSON_FIELD_TYPES);
  }

  const clonedServicesRecord = structuredClone(servicesRecord);
  if(clonedServicesRecord) {
    for(const table of clonedServicesRecord.services) {
      for(const row of table.records) {
        normalize(row, SERVICE_FIELD_TYPES);
      }
    }
  }

  return new DirtySailor(
    !(isEqual(clonedNameRecord, nameQuery)),
    !(isEqual(clonedServicesRecord, servicesQuery)),
    !(isEqual(otherServicesRecord, otherServicesQuery)),
    !(isEqual(otherDataRecord, otherDataQuery)),
  );
}

//Block if we are navigating away from the current sailor's record and there is any dirty data
export function useDirtySailorBlocker(dirty) {
  const {sailorType, nameId} = useParams();
  const personRecord  = useRecord(sailorType, nameId, 'name');
  const serviceRecord = useRecord(sailorType, nameId, 'service');
  function block({currentLocation, nextLocation}) {
    function getMatchParams(loc) {
      const matches = matchRoutes([
        {path: 'person/:sailorType/:nameId/:dataType'},
      ], loc);

      //no matches
      if(matches === null || matches.length === 0) return null;

      //react-router documentation is awful. This function returns an array, I presume in case we match more than one of the given routes.
      if(matches.length > 1) throw new Error('Too many matches'); //Just blow up if the return is surprising

      const params = matches[0].params;
      //throw an error if my assumptions break
      //(breakage here is on me, not on the bad react-router documentation)
      const keys = Object.keys(params);
      if(!(keys.length === 3 &&
           keys.includes('sailorType') &&
           keys.includes('nameId') &&
           keys.includes('dataType'))) {
        throw new Error('Bad params: ' + JSON.stringify(params));
      }
      return params;
    }

    const current = getMatchParams(currentLocation); //is from current location, so should always match for current contexts
    const next = getMatchParams(nextLocation); //may be null if we are navigating away

    if(current === null) throw new Error('current did not match expected route type');
    if(next !== null) {
      if(current.sailorType === next.sailorType &&
         current.nameId === next.nameId) {
        return false;
      }
    }
    if(!(dirty.any())) { //we are about to navigate away
      snapshot('done', false, {
        name: personRecord,
        service: serviceRecord,
      }, nameId);
    }
    return dirty.any();
  }

  return(useBlocker(block));
}
