export function catref(data) {
  return 'ADM ' + data.series + '/' + data.piece + '/' + data.item;
}

export function officerref(data) {
  let retval = data.nameid;
  if(data.rating || data.forename || data.surname) {
    retval += ':';
    if(data.rating) {
      retval += ' ' + data.rating;
    }
    if(data.forename) {
      retval += ' ' + data.forename;
    }
    if(data.surname) {
      retval += ' ' + data.surname;
    }
  }
  return retval;
}

export function surname_officerref(data) {
  return (data.surname ? data.surname: '<unknown>') + ', ' + (data.forename ? data.forename : '<unknown>');
}

export const OFFICER_FIELDS = [
    'forename',
    'surname',
    'birthday',
    'birthmonth',
    'birthyear',
    'birthplace',
    'birthcounty',
];

//HTML5 field types
export const PERSON_FIELD_TYPES = {
    forename:        'text',
    surname:         'text',
    officialnumber:  'text', //can have a letter prefix
    birthday:        'number',
    birthmonth:      'number',
    birthyear:       'number',
    birthplace:      'text',
    birthcounty:     'text',
    occupation:      'text',
    dischargeday:    'number',
    dischargemonth:  'number',
    dischargeyear:   'number',
    dischargereason: 'text',
};

export const SERVICE_FIELD_TYPES = {
  ship:      'text',
  rating:    'text',
  fromday:   'number',
  frommonth: 'number',
  fromyear:  'number',
  today:     'number',
  tomonth:   'number',
  toyear:    'number',
}

export const OTHER_SERVICE_FIELD_TYPES = {
  reference: 'text',
  piece: 'text',
  subref: 'text',
  ...SERVICE_FIELD_TYPES,
}

export const OTHER_DATA_FIELD_TYPES = {
  sourceid: 'text',
  subref:   'text',
  day:      'number',
  month:    'number',
  year:     'number',
  type:     'text',
  entry:    'text',
}

//These validators assume correct type
export const PERSON_FIELD_VALIDATORS = {
    forename:        () => true,
    surname:         () => true,
    officialnumber:  (x) => x.match(/^[A-Z]?\d+$/),
    birthday:        (x) => x >= 0 && x <= 31,
    birthmonth:      (x) => x >= 0 && x <= 12,
    birthyear:       (x) => x === 0 || (x >= 1700 && x <= 2000), //just try to catch extremely wrong values
    birthplace:      () => true,
    birthcounty:     () => true,
    occupation:      () => true,
    dischargeday:    (x) => x >= 0 && x <= 31,
    dischargemonth:  (x) => x >= 0 && x <= 12,
    dischargeyear:   (x) => x === 0 || (x >= 1700 && x <= 2000), //just try to catch extremely wrong values
    dischargereason: () => true,
};

export const SERVICE_FIELD_VALIDATORS = {
  ship:      () => true,
  rating:    () => true,
  fromday:   (x) => x >= 0 && x <= 31,
  frommonth: (x) => x >= 0 && x <= 12,
  fromyear:  (x) => x === 0 || (x >= 1700 && x <= 2000), //just try to catch extremely wrong values
  today:     (x) => x >= 0 && x <= 31,
  tomonth:   (x) => x >= 0 && x <= 12,
  toyear:    (x) => x === 0 || (x >= 1700 && x <= 2000), //just try to catch extremely wrong values
};

//do some date validation, allowing that we may have missing information
export function get_datevalidator(map) {
  return (fields) => {
    const day = fields[map.day];
    const month = fields[map.month];
    const year = fields[map.year];

    let bad = [];
    if([9, 4, 6, 11].includes(month)) {
      if(day > 30) bad = [map.day, map.month];
    }
    else if(month === 2) {
      if(year === 0 || new Date(year, 1, 29).getDate() === 29) { //unknown or leap year https://stackoverflow.com/a/43819507
        if(day > 29) bad = [map.day, map.month];
      }
      else {
        if(day > 28) bad = [map.day, map.month, map.year]; //only case where the year is relevant
      }
    }
    else { //includes case where month is unknown (0)
      if(day > 31) bad = [map.day]; //the month is irrelevant for this case
    }

    return bad;
  };
}

export function normalize(data, types) {
  for(const field of Object.getOwnPropertyNames(data)) {
    if(data[field] !== null && types[field] === 'text') {
      data[field] = data[field].trim();
      if(data[field] === "") {
        data[field] = null;
      }
    }
    else if(types[field] === 'number') {
      if(data[field] === null) data[field] = 0;
      else if(typeof data[field] !== 'number') data[field] = 0;
    }
  }
  return data;
}

export const RATING_LAYOUT = [
    {labels: {'Forename, surname': 2},      fields: {forename: 3, surname:3}, validator: () => [], },
    {labels: {'Official number': 2},        fields: {officialnumber: 3}, validator: () => [], },
    {labels: {'Born': 2},                   fields: {birthday: 1, birthmonth: 1, birthyear: 1}, validator: get_datevalidator({day: 'birthday', month: 'birthmonth', year: 'birthyear'}), },
    {labels: {'Birth place, county': 2},    fields: {birthplace: 3, birthcounty: 3}, validator: () => [], },
    {labels: {'Occupation': 2},             fields: {occupation: 3}, validator: () => [], },
    {labels: {'Discharge date, reason': 2}, fields: {dischargeday: 1, dischargemonth: 1, dischargeyear: 1, dischargereason: 3}, validator: get_datevalidator({ day: 'dischargeday', month: 'dischargemonth', year: 'dischargeyear'}), },
];

export const OFFICER_LAYOUT = [RATING_LAYOUT[0], RATING_LAYOUT[2], RATING_LAYOUT[3]];

export function init_data(type) {
  if(type !== 'officer') throw new Error('Non-officer values no longer supported');
  return OFFICER_FIELDS.reduce((a, b) => ({...a, [b]: ''}), {});
}

//TODO: Confirm that this contractually produces an empty string at parts[1] when there is no prefix
export function getOfficialNumber(n) {
  const re = new RegExp('^(\\D*)(\\d+)$');
  const parts = n.match(re);
  parts[2] = Number(parts[2]);
  return parts.slice(1);
}

export function getOfficialNumberDigits(n) {
  return getOfficialNumber(n)[1];
}

export function getOfficialNumberPrefix(n) {
  return getOfficialNumber(n)[0];
}

/*
Bit:              5             4            3            2          1
Meaning:    Updated    Reconciled    Complete2    Complete1    Started         STATUS         WORKFLOW
                                                                          0    Not Started    RATINGS
                                                                     1    1    Transcription  RATINGS
                                                          1          1    3    Transcription  RATINGS
                                             1                       1    5    Transcription  RATINGS
                                             1            1          1    7    Checking       RATINGS
                                1            1            1          1   15    Reconciled     RATINGS
                  1             1            1            1          1   31    Maintaining    RATINGS
*/
export function status_label(status_code) {
  switch(status_code) {
    case  0: return 'Not started';
    case  1:
    case  3:
    case  5: return 'Transcription';
    case  7: return 'Checking';
    case 15: return 'Reconciled';
    case 31: return 'Maintaining';
    default: return 'Process Error';
  }
}

export function status_encode(service_record) {
  let bitfield = 0;
  if(service_record.services[0].userid || service_record.services[1].userid) bitfield |= 1;
  if(service_record.services[0].complete) bitfield |= 2;
  if(service_record.services[1].complete) bitfield |= 4;
  if(service_record.reconciled) bitfield |= 8;
  return { status_code: bitfield, description: status_label(bitfield) };
}

export function getSkippedStr(skipped, noun) {
  if(skipped) {
    if(skipped.length === 1)    return `Skipped missing ${noun} ${skipped[0]}`;
    else if(skipped.length > 1) return `Skipped missing ${noun}s ${skipped.slice(0, -1).join(', ')} and ${skipped.at(-1)}`;
  }
  return '';
}

export function isNew(id) {
  return id === 0;
}
