// @vitest-environment jsdom

import { prettyDOM, routeRender, within, mockServer } from './config/testutils';
import userEvent from '@testing-library/user-event';
import { cleanup } from '@testing-library/react';
import Person from '@/person.jsx';
import { describe, test as baseTest, vi } from 'vitest';
import { dump } from './config/testutils';
import { union, intersection, difference, random, range, isEqual, sampleSize } from 'lodash';

//Following tricks https://stackoverflow.com/a/72289488, http://pawelgoscicki.com/archives/2022/05/testing-usenavigate-navigate-from-react-router-v6/, https://mayashavin.com/articles/two-shades-of-mocking-vitest to spy on useParams
import * as router from 'react-router';

const N_MULTITESTS = import.meta.env.VITE_TEST_N_MULTITESTS ? import.meta.env.VITE_TEST_N_MULTITESTS : 10; //number of "multi-field" tests to apply per block that has a multi-field option
const PAGE_ROWS = 100; //expected number of rows per page

//This is encapsualated to prevent tests from accidentally picking up null variables or any state accidentally left hanging
const FIXTURES = (function(){
  let user = null;
  let component = null;
  let birthYear = null;
  let personTable = null;
  let personCommitButton = null;
  let servicesCommitButton = null;
  let xCheck = null;
  let serviceTable0 = null;
  let serviceTable1 = null;
  let notWW1 = null;
  let errorToggle = null;

  //These are functions. It seems that when a function is assigned to a variable as the function is declared,
  //the variable does *not* have to have been declared. This may be a Javascript thing rather than a test
  //framework thing. Either way, I am worryingly uncertain about scope here.
  let postSpy = null;
  let paramSpy = null;
  let getLastPost = null;

  return {
    dataTest: function(personId = 379254, sailorType = 'rating') {
      return {
        paramSpy: async({}, use) => {
          paramSpy = vi.spyOn(router, 'useParams');
          await use(paramSpy);
          paramSpy.mockReset();
          paramSpy = null;
        },
        postSpy: async({}, use) => { //it may be that nothing needs to access postSpy directly
                                     //if so, could consider encapsulating it within getLastPost
                                     //however, I'm worried that I'm already doing too much in
                                     //this fixture (see next comment)

          //The behaviour here is very strange to me. If I declare a listener here which calls
          //postSpy then it seems that the listener will always call the postSpy defined in this
          //enclosing scope. That is, if a listener from an earlier test is not removed then it will
          //run again with not the spy that it was originally declared with, but the current spy.
          //
          //One way around this is to add a bit more indirection, as I have done here. Rather than
          //declare the listener directly with postSpy, I have defined a function to return a listener
          //and passed it the postSpy. At time of writing this seems to work, but I wonder whether this
          //is guaranteed behaviour. I could see how it could be optimized differently so I guess it
          //comes down to what is specified and what is undefined behaviour.
          //
          //There is anyway only an issue if we leave listeners hanging around, which of course is
          //something that we should not do. So, though this feels dangerous, I have opted to keep it
          //like this as it gives me convenient setup and teardown -- it's just important to be careful
          //here.

          postSpy = vi.fn();

          //re https://github.com/mswjs/examples/issues/30 for advice on spying on request
          //   see especially https://github.com/mswjs/examples/issues/30#issuecomment-1222176735
          function getListener(ps) {
            return async({request}) => {
              if(request.method === 'POST') {
                ps({
                  url: request.url,
                  body: await request.clone().json(),
                });
              }
            }
          };
          const listener = getListener(postSpy);
          mockServer.events.on('request:start', listener);
          await use(postSpy);
          mockServer.events.removeListener('request:start', listener);
          postSpy.mockReset();
          postSpy = null;
        },
        getLastPost: async({expect, postSpy}, use) => {
          getLastPost = async(expectedCalls = 1) => {
            //for mock.{calls,lastCall} re https://stackoverflow.com/q/45177287
            await vi.waitFor(()=>expect(postSpy).toHaveBeenCalledTimes(expectedCalls));
            return postSpy.mock.lastCall[0];
          };
          await use(getLastPost);
          getLastPost = null;
        },
        user: async({}, use) => {
          user = userEvent.setup();
          await use(user);
          user = null;
        },
        component: async({user, paramSpy}, use) => {
          component = routeRender(<Person/>, 'person/:sailorType/:nameId/:dataType', `/person/${sailorType}/${personId}/main`);
          await use(component);
          cleanup();
          component = null;
        },
        birthYear: async({component}, use) => {
          //birthYear = await component.findByTestId('birthyear', undefined, { timeout: 10000 }).then((x)=>x.querySelector('input'));
          birthYear = await component.findByTestId('birthyear').then((x)=>x.querySelector('input'));
          await use(birthYear);
          cleanup();
          birthYear = null;
        },
        personTable: async({component}, use) => {
          personTable = await component.findByTestId('personTable');
          await use(personTable);
          cleanup();
          personTable = null;
        },
        personCommitButton: async({component}, use) => {
          personCommitButton = await component.findByTestId('personCommitButton');
          await use(personCommitButton);
          personCommitButton = null;
        },
        servicesCommitButton: async({component}, use) => {
          servicesCommitButton = await component.findByTestId('servicesCommitButton');
          await use(servicesCommitButton);
          servicesCommitButton = null;
        },
        xCheck: async({component}, use) => {
          xCheck = await component.findByTestId('xCheck');
          await use(xCheck);
          xCheck = null;
        },
        serviceTable0: async({component}, use) => {
          serviceTable0 = await component.findByTestId('serviceTable0');
          await use(serviceTable0);
          serviceTable0 = null;
        },
        serviceTable1: async({component}, use) => {
          serviceTable1 = await component.findByTestId('serviceTable1');
          await use(serviceTable1);
          serviceTable1 = null;
        },
        notWW1: async({component}, use) => {
          notWW1 = await component.findByTestId('notww1');
          await use(notWW1);
          notWW1 = null;
        },
        errorToggle: async({component}, use) => {
          errorToggle = await component.findByTestId('error');
          await use(errorToggle);
          errorToggle = null;
        },
      };
    }, // end of dataTest
  };
})();

//App rather than API naming throughout (though they can be the same)
const PERSON_FIELDS = [
  'nameId',
  'forename',
  'surname',
  'officialnumber',
  'birthday',
  'birthmonth',
  'birthyear',
  'birthplace',
  'birthcounty',
  'occupation',
  'dischargeday',
  'dischargemonth',
  'dischargeyear',
  'dischargereason',
  'person_role',
  'notww1',
  'error',
];

const EDITABLE_PERSON_TEXT_FIELDS = [
  'forename',
  'surname',
  'officialnumber', //can contain text!
  'birthplace',
  'birthcounty',
  'occupation',
  'dischargereason',
];

const EDITABLE_PERSON_NUMERIC_FIELDS = [
  'birthday',
  'birthmonth',
  'birthyear',
  'dischargeday',
  'dischargemonth',
  'dischargeyear',
];

const EDITABLE_PERSON_TOGGLES = [
  'notww1',
];

const EDITABLE_PERSON_FIELDS = [
  ...EDITABLE_PERSON_TEXT_FIELDS,
  ...EDITABLE_PERSON_NUMERIC_FIELDS,
  ...EDITABLE_PERSON_TOGGLES,
];

const EMPTY_FIRST_ROW = {
  rowid: 1,
  ship: null,
  rating: null,
  fromday: 0,
  frommonth: 0,
  fromyear: 0,
  today: 0,
  tomonth: 0,
  toyear: 0,
};

{ //make sure I've set up my data right
  const comparison = ['nameId', 'person_role', 'error', ...EDITABLE_PERSON_TEXT_FIELDS, ...EDITABLE_PERSON_NUMERIC_FIELDS, ...EDITABLE_PERSON_TOGGLES];
  const d1 = difference(PERSON_FIELDS, comparison);
  const d2 = difference(comparison, PERSON_FIELDS);
  if(d1.length) {
    console.error(`The following field(s) in PERSON_FIELDS are missing from partial(s):\n${d1}`);
  }
  if(d1.length && d2.length) console.error();
  if(d2.length) {
    console.error(`The following field(s) in partial(s) are missing from PERSON_FIELDS:\n${d2}`);
  }
  if(d1.length || d2.length) console.error();
  const u = union(
    intersection(EDITABLE_PERSON_TEXT_FIELDS, EDITABLE_PERSON_NUMERIC_FIELDS),
    intersection(EDITABLE_PERSON_TEXT_FIELDS, EDITABLE_PERSON_TOGGLES),
    intersection(EDITABLE_PERSON_NUMERIC_FIELDS, EDITABLE_PERSON_TOGGLES),
  );
  if(u.length) {
    console.error(`The following field(s) appear in at least two of the partials:\n${u}`);
  }
  if(d1.length || d2.length || u.length) throw new Error('Mistake in data setup');
};

const SERVICE_FIELDS = [
  'rowid',
  'ship',
  'rating',
  'fromday',
  'frommonth',
  'fromyear',
  'today',
  'tomonth',
  'toyear',
];

const EDITABLE_SERVICE_TEXT_FIELDS = [
  'ship',
  'rating',
];

const EDITABLE_SERVICE_NUMERIC_FIELDS = [
  'fromday',
  'frommonth',
  'fromyear',
  'today',
  'tomonth',
  'toyear',
];

const SERVICE_NUMERIC_FIELDS = [
  'rowid',
  ...EDITABLE_SERVICE_NUMERIC_FIELDS,
];

const EDITABLE_SERVICE_FIELDS = [
 ...EDITABLE_SERVICE_TEXT_FIELDS,
 ...EDITABLE_SERVICE_NUMERIC_FIELDS,
];

function nTableRows(table) {
  const x_to_y_of_z = Object.values(within(table).getByClass(/^MuiTablePagination-displayedRows/)).at(-1).children;
  const nRows = Number(x_to_y_of_z.match(/\d+$/)[0]);
  return nRows;
}

function readTablePage(table, api = true) { //if api is true, convert data into api format
  const page = [];
  for(let i = 0; i < nTableRows(table); i++) {
    const row = {};
    for(const [key, value] of Object.entries(getServiceCells(getRow(table, i)))) {
      let readValue = value.title;
      if(readValue === '') readValue = null;
      else if(SERVICE_NUMERIC_FIELDS.includes(key)) readValue = Number(readValue);
      if(api && key === 'rowid') { //this is the only special case for api format
        row.row_number = readValue;
      }
      else {
        row[key] = readValue;
      }
    }
    page.push(row);
  }
  return page;
}

function dumpTablePage(table) {
  console.log(readTablePage(table));
}

function dumpTablesPage(table0, table1) {
  console.log('Table 0');
  dumpTablePage(table0);
  console.log();
  console.log('Table 1');
  dumpTablePage(table1);
}

function getServiceCells(row) {
  const cells = {};
  for(const field of SERVICE_FIELDS) {
    cells[field] = within(row).getByField(field);
  }
  return cells;
}

function getServiceData(cells) {
  const data = {};
  for(const [field, cell] of Object.entries(cells)) {
    data[field] = cell.getAttribute('title');
    if(SERVICE_NUMERIC_FIELDS.includes(field)) {
      data[field] = Number(data[field]);
    }
  }
  return data;
}

function getPageServiceData(table, page = 0 /* must pass in the current page (counting from 0) for row indices to match rowIndex in loop */) {
  const data = [];
  for(const rowIndex of range(page * PAGE_ROWS, Math.min(nTableRows(table), (page + 1) * PAGE_ROWS))) {
    data.push(getServiceData(getServiceCells(getRow(table, rowIndex))));
  }
  return data;
}

function getRow(table, index) {
  if(index < 0) {
    const rowIndex = nTableRows(table) + index;
    if(rowIndex < 0) throw new Error();
    return within(table).getByRowIndex(rowIndex);
  }
  else {
    return within(table).getByRowIndex(index);
  }
}

function getCheckboxes(tables) {
  const checkboxes = [];
  for(const table of tables) {
    checkboxes.push(within(table).getByTestId('completeCheckbox'));
  };
  return checkboxes;
}

function getDV(component) {
  return component.getAttribute('data-value');
}

async function cloneFrom(user, table) {
  await user.click(within(table).getByTestId(/^clone\dto\dButton$/));
}

function randomCellIdentifier() {
  return ['ship', 'rating', 'fromday', 'today', 'frommonth', 'tomonth', 'fromyear', 'toyear'][random(0, 7)];
}

function randomCellContent(field) {
  if(field === 'ship'      || field === 'rating')  return randomString();
  if(field === 'fromday'   || field === 'today')   return random(1, 28); //0 is a legit value, but also the default -- I usually want to differ from the default
  if(field === 'frommonth' || field === 'tomonth') return random(1, 12); //0 is a legit value, but also the default -- I usually want to differ from the default
  if(field === 'fromyear'  || field === 'toyear')  return Number('18' + random(10,99));
  throw new Error(`Unknown field ${field}`);
}

async function addFirstRow(user, table) {
  await user.click(within(await table).getByTestId('firstRowButton'));
  return getRow(table, 0);
}

async function addFirstRowBoth(user, table0, table1) {
  return [
    await addFirstRow(user, table0),
    await addFirstRow(user, table1),
  ];
}

async function addTopRow(user, table) {
  await user.click(within(getRow(table, 0)).getByTestId('newRowAboveButton'));
  return getRow(table, 0);
}

async function addRow(user, table, baseIndex = -1) { //add row below row with given index (negative numbers count from end)
  await user.click(within(getRow(table, baseIndex)).getByTestId('newRowBelowButton'));
  if(baseIndex < 0) return getRow(table, baseIndex);
  else              return getRow(table, baseIndex + 1);
}

async function partialPopulateRow(user, row, content = { [randomCellIdentifier()]: null }) {
  const fields = getServiceCells(row);
  for(const field in content) {
    const c = content[field] === null ? inputEscaper(randomCellContent(field)) : content[field];
    await user.type(fields[field], `${c}{Enter}`);
  }
}

async function addNServiceRows(user, table, nExtraRows = random(4, 7)) {
  const nRows = nTableRows(table);
  for(const row of range(nRows - 1, nRows - 1 + nExtraRows)) {
    await user.click(within(getRow(table, row)).getByTestId('newRowBelowButton'));
  }
  return nExtraRows;
}

//TODO: This is a very inefficient way to build up data.
//      In some cases I really want to test the user interface.
//      But where this is being used to set up test preconditions -- especially over multiple rows -- would be much more efficient to just create the data structure that the React component reads
async function populateRow(user, row, content = {}) {
  const fields = getServiceCells(row);
  await user.type(fields.ship,      `${content.ship      || inputEscaper(randomCellContent('ship'))     }{Enter}`);
  await user.type(fields.rating,    `${content.rating    || inputEscaper(randomCellContent('rating'))   }{Enter}`);
  await user.type(fields.fromday,   `${content.fromday   || inputEscaper(randomCellContent('fromday'))  }{Enter}`);
  await user.type(fields.frommonth, `${content.frommonth || inputEscaper(randomCellContent('frommonth'))}{Enter}`);
  await user.type(fields.fromyear,  `${content.fromyear  || inputEscaper(randomCellContent('fromyear')) }{Enter}`);
  await user.type(fields.today,     `${content.today     || inputEscaper(randomCellContent('today'))    }{Enter}`);
  await user.type(fields.tomonth,   `${content.tomonth   || inputEscaper(randomCellContent('tomonth'))  }{Enter}`);
  await user.type(fields.toyear,    `${content.toyear    || inputEscaper(randomCellContent('toyear'))   }{Enter}`);
}

async function populateRows(user, table, prefix = '', indexField = 'ship') {
  for(const row of range(0, nTableRows(table))) {
    const content = indexField ? { [indexField]: `${prefix}${row}` } : {};
    await populateRow(user, getRow(table, row), content);
  }
}

async function deleteRow(user, table, index) {
  await user.click(within(getRow(table, index)).getByTestId('deleteRowButton'));
}

function expectUnpressable(expect, user, button) {
  return expect(user.click(button)).rejects.toThrow('Unable to perform pointer interaction as the element has `pointer-events: none`');
}

function findPersonTableField(fieldId, personTable) {
  return within(personTable).findByTestId(fieldId).then((x)=>x.querySelector('input'));
}

function randomString() {
  let result = '';
  for(let i = random(4, 12); i >= 0; i--) {
    result += String.fromCharCode(random(32, 126));
  }
  return result;
}

//for use when we do not know exactly what we are getting, due to randomness above
//'acquire' because terms like 'get' and 'find' have meaning in this testing framework
//returns boolean values as booleans for convenience in tests that need to say "flip state"
async function acquireFieldComponent(field, type, personTable, notWW1) {
  if(type === 'boolean') { //the boolean components are not in the personTable
    const fieldComponent = function() {
      if(field === 'notww1')     return notWW1;
      else                       throw new Error(`Unknown boolean field ${field}`);
    }();
    const value = function() {
      const v = fieldComponent.getAttribute('data-value');
      if(v === 'true')       return true;
      else if(v === 'false') return false;
      else                   throw new Error(`Boolean field ${field} has non-boolean value ${v}`);
    }();
    return [ fieldComponent, value ];
  }
  else {
    const fieldComponent = await findPersonTableField(field, personTable);
    return [fieldComponent, fieldComponent.getAttribute('value')];
  }
}

//escape characters that are special for user.type input
function inputEscaper(input) {
  if(typeof(input) === 'string') {
    //just escape the opens
    //closes must only be escaped if there is a real open, and there will never be a real open, because we escape the here
    return input.replaceAll(/[\{\[]/g, '$&$&');
  }
  else { //e.g. a number
    return input;
  }
}

function randomCases() {
  const testcases = [];
  for(let i = 0; i < N_MULTITESTS; i++) { //just loop this many times
    const TOTAL_FIELDS = EDITABLE_PERSON_FIELDS.length;

    //we test minimum 2 non-toggle fields, if that's more than are in the array then we will have a problem
    //toggle fields are kind of special cases, so we always want to test at least a couple of the others
    if(EDITABLE_PERSON_TOGGLES.length + 2 >= TOTAL_FIELDS) { throw new Error(); }

    //randomly select n_fields fields to include in the test case
    //all fields must be different
    //no two test cases are allowed to contain the exact same fields
    let retry_count = 0;
    let testcase = null;
    do {
      const n_fields = random(2, TOTAL_FIELDS, false); // number of fields to include in this testcase
                                                                             // 2 <= nfields <= number of editable fields (also an integer)
      const fields = range(0, TOTAL_FIELDS); //all the possible field indices
      const candidate = []; //candidate test case
      while(fields.length > TOTAL_FIELDS - n_fields) { //sampling without replacement
        const fieldIndex = random(0, fields.length - 1);
        candidate.push(fields[fieldIndex]);
        fields.splice(fieldIndex, 1);
      }
      testcase = candidate;
    } while(function(){ //keep going until we have a testcase checking a unique set of fields
      const testcaseSorted = testcase.toSorted();
      for(const existingCase of testcases) {
        if(isEqual(testcaseSorted, existingCase.toSorted())) {
          retry_count++;
          if(retry_count < 10) {
            return true;
          }
          else { //avoid massive slowness/infinite loop
            throw new Error('Retry count exceeded while generated unique random tests');
          }
        }
      }
      retry_count = 0;
      return false;
    }());
    testcases.push(testcase);
  }
  const expandedTestcases = [];
  for(const testcase of testcases) {
    const expandedTestcase = [];
    for(const field of testcase) {
      const name = EDITABLE_PERSON_FIELDS[field];
      const type = function() {
        if     (EDITABLE_PERSON_TEXT_FIELDS.includes(name))    return 'text';
        else if(EDITABLE_PERSON_NUMERIC_FIELDS.includes(name)) return 'number';
        else if(EDITABLE_PERSON_TOGGLES.includes(name))        return 'boolean';
        else throw new Error(`Field ${name} not listed in any partial, cannot determine type.`);
      }();
      const data = function() {
        if(type === 'text')         return randomString();
        else if(type === 'number')  return random(0, 12);
        else if(type === 'boolean') return null;
      }();
      expandedTestcase.push({
        field: name,
        type: type,
        input:  type === 'boolean' ? null : inputEscaper(data),
        output: type === 'boolean' ? null : data,
      });
    }
    expandedTestcases.push(expandedTestcase);
  }
  return expandedTestcases;
}

function testcaseString(testcase) {
  return testcase.map((x)=>`${x.field} (${x.input})`).join(', ');
}

function fieldExpectation(type, data) {
  if(type === 'text') return ('' + data).trim();
  else if(type === 'number') {
    if(!data.match(/^\d+$/)) throw new Error(`Numeric data ${data} not a positive integer`);
    return Number(data);
  }
  else if(type === 'boolean') throw new Error('fieldExpectation cannot tell you your prior state');
  else throw new Error(`Unknown type ${type}`);
}

const it = baseTest.extend(FIXTURES.dataTest());
const fullPersonTest        = baseTest.extend(FIXTURES.dataTest(9999999901));
const emptyPersonTest       = baseTest.extend(FIXTURES.dataTest(9999999902));
const emptyServiceTest      = baseTest.extend(FIXTURES.dataTest(9999999905)); //services present but no rows and in incomplete state
const completeServiceTest   = baseTest.extend(FIXTURES.dataTest(9999999903)); //still no rows, but both tables are flagged complete
const reconciledServiceTest = baseTest.extend(FIXTURES.dataTest(9999999904)); //services reconciled into a single table (but no rows)
const blankServiceTest      = baseTest.extend(FIXTURES.dataTest(9999999906)); //one empty row (both tables)
const pageServiceTest       = baseTest.extend(FIXTURES.dataTest(9999999907)); //full page, with basic data (both tables)
const twoPageServiceTest    = baseTest.extend(FIXTURES.dataTest(9999999908)); //two full pages, with basic data (both tables)
//TODO: Add tests for pressing both types of button (services and person)
describe('person', () => {
  describe('commit url', () => {
    fullPersonTest('person', async({expect, user, getLastPost, birthYear, personCommitButton}) => {
      await user.clear(birthYear); //just to enable the button
      await user.click(personCommitButton);
      const {url} = await getLastPost();
      expect(url).toMatch(/\/person$/);
    });
    fullPersonTest('personThrice', async({expect, user, postSpy, birthYear, personCommitButton}) => {
      await user.clear(birthYear); //just to enable the button
      await Promise.all([
        user.click(personCommitButton),
        user.click(personCommitButton),
        user.click(personCommitButton),
      ]);

      //ideally only the first click would work
      //failing that, ideally all clicks would be rolled up into a single POST
      //as neither applies (potentially for "how testing works" resons), just confirm that all POSTs are the same
      expect(postSpy).toHaveBeenCalled();

      //also check that the button is *now* disabled
      await expectUnpressable(expect, user, personCommitButton);

      //I don't know the details of how this structure works, so tell me if assumptions break
      expect(postSpy.mock.calls.length).toBe(3); //outer list has 1 element per call (and is a list of lists but don't explicitly check that)
      expect(postSpy.mock.calls[0].length).toBe(1); //each inner list is a single element

      for(let i = 1; i < postSpy.mock.calls.length; i++) {
        expect(postSpy.mock.calls[i].length).toBe(1); //another assumption check -- each inner list is a single element
        expect(postSpy.mock.calls[0][0]).toEqual(postSpy.mock.calls[i][0]); //the actual (deep) equality test
      }
    });
    fullPersonTest('personTriple', async({expect, user, postSpy, birthYear, personCommitButton}) => { //same test as above, but using triple-click instead of 3 single clicks. This seems to work more as expected.
      await user.clear(birthYear); //just to enable the button
      await user.tripleClick(personCommitButton); //after the first click, subsequents should fail -- but in any case, result should be a single post

      //ideally only the first click would work
      //failing that, ideally all clicks would be rolled up into a single POST
      //as neither applies (potentially for "how testing works" resons), just confirm that all POSTs are the same
      expect(postSpy).toHaveBeenCalledOnce();

      //also check that the button is now disabled
      await expectUnpressable(expect, user, personCommitButton);
    });
  });

  describe('trim', () => {
    describe('text', () => {
      describe('individual', () => {
        describe('post-trim', () => {
          for(const field of EDITABLE_PERSON_TEXT_FIELDS) {
            fullPersonTest(field, async ({expect, user, personTable, personCommitButton}) => { //Confirm that all input person data gets trimmed. Doesn't really need to be the 'full' data, just need to start somewhere. But using real data lets me confirm that the trimmed text results in something un-submittable.
              const fieldComponent = await findPersonTableField(field, personTable);
              const baseText = fieldComponent.getAttribute('value');

              await user.type(fieldComponent, '  {Enter}');
              expect(fieldComponent.getAttribute('value')).toBe(baseText + '  ');
              await expectUnpressable(expect, user, personCommitButton); //button should be unavailable as nothing has meaningfully changed
              await user.type(fieldComponent, 'A  {Enter}'); //text has now changed in a non-trimmable way, but still has something to trim
              await user.click(personCommitButton);
              expect(fieldComponent.getAttribute('value')).toBe(baseText + '  A');
            });
          }
        });
        describe('pre-trim', () => {
          for(const field of EDITABLE_PERSON_TEXT_FIELDS) {
            fullPersonTest('pre-trim full text ' + field, async ({expect, user, personTable, personCommitButton}) => { //Confirm that all input person data gets trimmed. Doesn't really need to be the 'full' data, just need to start somewhere. But using real data lets me confirm that the trimmed text results in something un-submittable.
              const fieldComponent = await findPersonTableField(field, personTable);
              const baseText = fieldComponent.getAttribute('value');

              await user.clear(fieldComponent);
              await user.type(fieldComponent, '  ' + baseText + '{Enter}');
              expect(fieldComponent.getAttribute('value')).toBe('  ' + baseText);
              await expectUnpressable(expect, user, personCommitButton);
              await user.type(fieldComponent, 'B{Enter}');
              await user.click(personCommitButton);
              expect(fieldComponent.getAttribute('value')).toBe(baseText + 'B');
            });
          }
        });
        describe('empty trim', () => {
          for(const field of EDITABLE_PERSON_TEXT_FIELDS) {
            emptyPersonTest('trim empty text ' + field, async ({expect, user, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              const baseText = fieldComponent.getAttribute('value');
              expect(baseText).toBe('');

              await user.type(fieldComponent, '    {Enter}');
              expect(fieldComponent.getAttribute('value')).toBe('    ');
              await expectUnpressable(expect, user, personCommitButton);
              await user.type(fieldComponent, 'C    {Enter}');
              await user.click(personCommitButton);
              expect(fieldComponent.getAttribute('value')).toBe('C');
            });
          }
        });
      });
    });
    describe('numeric', () => {
      describe('individual', () => {
        describe('post-trim', () => {
          for(const field of EDITABLE_PERSON_NUMERIC_FIELDS) {
            fullPersonTest(field, async ({expect, user, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              const base = fieldComponent.getAttribute('value');
              expect(base).toMatch(/^\d+$/);

              await user.clear(fieldComponent);
              await user.type(fieldComponent, base + ' {Enter}');
              expect(fieldComponent.getAttribute('value')).toBe(base);
              await expectUnpressable(expect, user, personCommitButton);
            });
          }
        });
        describe('pre-trim', () => {
          for(const field of EDITABLE_PERSON_NUMERIC_FIELDS) {
            fullPersonTest('pre-trim numeric ' + field, async ({expect, user, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              const base = fieldComponent.getAttribute('value');
              expect(base).toMatch(/^\d+$/);

              await user.clear(fieldComponent);
              await user.type(fieldComponent, ' ' + base + ' {Enter}');
              expect(fieldComponent.getAttribute('value')).toBe(base);
              await expectUnpressable(expect, user, personCommitButton);
            });
          }
        });
        describe('empty trim', () => {
          for(const field of EDITABLE_PERSON_NUMERIC_FIELDS) {
            emptyPersonTest('trim empty numeric ' + field, async ({expect, user, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              const base = fieldComponent.getAttribute('value');
              expect(base).toBe('0');

              await user.clear(fieldComponent);
              await user.type(fieldComponent, '     {Enter}');
              expect(fieldComponent.getAttribute('value')).toBe('0');
              await expectUnpressable(expect, user, personCommitButton);
            });
          }
        });
      });
    });
  });

  //Tests that actually enter data into each field and confirm that it gets posted correctly.
  //Much like the 'clear' tests below, except involve typing rather than just clearing.
  //The ones that start from empty text ("insert") will serve as new data entry.
  //The ones that that start with data ("edit") will serve as editing existing data.
  //Test every field individually. Also randomly test combinations of fields (exhaustive testing is possible but too much buck for bang).
  describe('edit', () => {
    describe('update', () => {
      describe('random', () => {
        for(const testcase of randomCases()) {
          fullPersonTest(testcaseString(testcase), async ({expect, user, getLastPost, personTable, notWW1, personCommitButton}) => {
            const expectedFields = {};
            for(const { field, type, input, output } of testcase) {
              const [ fieldComponent, base ] = await acquireFieldComponent(field, type, personTable, notWW1);

              if(type === 'boolean') {
                await user.click(fieldComponent);
                expectedFields[field] = !base;
              }
              else {
                expect(base).not.toBe(type === 'text' ? '' : '0'); //precondition
                await user.type(fieldComponent, `${input}{Enter}`);
                expectedFields[field] = fieldExpectation(type, base + output);
              }
            }
            await user.click(personCommitButton);
            const { body } = await getLastPost();
            expect(body.person).toMatchObject(expectedFields);
          });
        }
      });
      describe('individual', () => {
        describe('text', () => {
          for(const field of EDITABLE_PERSON_TEXT_FIELDS) {
            fullPersonTest(field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              const base = fieldComponent.getAttribute('value');
              expect(base).not.toBe(''); //precondition
              await user.type(fieldComponent, 'newthing{Enter}');
              await user.click(personCommitButton);
              const { body } = await getLastPost();
              expect(body.person[field]).toBe(base + 'newthing');
            });
          }
        });
        describe('numeric', () => {
          for(const field of EDITABLE_PERSON_NUMERIC_FIELDS) {
            fullPersonTest(field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              const base = fieldComponent.getAttribute('value');

              //precondition
              expect(base).not.toBe('');
              expect(base).not.toBe('0');
              expect(base).not.toBe(0);

              await user.type(fieldComponent, '123{Tab}'); //tabbing changes the selection
                                                           //for numeric fields, this is needed to cause the attribute 'value' to update
                                                           //an alternative approach is to check fieldComponent.value rather than
                                                           //fieldComponent.getAttribute('value')
                                                           //because I am trying to look at what ends up getting transmitted, I think
                                                           //the attribute ("actual" value for at least a controlled component) is what
                                                           //I want to look at, but this may be splitting hairs
                                                           //re https://github.com/testing-library/user-event/issues/411 and
                                                           //   https://stackoverflow.com/a/6004028
              expect(fieldComponent.getAttribute('value')).toBe(`${base}123`);
              await user.click(personCommitButton);
              const { body } = await getLastPost();
              expect(body.person[field]).toBe(Number(`${base}123`));
            });
          }
        });
      });
    });
    describe('insert', () => {
      describe('random', () => {
        for(const testcase of randomCases()) {
          emptyPersonTest(testcaseString(testcase), async({expect, user, getLastPost, personTable, notWW1, personCommitButton}) => {
            const expectedFields = {};
            for(const { field, type, input, output } of testcase) {
              const [ fieldComponent, base ] = await acquireFieldComponent(field, type, personTable, notWW1);
              if(type === 'boolean') {
                await user.click(fieldComponent);
                expectedFields[field] = !base;
              }
              else {
                expect(base).toBe(type === 'text' ? '' : '0'); //precondition
                await user.type(fieldComponent, `${input}{Enter}`);
                expectedFields[field] = fieldExpectation(type, '' + output); //fieldExpectation needs its input to be a string
              }
            }
            await user.click(personCommitButton);
            const { body } = await getLastPost();
            expect(body.person).toMatchObject(expectedFields);
          });
        }
      });
      describe('individual', () => {
        describe('text', () => {
          for(const field of EDITABLE_PERSON_TEXT_FIELDS) {
            emptyPersonTest(field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              const base = fieldComponent.getAttribute('value');
              expect(base).toBe(''); //precondition
              await user.type(fieldComponent, 'newthing{Enter}');
              await user.click(personCommitButton);
              const { body } = await getLastPost();
              expect(body.person[field]).toBe('newthing');
            });
          }
        });
        describe('numeric', () => {
          for(const field of EDITABLE_PERSON_NUMERIC_FIELDS) {
            emptyPersonTest(field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              const base = fieldComponent.getAttribute('value');

              //precondition
              expect(base).toBe('0');

              await user.type(fieldComponent, '123{Tab}'); //tabbing changes the selection
                                                           //for numeric fields, this is needed to cause the attribute 'value' to update
                                                           //an alternative approach is to check fieldComponent.value rather than
                                                           //fieldComponent.getAttribute('value')
                                                           //because I am trying to look at what ends up getting transmitted, I think
                                                           //the attribute ("actual" value for at least a controlled component) is what
                                                           //I want to look at, but this may be splitting hairs
                                                           //re https://github.com/testing-library/user-event/issues/411 and
                                                           //   https://stackoverflow.com/a/6004028
              expect(fieldComponent.getAttribute('value')).toBe('123'); //0-prefix should disappear
              await user.click(personCommitButton);
              const { body } = await getLastPost();
              expect(body.person[field]).toBe(123);
            });
          }
        });
      });
    });
  });
  describe('clear', () => {
    describe('update', () => {
      describe('random', () => {
        for(const testcase of randomCases()) {
          fullPersonTest(testcaseString(testcase), async ({expect, user, getLastPost, personTable, notWW1, personCommitButton}) => {
            const expectedFields = {};
            for(const { field, type } of testcase) {
              const [ fieldComponent, base ] = await acquireFieldComponent(field, type, personTable, notWW1);

              if(type === 'boolean') { //not entirely clear what "clear" means for a boolean, but let's say "ensure false"
                if(base) await user.click(fieldComponent);
                expectedFields[field] = false;
              }
              else {
                expect(fieldComponent.getAttribute('value')).not.toBe(type === 'text' ? '' : '0'); //precondition
                await user.clear(fieldComponent);
                await user.type(fieldComponent, '{Enter}');
                expectedFields[field] = type === 'text' ? '' : 0;
              }
            }
            await user.click(personCommitButton);
            const { body } = await getLastPost();
            expect(body.person).toMatchObject(expectedFields);
          });
        }
      });
      describe('individual', () => {
        describe('text', () => {
          for(const field of EDITABLE_PERSON_TEXT_FIELDS) {
            fullPersonTest(field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              expect(fieldComponent.getAttribute('value')).not.toBe(''); //precondition -- we are clearing the field, so to be able to submit it to test API POST, it must not be empty
              await user.clear(fieldComponent);
              await user.type(fieldComponent, '{Enter}'); //seem to need this to pick up the change
              await user.click(personCommitButton);

              const { body } = await getLastPost();
              expect(body.person[field]).toEqual('');
            });
          }
        });
        describe('numeric', () => {
          for(const field of EDITABLE_PERSON_NUMERIC_FIELDS) {
            fullPersonTest(field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);

              //precondition -- we are clearing the field, so to be able to submit it to test API POST, it must not be empty
              expect(fieldComponent.getAttribute('value')).not.toBe('');
              expect(fieldComponent.getAttribute('value')).not.toBe('0');
              expect(fieldComponent.getAttribute('value')).not.toBe(0);

              await user.clear(fieldComponent);
              await user.type(fieldComponent, '{Enter}'); //seem to need this to pick up the change
              await user.click(personCommitButton);

              const { body } = await getLastPost();
              expect(body.person[field]).toBe(0);
            });
          }
        });
      });
    });
    describe('insert', () => {
      describe('random', () => {
        for(const testcase of randomCases()) {
          emptyPersonTest(testcaseString(testcase), async ({expect, user, postSpy, personTable, notWW1, personCommitButton}) => {
            const expectedFields = {};
            for(const {field, type} of testcase) {
              const [ fieldComponent, base ] = await acquireFieldComponent(field, type, personTable, notWW1);

              if(type === 'boolean') { //not clear what 'clear' means for a boolean, but in the context of this test we just have to maintain the state
                expectedFields[field] = base;
              }
              else {
                expect(fieldComponent.getAttribute('value')).toBe(type === 'text' ? '' : '0');  //precondition -- we are testing that we cannot post if we change the record to be the same as it was before
                await user.clear(fieldComponent);
                await user.type(fieldComponent, '{Enter}');
                expectedFields[field] = type === 'text' ? '' : 0;
              }
            }
            await expectUnpressable(expect, user, personCommitButton);
            await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button
          });
        }
      });
      describe('individual', () => {
        describe('text', () => {
          for(const field of EDITABLE_PERSON_TEXT_FIELDS) {
            emptyPersonTest(field, async ({expect, user, postSpy, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);
              expect(fieldComponent.getAttribute('value')).toBe(''); //precondition -- we are testing that we cannot post if we change the record to be the same as it was before
              await user.clear(fieldComponent);
              await user.type(fieldComponent, '{Enter}');
              await expectUnpressable(expect, user, personCommitButton);

              await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button
            });
          }
        });
        describe('numeric', () => {
          for(const field of EDITABLE_PERSON_NUMERIC_FIELDS) {
            emptyPersonTest(field, async ({expect, user, postSpy, personTable, personCommitButton}) => {
              const fieldComponent = await findPersonTableField(field, personTable);

              //precondition -- we are clearing the field, so to be able to submit it to test API POST, it must not be empty
              expect(fieldComponent.getAttribute('value')).toBe('0'); //seems that whatever we read from this field will be stringified, so check the type as well. That we actually end up posting a numeric 0 in such cases should be tested elsewhere
              expect(fieldComponent.getAttribute('type')).toBe('number');

              await user.clear(fieldComponent);
              await user.type(fieldComponent, '{Enter}');
              await expectUnpressable(expect, user, personCommitButton);
              await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button

              //test with the explicit 0 too
              await user.clear(fieldComponent);
              await user.type(fieldComponent, '0{Enter}');
              await expectUnpressable(expect, user, personCommitButton);
              await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button
            });
          }
        });
      });
    });
  });
  describe('toggles', () => {
    fullPersonTest('not ww1 true to false', async ({expect, user, getLastPost, notWW1, personCommitButton}) => { //baselines to true
      //expected pre-condition
      expect(notWW1.getAttribute('data-value')).toBe('true'); //stringified in the DOM
      await expectUnpressable(expect, user, personCommitButton);

      await user.click(notWW1);
      await user.click(personCommitButton);
      const { body } = await getLastPost();
      expect(body.person.notww1).toBe(false);
    });
    emptyPersonTest('not ww1 false to true', async ({expect, user, getLastPost, notWW1, personCommitButton}) => { //baselines to false
      //expected pre-condition
      expect(notWW1.getAttribute('data-value')).toBe('false'); //stringified in the DOM
      await expectUnpressable(expect, user, personCommitButton);

      await user.click(notWW1);
      await user.click(personCommitButton);
      const { body } = await getLastPost();
      expect(body.person.notww1).toBe(true);
    });
    fullPersonTest('error true to false', async ({expect, user, postSpy, errorToggle, personCommitButton}) => { //baselines to true. Reporting only, cannot be changed.
      //expected pre-condition
      expect(errorToggle.getAttribute('data-value')).toBe('true'); //stringified in the DOM
      await expectUnpressable(expect, user, personCommitButton);

      await expectUnpressable(expect, user, errorToggle);
      await expectUnpressable(expect, user, personCommitButton);
      await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button
    });
    emptyPersonTest('error false to true', async ({expect, user, postSpy, errorToggle, personCommitButton}) => { //baselines to false. Reporting only, cannot be changed.
      //expected pre-condition
      expect(errorToggle.getAttribute('data-value')).toBe('false'); //stringified in the DOM
      await expectUnpressable(expect, user, personCommitButton);

      await expectUnpressable(expect, user, errorToggle);
      await expectUnpressable(expect, user, personCommitButton);
      await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button
    });
  });
  describe.todo('catref', ()=>{}); //check the the catref matches the series and actual item number. manufacture cases where other ususal suspects (officialnumber, nameid) are different from the item number. could be that my existing test data already happens to have these properties.
});

describe('services', () => {
  describe('commit url', () => {
    //TODO: Multi-click tests for services.
    it('services', async({expect, user, getLastPost, serviceTable0, serviceTable1, component}) => {
      //this part more checking the preconditions that part of the test
      //i.e. do I need to press the 'complete' checkboxes in order to submit?
      const cb = getCheckboxes([serviceTable0, serviceTable1]);
      expect(getDV(cb[0])).toBe('true');
      expect(getDV(cb[1])).toBe('true');

      //add a row to each table so that the button will be enabled
      //NB Cannot use addFirstRowBoth, this would generate the popup that warns about empty rows
      await partialPopulateRow(user, await addFirstRow(user, serviceTable0));
      await cloneFrom(user, serviceTable0);

      await user.click(component.getByTestId('servicesCommitButton'));
      const {url} = await getLastPost();
      expect(url).toMatch(/\/person$/);
    });
  });
  describe('states', () => {
    describe('complete', () => { //these tests check a couple of things -- that the xcheckbutton is unpressable unless both tables are marked complete, and that the submit button is unpressable unless both tables are marked complete
      emptyServiceTest('neither', async ({expect, user, xCheck, serviceTable0, serviceTable1, servicesCommitButton}) => {
        const cb = getCheckboxes([serviceTable0, serviceTable1]);

        //NB Cannot use addFirstRowBoth, this would generate the popup that warns about empty rows
        await partialPopulateRow(user, await addFirstRow(user, serviceTable0));
        await cloneFrom(user, serviceTable0);

        //preconditions
        expect(getDV(cb[0])).toBe('false');
        expect(getDV(cb[1])).toBe('false');

        await expectUnpressable(expect, user, xCheck);
        await expectUnpressable(expect, user, servicesCommitButton);
      });
      emptyServiceTest('first', async ({expect, user, xCheck, serviceTable0, serviceTable1, servicesCommitButton}) => {
        const cb = getCheckboxes([serviceTable0, serviceTable1]);

        //preconditions
        await user.click(cb[0]);
        expect(getDV(cb[0])).toBe('true'); //this is already a state change
        expect(getDV(cb[1])).toBe('false');

        await expectUnpressable(expect, user, xCheck);
        await expectUnpressable(expect, user, servicesCommitButton);
      });
      emptyServiceTest('second', async ({expect, user, xCheck, serviceTable0, serviceTable1, servicesCommitButton}) => {
        const cb = getCheckboxes([serviceTable0, serviceTable1]);

        //preconditions
        await user.click(cb[1]);
        expect(getDV(cb[0])).toBe('false');
        expect(getDV(cb[1])).toBe('true'); //this is already a state change

        await expectUnpressable(expect, user, xCheck);
        await expectUnpressable(expect, user, servicesCommitButton);
      });
      emptyServiceTest('both', async ({expect, user, getLastPost, xCheck, serviceTable0, serviceTable1, servicesCommitButton}) => {
        const cb = getCheckboxes([serviceTable0, serviceTable1]);

        //preconditions
        await Promise.all([ //these are already state changes
          user.click(cb[0]),
          user.click(cb[1]),
        ]);
        expect(getDV(cb[0])).toBe('true');
        expect(getDV(cb[1])).toBe('true');

        user.click(xCheck);
        user.click(servicesCommitButton);
        await getLastPost(); //includes check that there has been a single POST
      });
    });
  });
  describe('xcheck', () => { //we already know from above tests that xcheck can only be set if both complete flags are set
                             //this is actually a UI test, but it is a constraint on data flow, so not completely nuts to have it here
                             //TODO: Setting this _should_ result in sending a single unified table back?
    completeServiceTest('xcheck locks complete', async ({expect, user, xCheck, serviceTable0, serviceTable1})=> {
      //establish preconditions
      await user.click(xCheck);
      expect(getDV(xCheck)).toBe('true');

      const cb = getCheckboxes([serviceTable0, serviceTable1]);
      await expectUnpressable(expect, user, cb[0]);
      await expectUnpressable(expect, user, cb[1]);
    });
  });
  describe('clone', () => { //check the left-to-right and right-to-left buttons
    describe('disabled', () => {
      emptyServiceTest('disabled on empty', async ({expect, user, serviceTable0, serviceTable1}) => {
        const cloneButton = within(serviceTable0).getByTestId('clone0to1Button');
        await expectUnpressable(expect, user, cloneButton);

        //check that it is also unpressable after we return to empty from a non-empty state
        await addFirstRow(user, serviceTable0);
        await deleteRow(user, serviceTable0, 0);
        await expectUnpressable(expect, user, cloneButton);
      });
      emptyServiceTest('disabled on identical', async ({expect, user, serviceTable0, serviceTable1}) => {
        const cloneButton = within(serviceTable0).getByTestId('clone0to1Button');
        await addFirstRowBoth(user, serviceTable0, serviceTable1);
        await expectUnpressable(expect, user, cloneButton);
      });
    });
    completeServiceTest('works', async ({expect, user, serviceTable0, serviceTable1}) => {
      const cloneButton = within(serviceTable0).getByTestId('clone0to1Button');
      const row = await addFirstRow(user, serviceTable0);
      await populateRow(user, row);
      await user.click(cloneButton);
      expect(readTablePage(serviceTable0)).toStrictEqual(readTablePage(serviceTable1));
    });
  });
  describe('tables', () => { //confirm that mismatched tables prevent submission and clear/block the xCheck
    for(const side of ['left', 'right']) {
      describe(side, () => { //modifying the <side>-hand table
        completeServiceTest('blank', async ({expect, user, xCheck, serviceTable0, serviceTable1, servicesCommitButton}) => {
//console.log('** BLANK (0) **'); dumpTablesPage(serviceTable0, serviceTable1);
          await user.click(xCheck); //this state change would permit commit, but is about to get undone
          expect(getDV(xCheck)).toBe('true');

          await addFirstRow(user, side === 'left' ?  serviceTable0 : serviceTable1); //this state change should permit commit, but commit will be blocked due to other table not matching
//console.log('** BLANK (1) **'); dumpTablesPage(serviceTable0, serviceTable1);
          expect(getDV(xCheck)).toBe('false');
          await expectUnpressable(expect, user, xCheck);
          await expectUnpressable(expect, user, servicesCommitButton);
        });
        completeServiceTest('partial', async ({expect, user, xCheck, serviceTable0, serviceTable1, servicesCommitButton}) => {
//console.log('** PARTIAL (0)*'); dumpTablesPage(serviceTable0, serviceTable1);
          await user.click(xCheck); //this state change would permit commit, but is about to get undone
          expect(getDV(xCheck)).toBe('true');

          const row = await addFirstRow(user, side === 'left' ?  serviceTable0 : serviceTable1); //this state change should permit commit, but commit will be blocked due to other table not matching
          await partialPopulateRow(user, row);
//console.log('** PARTIAL (1)*'); dumpTablesPage(serviceTable0, serviceTable1);
          expect(getDV(xCheck)).toBe('false');
          await expectUnpressable(expect, user, xCheck);
          await expectUnpressable(expect, user, servicesCommitButton);
        });
        completeServiceTest('full', async ({expect, user, xCheck, serviceTable0, serviceTable1, servicesCommitButton}) => {
//console.log('** FULL  (0) **'); dumpTablesPage(serviceTable0, serviceTable1);
          await user.click(xCheck); //this state change would permit commit, but is about to get undone
          expect(getDV(xCheck)).toBe('true');

          const row = await addFirstRow(user, side === 'left' ?  serviceTable0 : serviceTable1); //this state change should permit commit, but commit will be blocked due to other table not matching
          await populateRow(user, row);
//console.log('** FULL  (1) **'); dumpTablesPage(serviceTable0, serviceTable1);
          expect(getDV(xCheck)).toBe('false');
          await expectUnpressable(expect, user, xCheck);
          await expectUnpressable(expect, user, servicesCommitButton);
        });
        describe('different', () => { //different row *content*, but same row count
          completeServiceTest('partial', async ({expect, user, xCheck, serviceTable0, serviceTable1, servicesCommitButton}) => {
            const fieldId = randomCellIdentifier();
//console.log('** DP    (0) **'); dumpTablesPage(serviceTable0, serviceTable1);
            const [leftRow] = await addFirstRowBoth(user, serviceTable0, serviceTable1);
            await partialPopulateRow(user, leftRow, { [fieldId]: null });
            await cloneFrom(user, serviceTable0);
//console.log('** DP    (1) **'); dumpTablesPage(serviceTable0, serviceTable1);

            await user.click(xCheck);
            expect(getDV(xCheck)).toBe('true');

            const field = getServiceCells(getRow(side === 'left' ?  serviceTable0 : serviceTable1, 0))[fieldId];
            await user.type(field, '0{Enter}');
//console.log('** DP    (2) **'); dumpTablesPage(serviceTable0, serviceTable1);

            expect(getDV(xCheck)).toBe('false');
            await expectUnpressable(expect, user, xCheck);
            await expectUnpressable(expect, user, servicesCommitButton);
          });
          completeServiceTest('full', async ({expect, user, xCheck, serviceTable0, serviceTable1, servicesCommitButton}) => {
//console.log('** DF    (0) **'); dumpTablesPage(serviceTable0, serviceTable1);
            const [leftRow] = await addFirstRowBoth(user, serviceTable0, serviceTable1);
            await populateRow(user, leftRow);
            await cloneFrom(user, serviceTable0);
//console.log('** DF    (1) **'); dumpTablesPage(serviceTable0, serviceTable1);
            await user.click(xCheck);
            expect(getDV(xCheck)).toBe('true');
            const field = getServiceCells(getRow(side === 'left' ?  serviceTable0 : serviceTable1, 0))[randomCellIdentifier()];
            await user.type(field, '0{Enter}');
//console.log('** DF    (2) **'); dumpTablesPage(serviceTable0, serviceTable1);

            expect(getDV(xCheck)).toBe('false');
            await expectUnpressable(expect, user, xCheck);
            await expectUnpressable(expect, user, servicesCommitButton);
          });
        });
      });
    }
  });
  describe('post', () => { //test that we can post fields OK
    describe('individual', () => {
      for(const field of EDITABLE_SERVICE_FIELDS) {
        completeServiceTest(field, async ({expect, user, getLastPost, serviceTable0, servicesCommitButton}) => {
          await partialPopulateRow(user, await addFirstRow(user, serviceTable0), { [field]: null });
          const expectation = readTablePage(serviceTable0)[0];
          await cloneFrom(user, serviceTable0);
          await user.click(servicesCommitButton);
          const { body } = await getLastPost();
          for(let i = 0; i < 2; i++) {
            expect(body.service.MAIN[i].rows.length).toBe(1);
            expect(body.service.MAIN[i].rows[0]).toStrictEqual(expectation);
            expect(body.service.MAIN[i].step).toBe('TRANSCRIBE' + (i + 1));
            expect(body.service.MAIN[i].complete).toBe(true);
          }
        });
      }
    });
    describe('full', () => {
      completeServiceTest('complete', async ({expect, user, getLastPost, serviceTable0, servicesCommitButton}) => {
        await populateRow(user, await addFirstRow(user, serviceTable0));
        const expectation = readTablePage(serviceTable0)[0];
        await cloneFrom(user, serviceTable0);
        await user.click(servicesCommitButton);
        const { body } = await getLastPost();
        for(let i = 0; i < 2; i++) {
          expect(body.service.MAIN[i].rows.length).toBe(1);
          expect(body.service.MAIN[i].rows[0]).toStrictEqual(expectation);
          expect(body.service.MAIN[i].step).toBe('TRANSCRIBE' + (i + 1));
          expect(body.service.MAIN[i].complete).toBe(true);
        }
      });
    });
    describe('random', () => { //TODO Should run this with and without xCheck set (and perhaps run all of the above services.post tests with and without xcheck set, too)
                               //     Leaving it for now as the xCheck set behaviour needs tidying up (e.g. should I only send one table in this case)
      for(let k = 0; k < N_MULTITESTS; k++) {
        completeServiceTest('multiple rows', async ({expect, user, getLastPost, serviceTable0, servicesCommitButton}) => { //I think this should be enough for random testing
          const nRows = random(4, 11); //tests get slower and slower as row count increases
          await(addFirstRow(user, serviceTable0));
          for(let i = 1; i < nRows; i++) {
            await addTopRow(user, serviceTable0); //addTopRow should be a touch faster, so is the best choice when I don't care exactly where the row is
          }
          for(let i = 0; i < nRows; i++) {
            const fields = [...(sampleSize(EDITABLE_SERVICE_TEXT_FIELDS, random(1, EDITABLE_SERVICE_TEXT_FIELDS.length))),
                            ...(sampleSize(EDITABLE_SERVICE_NUMERIC_FIELDS, random(1, EDITABLE_SERVICE_NUMERIC_FIELDS.length))),
            ]; //IIUC, sampleSize samples without replacement. Therefore this gets a random mix of unique text and numeric fields, but at least one of each.
            const content = {};
            for(const field of fields) content[field] = null;
            await(partialPopulateRow(user, getRow(serviceTable0, i), content));
          }
          const expectation = readTablePage(serviceTable0);
          await cloneFrom(user, serviceTable0);
          await user.click(servicesCommitButton);
          const { body } = await getLastPost();
          for(let i = 0; i < 2; i++) {
            expect(body.service.MAIN[i].rows.length).toBe(nRows);
            expect(body.service.MAIN[i].rows).toStrictEqual(expectation);
            expect(body.service.MAIN[i].step).toBe('TRANSCRIBE' + (i + 1));
            expect(body.service.MAIN[i].complete).toBe(true);
          }
        });
      }
    });
  });
});

//TODO Is this an API test? Feels much more like interface.
//TODO Implement the "single table" view and test that -- though it should be much the same thing, minus a few buttons
describe('servicereconciler', () => {
  for(const side of ['left', 'right']) {
    emptyServiceTest(`add first row (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;
      await addFirstRow(user, thisTable);
      expect(nTableRows(thisTable)).toBe(1);
      expect(getServiceData(getServiceCells(getRow(thisTable, 0)))).toStrictEqual(EMPTY_FIRST_ROW);
      expect(nTableRows(thatTable)).toBe(0);
      //TODO: expect the 'add first' button to be present in thatTable, expect it not to be in thisTable (perhaps in separate tests that establish this is what happens when nTableRows === 0)
    });
    blankServiceTest(`add row above single service entry (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;
      expect(nTableRows(thisTable)).toBe(1);
      await populateRow(user, getRow(thisTable, 0));
      const rowData = getServiceData(getServiceCells(getRow(thisTable, 0)));
      await user.click(within(getRow(thisTable, 0)).getByTestId('newRowAboveButton'));
      expect(nTableRows(thisTable)).toBe(2);
      expect(getServiceData(getServiceCells(getRow(thisTable, 0)))).toStrictEqual(EMPTY_FIRST_ROW);
      expect(getServiceData(getServiceCells(getRow(thisTable, 1)))).toStrictEqual({...rowData, rowid: 2});
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`add row above top of multiple service entries (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      await addNServiceRows(user, thisTable);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);
      for(const row of thisData) { row.rowid += 1; }

      //actual test
      await user.click(within(getRow(thisTable, 0)).getByTestId('newRowAboveButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([EMPTY_FIRST_ROW, ...thisData]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`add row above bottom of multiple service entries (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      const extraRowCount = await addNServiceRows(user, thisTable);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);
      const lastThisData = thisData.pop();
      lastThisData.rowid += 1;

      //actual test
      await user.click(within(getRow(thisTable, -1)).getByTestId('newRowAboveButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([...thisData, { ...EMPTY_FIRST_ROW, rowid: extraRowCount + 1 }, lastThisData]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`add row above in middle of multiple service entries (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //precondition
      await addNServiceRows(user, thisTable, 3);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);

      //actual test
      await user.click(within(getRow(thisTable, 2)).getByTestId('newRowAboveButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([thisData[0], thisData[1], {...EMPTY_FIRST_ROW, rowid: 3}, {...thisData[2], rowid: 4}, {...thisData[3], rowid: 5}]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`add row below single service entry (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;
      expect(nTableRows(thisTable)).toBe(1);
      await populateRow(user, getRow(thisTable, 0));
      const rowData = getServiceData(getServiceCells(getRow(thisTable, 0)));
      await user.click(within(getRow(thisTable, 0)).getByTestId('newRowBelowButton'));
      expect(nTableRows(thisTable)).toBe(2);
      expect(getServiceData(getServiceCells(getRow(thisTable, 0)))).toStrictEqual(rowData);
      expect(getServiceData(getServiceCells(getRow(thisTable, 1)))).toStrictEqual({...EMPTY_FIRST_ROW, rowid: 2});
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`add row below bottom of multiple service entries (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      const extraRowCount = await addNServiceRows(user, thisTable);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);

      //actual test
      await user.click(within(getRow(thisTable, -1)).getByTestId('newRowBelowButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([...thisData, { ...EMPTY_FIRST_ROW, rowid: extraRowCount + 2 }]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`add row below top of multiple service entries (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      const extraRowCount = await addNServiceRows(user, thisTable);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);
      const firstThisData = thisData.shift();
      for(const row of thisData) { row.rowid += 1; }

      //actual test
      await user.click(within(getRow(thisTable, 0)).getByTestId('newRowBelowButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([firstThisData, { ...EMPTY_FIRST_ROW, rowid: 2 }, ...thisData]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`add row below in middle of multiple service entries (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      await addNServiceRows(user, thisTable, 3);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);

      //actual test
      await user.click(within(getRow(thisTable, 1)).getByTestId('newRowBelowButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([thisData[0], thisData[1], {...EMPTY_FIRST_ROW, rowid: 3}, {...thisData[2], rowid: 4}, {...thisData[3], rowid: 5}]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`delete single service entry (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;
      expect(nTableRows(thisTable)).toBe(1);
      await user.click(within(getRow(thisTable, 0)).getByTestId('deleteRowButton'));
      expect(nTableRows(thisTable)).toBe(0);
      //TODO: Expect the "add first row" button to have appeared
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`delete top service entry of two (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      await addNServiceRows(user, thisTable, 1);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);

      //test
      await user.click(within(getRow(thisTable, 0)).getByTestId('deleteRowButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([{...(thisData[1]), rowid: 1}]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`delete bottom service entry of two (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      await addNServiceRows(user, thisTable, 1);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);

      //test
      await user.click(within(getRow(thisTable, 1)).getByTestId('deleteRowButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([thisData[0]]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`delete top service entry of multiple (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      await addNServiceRows(user, thisTable, 2);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);

      //test
      await user.click(within(getRow(thisTable, 0)).getByTestId('deleteRowButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([{...(thisData[1]), rowid: 1}, {...(thisData[2]), rowid: 2}]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`delete bottom service entry of multiple (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      await addNServiceRows(user, thisTable, 2);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);
      thisData.pop();

      //test
      await user.click(within(getRow(thisTable, 2)).getByTestId('deleteRowButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual(thisData);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`delete middle service entry of multiple (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //preconditions
      await addNServiceRows(user, thisTable, 2);
      await populateRows(user, thisTable, 'this');
      const thisData = getPageServiceData(thisTable);

      //test
      await user.click(within(getRow(thisTable, 1)).getByTestId('deleteRowButton'));
      expect(getPageServiceData(thisTable)).toStrictEqual([{...(thisData[0]), rowid: 1}, {...(thisData[2]), rowid: 2}]);
      expect(getPageServiceData(thatTable)).toStrictEqual([EMPTY_FIRST_ROW]);
    });
    blankServiceTest(`overwrite other (from ${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;
      await populateRow(user, getRow(thisTable, 0));
      const rowData = getServiceData(getServiceCells(getRow(thisTable, 0)));
      await user.click(within(getRow(thisTable, 0)).getByTestId('overwriteOtherButton'));
      expect(nTableRows(thisTable)).toBe(1);
      expect(nTableRows(thatTable)).toBe(1);
      expect(getServiceData(getServiceCells(getRow(thisTable, 0)))).toStrictEqual(rowData);
      expect(getServiceData(getServiceCells(getRow(thatTable, 0)))).toStrictEqual(rowData);
    });
    blankServiceTest(`insert other (from ${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;
      await populateRow(user, getRow(thisTable, 0));
      const rowData = getServiceData(getServiceCells(getRow(thisTable, 0)));
      await user.click(within(getRow(thisTable, 0)).getByTestId('insertOtherButton'));
      expect(nTableRows(thisTable)).toBe(1);
      expect(nTableRows(thatTable)).toBe(2);
      expect(getServiceData(getServiceCells(getRow(thisTable, 0)))).toStrictEqual(rowData);
      expect(getServiceData(getServiceCells(getRow(thatTable, 0)))).toStrictEqual(rowData);
      expect(getServiceData(getServiceCells(getRow(thatTable, 1)))).toStrictEqual({...EMPTY_FIRST_ROW, rowid: 2});
    });
    for(const copyMode of ['insert', 'overwrite']) {
      blankServiceTest(`${copyMode} directly below from directly below (from ${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
        const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
        const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

        //preconditions -- thisTable has one extra row, with some random content
        await user.click(within(getRow(thisTable, 0)).getByTestId('newRowBelowButton'));
        expect(nTableRows(thisTable)).toBe(2);
        await populateRow(user, getRow(thisTable, 0), { ship: 'this.one' });
        await populateRow(user, getRow(thisTable, 1), { ship: 'this.two' });
        await populateRow(user, getRow(thatTable, 0), { ship: 'that.one' });
        const thisData = getPageServiceData(thisTable);
        const thatData = getPageServiceData(thatTable);

        //actual test
        await user.click(within(getRow(thisTable, -1)).getByTestId(copyMode + 'OtherButton'));
        expect(getPageServiceData(thisTable)).toStrictEqual(thisData);
        expect(getPageServiceData(thatTable)).toStrictEqual([...thatData, thisData[1]]);
      });
      blankServiceTest(`${copyMode} directly below from deeper below (from ${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
        const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
        const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

        //preconditions -- thisTable has n extra rows, with some random content
        await addNServiceRows(user, thisTable);
        await populateRows(user, thisTable, 'this');
        await populateRow(user, getRow(thatTable, 0), { ship: 'that1' });
        const thisData = getPageServiceData(thisTable);
        const thatData = getPageServiceData(thatTable);

        //actual test
        await user.click(within(getRow(thisTable, -1)).getByTestId(copyMode + 'OtherButton'));
        expect(getPageServiceData(thisTable)).toStrictEqual(thisData);
        expect(getPageServiceData(thatTable)).toStrictEqual([...thatData, {...thisData.at(-1), rowid: 2}]); //bottom row of this table appears at bottom of thatTable, with updated rowid
      });
      blankServiceTest(`${copyMode} random middle (bigger) to random middle (smaller) (from ${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
        const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
        const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

        //preconditions (these tables are guaranteed to both have a row at the index being copied, "missing row" cases are covered above)
        const thisExtraRows = await addNServiceRows(user, thisTable, random(5, 7));
        const thatExtraRows = await addNServiceRows(user, thatTable, random(3, 4));
        expect(thisExtraRows).toBeGreaterThan(thatExtraRows);
        await populateRows(user, thisTable, 'this'); //a handy side-effect of the this/that labelling is that it is impossible for rows in different tables to contain identical data
        await populateRows(user, thatTable, 'that');
        const thisData = getPageServiceData(thisTable);
        const thatData = getPageServiceData(thatTable);
        const baseRow = random(1, 2); //smaller side has minimum 3 + 1 = 4 rows, indexed 0 - 3. Indices 1 and 2 therefore count as "middle".

        //test
        await user.click(within(getRow(thisTable, baseRow)).getByTestId(copyMode + 'OtherButton'));
        expect(getPageServiceData(thisTable)).toStrictEqual(thisData);
        if(copyMode === 'insert') {
          const postscript = thatData.slice(baseRow);
          for(const row of postscript) row.rowid += 1;
          expect(getPageServiceData(thatTable)).toStrictEqual([...(thatData.slice(0, baseRow)), thisData[baseRow], ...postscript]);
        }
        else if(copyMode === 'overwrite') {
          expect(getPageServiceData(thatTable)).toStrictEqual([...(thatData.slice(0, baseRow)), thisData[baseRow], ...(thatData.slice(baseRow + 1))]);
        }
        else {
          throw new Error();//unreachable
        }
      });
      blankServiceTest(`${copyMode} random middle (smaller) to random middle (bigger) (from ${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
        const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
        const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

        //preconditions (these tables are guaranteed to both have a row at the index being copied, "missing row" cases are covered above)
        const thisExtraRows = await addNServiceRows(user, thisTable, random(3, 4));
console.log(prettyDOM(thisTable));
        const thatExtraRows = await addNServiceRows(user, thatTable, random(5, 7));
        expect(thisExtraRows).toBeLessThan(thatExtraRows);
        await populateRows(user, thisTable, 'this'); //a handy side-effect of the this/that labelling is that it is impossible for rows in different tables to contain identical data
        await populateRows(user, thatTable, 'that');
        const thisData = getPageServiceData(thisTable);
        const thatData = getPageServiceData(thatTable);
        const baseRow = random(1, 2); //smaller side has minimum 3 + 1 = 4 rows, indexed 0 - 3. Indices 1 and 2 therefore count as "middle".

        //test
        await user.click(within(getRow(thisTable, baseRow)).getByTestId(copyMode + 'OtherButton'));
        expect(getPageServiceData(thisTable)).toStrictEqual(thisData);
        if(copyMode === 'insert') {
          const postscript = thatData.slice(baseRow);
          for(const row of postscript) row.rowid += 1;
          expect(getPageServiceData(thatTable)).toStrictEqual([...(thatData.slice(0, baseRow)), thisData[baseRow], ...postscript]);
        }
        else if(copyMode === 'overwrite') {
          expect(getPageServiceData(thatTable)).toStrictEqual([...(thatData.slice(0, baseRow)), thisData[baseRow], ...(thatData.slice(baseRow + 1))]);
        }
        else {
          throw new Error();//unreachable
        }
      });
    }
    pageServiceTest(`add row above top of full page (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      const expectedThisData = [EMPTY_FIRST_ROW];
      for(const row of getPageServiceData(thisTable)) { //initial data (we know this test case starts with exactly one full page
        row.rowid += 1;
        expectedThisData.push(row);
      }
      const expectedThatData = getPageServiceData(thatTable); //we know that this test case starts with exactly one full page

      //test action
      await user.click(within(getRow(thisTable, 0)).getByTestId('newRowAboveButton'));

      //new data
      const postData = getPageServiceData(thisTable);
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      postData.push(...(getPageServiceData(thisTable, 1)));
      expect(postData).toStrictEqual(expectedThisData);

      //thatTable is unchanged
      expect(getPageServiceData(thatTable)).toStrictEqual(expectedThatData);
      expect(nTableRows(thatTable)).toBe(PAGE_ROWS);
    });
    pageServiceTest(`add row below top of full page (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      const thisData = getPageServiceData(thisTable); //initial data (we know this test case starts with exactly one full page
      const expectedThisData = [thisData.shift(), {...EMPTY_FIRST_ROW, rowid: 2}];
      for(const row of thisData) {
        row.rowid += 1;
        expectedThisData.push(row);
      }
      const expectedThatData = getPageServiceData(thatTable); //we know that this test case starts with exactly one full page

      //test action
      await user.click(within(getRow(thisTable, 0)).getByTestId('newRowBelowButton'));

      //new data
      const postData = getPageServiceData(thisTable);
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      postData.push(...(getPageServiceData(thisTable, 1)));
      expect(postData).toStrictEqual(expectedThisData);

      //thatTable is unchanged
      expect(getPageServiceData(thatTable)).toStrictEqual(expectedThatData);
      expect(nTableRows(thatTable)).toBe(PAGE_ROWS);
    });
    pageServiceTest(`add row below bottom of full page (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      const expectedThisData = [...(getPageServiceData(thisTable)), {...EMPTY_FIRST_ROW, rowid: PAGE_ROWS + 1}];
      const expectedThatData = getPageServiceData(thatTable); //we know that this test case starts with exactly one full page

      //test action
      await user.click(within(getRow(thisTable, -1)).getByTestId('newRowBelowButton')); //this works because we have not yet got more than one page

      //new data
      const postData = getPageServiceData(thisTable);
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      postData.push(...(getPageServiceData(thisTable, 1)));
      expect(postData).toStrictEqual(expectedThisData);

      //thatTable is unchanged
      expect(getPageServiceData(thatTable)).toStrictEqual(expectedThatData);
      expect(nTableRows(thatTable)).toBe(PAGE_ROWS);
    });
    pageServiceTest(`add row above bottom of full page (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      const thisData = getPageServiceData(thisTable);
      const expectedThisData = [...(thisData.slice(0, -1)), {...EMPTY_FIRST_ROW, rowid: PAGE_ROWS}, {...(thisData.at(-1)), rowid: PAGE_ROWS + 1}];
      const expectedThatData = getPageServiceData(thatTable); //we know that this test case starts with exactly one full page

      //test action
      await user.click(within(getRow(thisTable, -1)).getByTestId('newRowAboveButton')); //this works because we have not yet got more than one page

      //new data
      const postData = getPageServiceData(thisTable);
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      postData.push(...(getPageServiceData(thisTable, 1)));
      expect(postData).toStrictEqual(expectedThisData);

      //thatTable is unchanged
      expect(getPageServiceData(thatTable)).toStrictEqual(expectedThatData);
      expect(nTableRows(thatTable)).toBe(PAGE_ROWS);
    });
    pageServiceTest(`add row below in middle of full page (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      const targetRow = random(40, 60);
      const thisData = getPageServiceData(thisTable);
      for(const row of thisData.slice(targetRow + 1)) row.rowid += 1;
      const expectedThisData = [...(thisData.slice(0, targetRow + 1)), {...EMPTY_FIRST_ROW, rowid: targetRow + 2}, ...(thisData.slice(targetRow + 1))];
      const expectedThatData = getPageServiceData(thatTable); //we know that this test case starts with exactly one full page

      //test action
      await user.click(within(getRow(thisTable, targetRow)).getByTestId('newRowBelowButton')); //this works because we have not yet got more than one page

      //new data
      const postData = getPageServiceData(thisTable);
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      postData.push(...(getPageServiceData(thisTable, 1)));
      expect(postData).toStrictEqual(expectedThisData);

      //thatTable is unchanged
      expect(getPageServiceData(thatTable)).toStrictEqual(expectedThatData);
      expect(nTableRows(thatTable)).toBe(PAGE_ROWS);
    });
    pageServiceTest(`add row above in middle of full page (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      const targetRow = random(40, 60);
      const thisData = getPageServiceData(thisTable);
      for(const row of thisData.slice(targetRow)) row.rowid += 1;
      const expectedThisData = [...(thisData.slice(0, targetRow)), {...EMPTY_FIRST_ROW, rowid: targetRow + 1}, ...(thisData.slice(targetRow))];
      const expectedThatData = getPageServiceData(thatTable); //we know that this test case starts with exactly one full page

      //test action
      await user.click(within(getRow(thisTable, targetRow)).getByTestId('newRowAboveButton')); //this works because we have not yet got more than one page

      //new data
      const postData = getPageServiceData(thisTable);
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      postData.push(...(getPageServiceData(thisTable, 1)));
      expect(postData).toStrictEqual(expectedThisData);

      //thatTable is unchanged
      expect(getPageServiceData(thatTable)).toStrictEqual(expectedThatData);
      expect(nTableRows(thatTable)).toBe(PAGE_ROWS);
    });
    twoPageServiceTest(`delete row at top of full two pages (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //we know that this test case starts with exactly two full pages
      const expectedThisData = getPageServiceData(thisTable);
      expectedThisData.shift();
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      expectedThisData.push(...(getPageServiceData(thisTable, 1)));
      for(const row of expectedThisData) row.rowid -= 1;
      await user.click(within(thisTable).getByAriaLabel('Go to previous page'));

      const expectedThatData = getPageServiceData(thatTable);
      await user.click(within(thatTable).getByAriaLabel('Go to next page'));
      expectedThatData.push(...(getPageServiceData(thatTable, 1)));
      await user.click(within(thatTable).getByAriaLabel('Go to previous page'));

      //test action
      await user.click(within(getRow(thisTable, 0)).getByTestId('deleteRowButton'));

      //new data
      const postThisData = getPageServiceData(thisTable);
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      postThisData.push(...(getPageServiceData(thisTable, 1)));
      expect(postThisData).toStrictEqual(expectedThisData);

      //thatTable is unchanged
      const postThatData = getPageServiceData(thatTable);
      await user.click(within(thatTable).getByAriaLabel('Go to next page'));
      postThatData.push(...(getPageServiceData(thatTable, 1)));
      expect(postThatData).toStrictEqual(expectedThatData);
      expect(nTableRows(thatTable)).toBe(PAGE_ROWS * 2); //confirms that there is not another page to look at
    });
    twoPageServiceTest(`delete row at bottom of first of full two pages (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => {
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //we know that this test case starts with exactly two full pages
      const expectedThisData = getPageServiceData(thisTable);
      expectedThisData.pop();
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      expectedThisData.push(...(getPageServiceData(thisTable, 1)));
      for(const row of expectedThisData.slice(PAGE_ROWS - 1)) row.rowid -= 1;
      await user.click(within(thisTable).getByAriaLabel('Go to previous page'));

      const expectedThatData = getPageServiceData(thatTable);
      await user.click(within(thatTable).getByAriaLabel('Go to next page'));
      expectedThatData.push(...(getPageServiceData(thatTable, 1)));
      await user.click(within(thatTable).getByAriaLabel('Go to previous page'));

      //test action
      await user.click(within(getRow(thisTable, PAGE_ROWS - 1)).getByTestId('deleteRowButton'));

      //new data
      const postThisData = getPageServiceData(thisTable);
      await user.click(within(thisTable).getByAriaLabel('Go to next page'));
      postThisData.push(...(getPageServiceData(thisTable, 1)));
      expect(postThisData).toStrictEqual(expectedThisData);

      //thatTable is unchanged
      const postThatData = getPageServiceData(thatTable);
      await user.click(within(thatTable).getByAriaLabel('Go to next page'));
      postThatData.push(...(getPageServiceData(thatTable, 1)));
      expect(postThatData).toStrictEqual(expectedThatData);
      expect(nTableRows(thatTable)).toBe(PAGE_ROWS * 2); //confirms that there is not another page to look at
    });
    pageServiceTest(`insert row in middle of full page (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => { //TODO
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //we know that this test case starts with exactly one full page
      const expectedThisData = getPageServiceData(thisTable);

      const targetRow = random(40, 60);
      const thatData = getPageServiceData(thatTable);
      const expectedThatData = thatData.slice(0, targetRow);
      expectedThatData.push(expectedThisData[targetRow]);
      expectedThatData.push(...(thatData.slice(targetRow)));
      for(const row of expectedThatData.slice(targetRow + 1)) {
        row.rowid += 1;
      }

      //test action
      await user.click(within(getRow(thisTable, targetRow)).getByTestId('insertOtherButton'));

      //thisTable is unchanged
      const postThisData = getPageServiceData(thisTable);
      expect(postThisData).toStrictEqual(expectedThisData);
      expect(nTableRows(thisTable)).toBe(PAGE_ROWS);

      //new data
      const postThatData = getPageServiceData(thatTable);
      await user.click(within(thatTable).getByAriaLabel('Go to next page'));
      postThatData.push(...(getPageServiceData(thatTable, 1)));
      expect(postThatData).toStrictEqual(expectedThatData);
    });
    pageServiceTest(`overwrite row in middle of full page (${side})`, async ({expect, user, serviceTable0, serviceTable1}) => { //TODO
      const thisTable = side === 'left' ? serviceTable0 : serviceTable1;
      const thatTable = side === 'left' ? serviceTable1 : serviceTable0;

      //we know that this test case starts with exactly one full page
      const expectedThisData = getPageServiceData(thisTable);

      const targetRow = random(40, 60);
      const thatData = getPageServiceData(thatTable);
      const expectedThatData = thatData.slice(0, targetRow);
      expectedThatData.push(expectedThisData[targetRow]);
      expectedThatData.push(...(thatData.slice(targetRow + 1)));

      //test action
      await user.click(within(getRow(thisTable, targetRow)).getByTestId('overwriteOtherButton'));

      //thisTable is unchanged
      const postThisData = getPageServiceData(thisTable);
      expect(postThisData).toStrictEqual(expectedThisData);
      expect(nTableRows(thisTable)).toBe(PAGE_ROWS);

      //new data
      const postThatData = getPageServiceData(thatTable);
      expect(postThatData).toStrictEqual(expectedThatData);
      expect(nTableRows(thisTable)).toBe(PAGE_ROWS);
    });
  }
});

baseTest.extend(FIXTURES.dataTest(100124))('SECOND API TEST', async ({expect, user, getLastPost, serviceTable0, serviceTable1, servicesCommitButton}) => {
  await populateRow(user, await addFirstRow(user, serviceTable0), {
    ship:      'Indus',
    rating:    'Butch',
    fromday:   5,
    frommonth: 1,
    fromyear:  1869,
    today:     7,
    tomonth:   2,
    toyear:    1874,
  });
  await user.click(within(serviceTable0).getByTestId('completeCheckbox'));
  await user.click(within(serviceTable0).getByTestId('clone0to1Button'));
  await user.click(within(serviceTable1).getByTestId('completeCheckbox'));
  await user.click(servicesCommitButton);

  const lastPost = await getLastPost();
  const main = lastPost.body.service.MAIN;
  expect(Array.isArray(main)).toBe(true);
  expect(main.length).toBe(2);
  for(const {rows} of main) {
    expect(rows.length).toBe(1);
    expect(rows[0]).toStrictEqual({
      row_number: 1,
      ship: 'Indus',
      rating: 'Butch',
      fromday: 5,
      frommonth: 1,
      fromyear: 1869,
      today: 7,
      tomonth: 2,
      toyear: 1874,
    });
  }
});
