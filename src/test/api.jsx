// @vitest-environment jsdom

import { prettyDOM, routeRender, within, mockServer } from './config/testutils';
import userEvent from '@testing-library/user-event';
import { cleanup } from '@testing-library/react';
import Person from '@/person.jsx';
import { describe, test as baseTest, vi } from 'vitest';
import { dump } from './config/testutils';

//Following tricks https://stackoverflow.com/a/72289488, http://pawelgoscicki.com/archives/2022/05/testing-usenavigate-navigate-from-react-router-v6/, https://mayashavin.com/articles/two-shades-of-mocking-vitest to spy on useParams
import * as router from 'react-router';

//This is encapsualated to prevent tests from accidentally picking up null variables or any state accidentally left hanging
const FIXTURES = (function(){
  let user = null;
  let component = null;
  let birthYear = null;
  let personTable = null;
  let personCommitButton = null;
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
          notWW1 = await component.findByTestId('notWW1');
          await use(notWW1);
          notWW1 = null;
        },
        errorToggle: async({component}, use) => {
          errorToggle = await component.findByTestId('errorToggle');
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

function getServiceCells(row) {
  const cells = {};
  for(const field of SERVICE_FIELDS) {
    cells[field] = within(row).getByField(field);
  }
  return cells;
}

function getRow(table, index) {
  return within(table).getByRowIndex(index);
}

function getCheckboxes(tables) {
  const checkboxes = [];
  for(const table of tables) {
    const component = within(table).getByTestId('completeCheckbox');
    const state = component.getAttribute('data-value');
    checkboxes.push({component: component, state: state});
  };
  return checkboxes;
}

function unpressable(expect, user, button) {
  return expect(user.click(button)).rejects.toThrow('Unable to perform pointer interaction as the element has `pointer-events: none`');
}

const it = baseTest.extend(FIXTURES.dataTest());
const fullPersonTest  = baseTest.extend(FIXTURES.dataTest(9999999901));
const emptyPersonTest = baseTest.extend(FIXTURES.dataTest(9999999902));
describe('data flow', () => {
  describe('commit url', () => {
    fullPersonTest('person', async({expect, user, getLastPost, birthYear, personCommitButton}) => {
      await user.clear(birthYear); //just to enable the button
      await user.click(personCommitButton);
      const {url} = await getLastPost();
      expect(url).toMatch(/\/person$/);
    });
    fullPersonTest.todo('personTriple', async({expect, user, postSpy, birthYear, personCommitButton}) => {
      await user.clear(birthYear); //just to enable the button
      await user.tripleClick(personCommitButton); //after the first click, subsequents should fail -- but in any case, result should be a single post
      expect(postSpy).toHaveBeenCalledOnce();
      user.click(personCommitButton); //TODO: That I can still press the button suggests something might be wrong with how the test library sees current state. Maybe not critical.
    });
    fullPersonTest.todo('personThrice', async({expect, user, postSpy, birthYear, personCommitButton}) => {
      await user.clear(birthYear); //just to enable the button
      await Promise.all([
        user.click(personCommitButton),
        user.click(personCommitButton),
        user.click(personCommitButton),
      ]);
      expect(postSpy).toHaveBeenCalledOnce(); //TODO: Disappointing that this fails. But not mission-critical.
    });
    //TODO: Multi-click tests for services. A multi-click test for services + person data (maybe the most interesting case)
    it('services', async({expect, user, getLastPost, serviceTable0, serviceTable1, component}) => {
      //all this just to enable the button
      await user.click(within(await serviceTable0).getByTestId('firstRowButton'));

      //this part more checking the preconditions that part of the test
      //i.e. do I need to press the 'complete' checkboxes in order to submit?
      const cb = getCheckboxes([serviceTable0, serviceTable1]);
      expect(cb[0].state).toBe('true');
      expect(cb[1].state).toBe('true');

      const fields = getServiceCells(getRow(serviceTable0, 0));
      await user.type(fields.ship, 'Indus{Enter}');
      await user.click(within(serviceTable0).getByTestId('clone0to1Button'));
      //now ready to press the button!

      await user.click(component.getByTestId('servicesCommitButton'));
      const {url} = await getLastPost();
      expect(url).toMatch(/\/person$/);
    });
  });
  describe('trim individually', () => {
    for(const field of EDITABLE_PERSON_TEXT_FIELDS) {
      fullPersonTest('post-trim full text ' + field, async ({expect, user, getLastPost, personTable, personCommitButton}) => { //Confirm that all input person data gets trimmed. Doesn't really need to be the 'full' data, just need to start somewhere. But using real data lets me confirm that the trimmed text results in something un-submittable.
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));
        const baseText = fieldComponent.getAttribute('value');

        await user.clear(fieldComponent);
        await user.type(fieldComponent, baseText + '  {Enter}');
        expect(fieldComponent.getAttribute('value')).toBe(baseText);
        await unpressable(expect, user, personCommitButton);
      });
      fullPersonTest('pre-trim full text ' + field, async ({expect, user, getLastPost, personTable, personCommitButton}) => { //Confirm that all input person data gets trimmed. Doesn't really need to be the 'full' data, just need to start somewhere. But using real data lets me confirm that the trimmed text results in something un-submittable.
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));
        const baseText = fieldComponent.getAttribute('value');

        await user.clear(fieldComponent);
        await user.type(fieldComponent, ' ' + baseText + '{Enter}');
        expect(fieldComponent.getAttribute('value')).toBe(baseText);
        await unpressable(expect, user, personCommitButton);
      });
      emptyPersonTest('trim empty text ' + field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));
        const baseText = fieldComponent.getAttribute('value');
        expect(baseText).toBe('');

        await user.type(fieldComponent, '    ' + baseText + '{Enter}');
        expect(fieldComponent.getAttribute('value')).toBe(baseText);
        await unpressable(expect, user, personCommitButton);
      });
    }
    for(const field of EDITABLE_PERSON_NUMERIC_FIELDS) {
      fullPersonTest('post-trim numeric ' + field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));
        const base = fieldComponent.getAttribute('value');
        expect(base).toMatch(/^\d+$/);

        await user.clear(fieldComponent);
        await user.type(fieldComponent, base + ' {Enter}');
        expect(fieldComponent.getAttribute('value')).toBe(base);
        await unpressable(expect, user, personCommitButton);
      });
      fullPersonTest('pre-trim numeric ' + field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));
        const base = fieldComponent.getAttribute('value');
        expect(base).toMatch(/^\d+$/);

        await user.clear(fieldComponent);
        await user.type(fieldComponent, ' ' + base + ' {Enter}');
        expect(fieldComponent.getAttribute('value')).toBe(base);
        await unpressable(expect, user, personCommitButton);
      });
      emptyPersonTest('trim empty numeric ' + field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));
        const base = fieldComponent.getAttribute('value');
        expect(base).toBe('0');

        await user.clear(fieldComponent);
        await user.type(fieldComponent, '     {Enter}');
        expect(fieldComponent.getAttribute('value')).toBe('0');
        await unpressable(expect, user, personCommitButton);
      });
    }
  });
  describe.todo('edit field individually', ()=>{}); //need tests that actually enter data into each field and confirm that it gets posted correctly. Much like the 'clear' tests below, except involve typing rather than just clearing
  describe('clear person individually', () => {
    for(const field of EDITABLE_PERSON_TEXT_FIELDS) {
      fullPersonTest('full text ' + field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));
        expect(fieldComponent.getAttribute('value')).not.toBe(''); //precondition -- we are clearing the field, so to be able to submit it to test API POST, it must not be empty
        await user.clear(fieldComponent);
        await user.type(fieldComponent, '{Enter}'); //seem to need this to pick up the change
        await user.click(personCommitButton);

        const { body } = await getLastPost();
        expect(body.person[field]).toEqual('');
      });
      emptyPersonTest('empty text ' + field, async ({expect, user, postSpy, personTable, personCommitButton}) => {
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));
        expect(fieldComponent.getAttribute('value')).toBe(''); //precondition -- we are testing that we cannot post if we change the record to be the same as it was before
        await user.clear(fieldComponent);
        await user.type(fieldComponent, '{Enter}');
        await unpressable(expect, user, personCommitButton);

        await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button
      });
    }
    for(const field of EDITABLE_PERSON_NUMERIC_FIELDS) {
      fullPersonTest('full numeric ' + field, async ({expect, user, getLastPost, personTable, personCommitButton}) => {
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));

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
      emptyPersonTest('empty numeric ' + field, async ({expect, user, postSpy, personTable, personCommitButton}) => {
        const fieldComponent = await within(personTable).findByTestId(field).then((x)=>x.querySelector('input'));

        //precondition -- we are clearing the field, so to be able to submit it to test API POST, it must not be empty
        expect(fieldComponent.getAttribute('value')).toBe('0'); //seems that whatever we read from this field will be stringified, so check the type as well. That we actually end up posting a numeric 0 in such cases should be tested elsewhere
        expect(fieldComponent.getAttribute('type')).toBe('number');

        await user.clear(fieldComponent);
        await user.type(fieldComponent, '{Enter}');
        await unpressable(expect, user, personCommitButton);
        await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button

        //test with the explicit 0 too
        await user.clear(fieldComponent);
        await user.type(fieldComponent, '0{Enter}');
        await unpressable(expect, user, personCommitButton);
        await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button
      });
    }
  });
  describe('toggles', () => {
    fullPersonTest('not ww1 true to false', async ({expect, user, getLastPost, notWW1, personCommitButton}) => { //baselines to true
      //expected pre-condition
      expect(notWW1.getAttribute('data-value')).toBe('true'); //stringified in the DOM
      await unpressable(expect, user, personCommitButton);

      await user.click(notWW1);
      await user.click(personCommitButton);
      const { body } = await getLastPost();
      expect(body.person.notww1).toBe(false);
    });
    emptyPersonTest('not ww1 false to true', async ({expect, user, getLastPost, notWW1, personCommitButton}) => { //baselines to false
      //expected pre-condition
      expect(notWW1.getAttribute('data-value')).toBe('false'); //stringified in the DOM
      await unpressable(expect, user, personCommitButton);

      await user.click(notWW1);
      await user.click(personCommitButton);
      const { body } = await getLastPost();
      expect(body.person.notww1).toBe(true);
    });
    fullPersonTest('error true to false', async ({expect, user, postSpy, errorToggle, personCommitButton}) => { //baselines to true. Reporting only, cannot be changed.
      //expected pre-condition
      expect(errorToggle.getAttribute('data-value')).toBe('true'); //stringified in the DOM
      await unpressable(expect, user, personCommitButton);

      await unpressable(expect, user, errorToggle);
      await unpressable(expect, user, personCommitButton);
      await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button
    });
    emptyPersonTest('error false to true', async ({expect, user, postSpy, errorToggle, personCommitButton}) => { //baselines to false. Reporting only, cannot be changed.
      //expected pre-condition
      expect(errorToggle.getAttribute('data-value')).toBe('false'); //stringified in the DOM
      await unpressable(expect, user, personCommitButton);

      await unpressable(expect, user, errorToggle);
      await unpressable(expect, user, personCommitButton);
      await expect(vi.waitFor(()=>expect(postSpy).not.toHaveBeenCalled())); //not sure about the timing here, but this is anyway not really necessary -- it really should not have been called if we cannot press the button
    });
  });
  describe.todo('catref', ()=>{}); //check the the catref matches the series and actual item number. manufacture cases where other ususal suspects (officialnumber, nameid) are different from the item number. could be that my existing test data already happens to have these properties.

  baseTest.extend(FIXTURES.dataTest(100124))('SECOND API TEST', async ({expect, user, getLastPost, serviceTable0, serviceTable1, component}) => {
    await user.click(within(await serviceTable0).getByTestId('firstRowButton'));
    const fields = getServiceCells(getRow(serviceTable0, 0));
    await user.type(fields.ship, 'Indus{Enter}');
    await user.type(fields.rating, 'Butch{Enter}');
    await user.type(fields.fromday, '5{Enter}');
    await user.type(fields.frommonth, '1{Enter}');
    await user.type(fields.fromyear, '1869{Enter}');
    await user.type(fields.today, '7{Enter}');
    await user.type(fields.tomonth, '2{Enter}');
    await user.type(fields.toyear, '1874{Enter}');
    await user.click(within(serviceTable0).getByTestId('completeCheckbox'));
    await user.click(within(serviceTable0).getByTestId('clone0to1Button'));
    await user.click(within(serviceTable1).getByTestId('completeCheckbox'));
    await user.click(component.getByTestId('servicesCommitButton'));

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
});
