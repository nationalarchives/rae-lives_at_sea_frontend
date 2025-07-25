// @vitest-environment jsdom

import { prettyDOM, routeRender, within } from './config/testutils';
import userEvent from '@testing-library/user-event';
import { cleanup } from '@testing-library/react';
import Person from '@/person.jsx';
import { describe, test as baseTest, vi } from 'vitest';

//Following tricks https://stackoverflow.com/a/72289488, http://pawelgoscicki.com/archives/2022/05/testing-usenavigate-navigate-from-react-router-v6/, https://mayashavin.com/articles/two-shades-of-mocking-vitest to spy on useParams
import * as router from 'react-router';

//TODO: Encapsulate this -- could put it in a different file and not export, for example
let paramSpy = null;
let user = null;
let component = null;
let birthYear = null;
let personCommitButton = null;
let serviceTable0 = null;
let serviceTable1 = null;

function dataTest(personId = 379254, sailorType = 'rating') {
  return {
    paramSpy: async({}, use) => {
      paramSpy = vi.spyOn(router, 'useParams');
      await use(paramSpy);
      paramSpy.mockReset();
      paramSpy = null;
    },
    postSpy: async({}, use) => {

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
      birthYear = await component.findByTestId('birthyearTextField', undefined, { timeout: 10000 }).then((x)=>x.querySelector('input'));
      await use(birthYear);
      birthYear = null;
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
  }
}

const it = baseTest.extend(dataTest());

describe('getting started', () => {
  it('FIRST API TEST', async ({birthYear, personCommitButton}) => {
    await(user.clear(birthYear));
    await user.click(personCommitButton);
  });
  baseTest.extend(dataTest(100124))('SECOND API TEST', async ({serviceTable0, serviceTable1, component}) => {
    await user.click(within(await serviceTable0).getByTestId('firstRowButton'));
    const firstRow = within(serviceTable0).getByRowIndex(0)
    await user.type(within(firstRow).getByField('ship'), 'Indus{Enter}');
    await user.type(within(firstRow).getByField('rating'), 'Butch{Enter}');
    await user.type(within(firstRow).getByField('fromday'), '5{Enter}');
    await user.type(within(firstRow).getByField('frommonth'), '1{Enter}');
    await user.type(within(firstRow).getByField('fromyear'), '1869{Enter}');
    await user.type(within(firstRow).getByField('today'), '7{Enter}');
    await user.type(within(firstRow).getByField('tomonth'), '2{Enter}');
    await user.type(within(firstRow).getByField('toyear'), '1874{Enter}');
    await user.click(within(serviceTable0).getByTestId('completeCheckbox'));
    await user.click(within(serviceTable0).getByTestId('clone0to1Button'));
    await user.click(within(serviceTable1).getByTestId('completeCheckbox'));
    await user.click(component.getByTestId('servicesCommitButton'));
  });
});
