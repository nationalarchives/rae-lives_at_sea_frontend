// @vitest-environment jsdom

import { routeRender, within } from './config/testutils';
import userEvent from '@testing-library/user-event';
import { cleanup } from '@testing-library/react';
import RatingsIndexNavigator from '@/ratingsindexnavigator';
import { describe, test as baseTest, vi } from 'vitest';

//Following tricks https://stackoverflow.com/a/72289488, http://pawelgoscicki.com/archives/2022/05/testing-usenavigate-navigate-from-react-router-v6/, https://mayashavin.com/articles/two-shades-of-mocking-vitest to spy on useParams
import * as router from 'react-router';

//TODO: Encapsulate this -- could put it in a different file and not export, for example
let paramSpy = null;
let stderrSpy = null;
let dropdownSelection = null;
let component = null;
let user = null;
let backButton = null;
let checkPiece = null;

function navTest(basePiece = 7, noStderr = false) {
  return {
    paramSpy: async ({}, use) => {
  //console.log('paramSpy');
      paramSpy = vi.spyOn(router, 'useParams');
      await use(paramSpy);
      paramSpy.mockReset();
      paramSpy = null;
    },
    stderrSpy: [
      async ({}, use) => {
        stderrSpy = vi.spyOn(console, 'error').mockImplementation(()=>{});
        await use(stderrSpy);
        stderrSpy.mockReset();
        stderrSpy = null;
      },
      {auto: noStderr},
    ],
    user: async ({}, use) => {
  //console.log('user');
      user = userEvent.setup();
      await use(user);
      user = null;
    },
    component: async({user, paramSpy}, use) => { //async { user } to establish the dep
  //console.log('component');
      component = routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', `/ratings/${basePiece}`);
      await use(component);
      cleanup();
      component = null;
    },
    backButton: async ({component}, use) => {
  //console.log('backbutton');
      backButton = await component.findByTestId('backButton');
      await use(backButton);
      backButton = null;
    },
    forwardButton: async({component}, use) => {
  //console.log('forwardbutton');
      backButton = await component.findByTestId('forwardButton');
      await use(backButton);
      backButton = null;
    },
    dropdownSelection: async ({component}, use) => {
  //console.log('dropdownSelection');
      dropdownSelection = within(await component.findByTestId('dropdown')).getByRole('combobox');
      await use(dropdownSelection);
      dropdownSelection = null;
    },
    checkPiece: async ({dropdownSelection, paramSpy, expect}, use) => {
      checkPiece = (expected) => {
        expect(dropdownSelection.value).toBe(expected);
        expect(paramSpy.mock.results.at(-1).type).toEqual('return');
        expect(paramSpy.mock.results.at(-1).value.piece).toBe(expected);
      };
      await use(checkPiece);
      checkPiece = null;
    },
  };
}

const it = baseTest.extend(navTest());
//baseTest.extend(navTest(8))('render 8', async ({paramSpy, dropdownSelection}) => { checkPiece('8'); });

//function checkPiece(expected) {
  //expect(dropdownSelection.value).toBe(expected);
  //expect(paramSpy.mock.results.at(-1).type).toEqual('return');
  //expect(paramSpy.mock.results.at(-1).value.piece).toBe(expected);
//}
function testFromTo(fromPiece, toPiece, increment) {
  if(increment === 0) throw Error();

  return () => {
    it('button', async ({forwardButton, backButton, user, checkPiece}) => { //tests navigation via buttons
      checkPiece('' + fromPiece);
      const button = increment > 0 ? forwardButton : backButton;
      const promises = [];
      for(let i = 0; i < Math.abs(increment); i++) promises.push(user.click(button));
      await(Promise.all(promises));
      checkPiece('' + toPiece); //awaiting the click seems to be enough for synchronization. Alternative if it ever fails:
                                //await(waitFor(()=>expect(dropdownSelection.value).toBe('6')));
    });
    it('keyboard', async ({dropdownSelection, user, checkPiece}) => { //tests navigation via combo
      checkPiece('' + fromPiece);
      await user.type(dropdownSelection, (fromPiece + increment) + '{Enter}');
      checkPiece('' + toPiece);
    });
    it('list', async ({dropdownSelection, user, checkPiece}) => { //Perhaps don't need this one, the above keyboard case already tests the combo
      checkPiece('' + fromPiece);
      await user.type(dropdownSelection, '{ArrowDown}'.repeat(fromPiece + increment - 5) + '{Enter}');
      checkPiece('' + toPiece);
    });
  };
}

describe('RatingsIndexNavigator', () => {
  describe('happy path', () => {
    it('renders', async ({checkPiece}) => {
      checkPiece('7');
    });
    describe('back one',    testFromTo(7,  6, -1));
    describe('forward one', testFromTo(7,  8,  1));
    describe('leap',        testFromTo(7, 27, 20));
    it('back and forth', async ({backButton, forwardButton, checkPiece}) => {
      checkPiece('7');
      await user.click(backButton);
      checkPiece('6');
      await user.click(forwardButton);
      checkPiece('7');
    });
    it('forth and back', async ({backButton, forwardButton, checkPiece, user}) => {
      checkPiece('7');
      await user.click(forwardButton);
      checkPiece('8');
      await user.click(backButton);
      checkPiece('7');
    });
    it('tooltips', async ({user, backButton, forwardButton, component, expect}) => {
       //seems tricky to test that the tooltips are not there when they should not be, but it is minor functionality, this will do
      await user.hover(backButton);
      expect(await component.findByText('Back one piece'));
      await user.hover(forwardButton);
      expect(await component.findByText('Forward one piece'));
      const forwardTip = await component.getByText('Forward one piece');
    });
  });
  describe('error path', () => {
    describe('invalid piece`', () => {
      baseTest.extend(navTest('fake', true))('string', async ({component, expect, stderrSpy}) => {
        expect(await component.findByText(/^No such piece: fake$/));
      });
      baseTest.extend(navTest('0', true))('zero', async ({component, expect}) => {
        expect(await component.findByText(/^No such piece: 0$/));
      });
      baseTest.extend(navTest('-1', true))('negative', async ({component, expect}) => {
        expect(await component.findByText(/^No such piece: -1$/));
      });
      baseTest.extend(navTest('3.141', true))('noninteger', async ({component, expect}) => {
        expect(await component.findByText(/^No such piece: 3.141$/));
      });
      baseTest.extend(navTest('/three/3', true))('path', async ({component, expect}) => {
        expect(await component.findByText(/^404 Not Found$/));
      });
      baseTest.extend(navTest('/three/point/one/four/1', true))('deep path', async ({component, expect}) => {
        expect(await component.findByText(/^404 Not Found$/));
      });
    });
  });
});
