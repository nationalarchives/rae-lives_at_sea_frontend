// @vitest-environment jsdom

import { routeRender, screen, within } from './config/testutils';
import userEvent from '@testing-library/user-event';
import RatingsIndexNavigator from '@/ratingsindexnavigator';
import { beforeAll, afterAll, beforeEach, describe, expect, it, vi } from 'vitest';

//Following tricks https://stackoverflow.com/a/72289488, http://pawelgoscicki.com/archives/2022/05/testing-usenavigate-navigate-from-react-router-v6/, https://mayashavin.com/articles/two-shades-of-mocking-vitest to spy on useParams
import * as router from 'react-router';

async function getComponents() {
  //screen appears to be quite global
  //These will fail if things cannot be found
  const dropdown = await screen.findByTestId('dropdown');
  return {
    dropdown:          dropdown,
    dropdownSelection: within(dropdown).getByRole('combobox'),
    forwardButton:     await screen.findByTestId('forwardButton'),
    backButton:        await screen.findByTestId('backButton'),
  }
}

async function checkPiece(ddSelection, pSpy, expected) {
  //screen appears to be quite global
  expect(ddSelection.value).toBe(expected);
  expect(pSpy.mock.results.at(-1).type).toEqual('return');
  expect(pSpy.mock.results.at(-1).value.piece).toBe(expected);
}

//re https://testing-library.com/docs/dom-testing-library/api-async for the async functions
describe('RatingsIndexNavigator', () => {
  let paramSpy;
  beforeAll(() => {
    paramSpy = vi.spyOn(router, 'useParams');
  });
  afterAll(() => {
    paramSpy.mockReset();
  });

  //fromPiece + increment may not equal toPiece as there can be gaps in the series
  function testFromTo(fromPiece, toPiece, increment) {
    if(increment === 0) throw Error();

    return () => {
      let dropdownSelection, forwardButton, backButton;
      beforeEach(async () => {
        const c = await getComponents();
        dropdownSelection = c.dropdownSelection;
        forwardButton = c.forwardButton;
        backButton = c.backButton;
        checkPiece(dropdownSelection, paramSpy, '' + fromPiece);
      });
      it('button', async () => { //tests navigation via buttons
        const button = increment > 0 ? forwardButton : backButton;
        const promises = [];
        for(let i = 0; i < Math.abs(increment); i++) promises.push(user.click(button));
        await(Promise.all(promises));
        checkPiece(dropdownSelection, paramSpy, '' + toPiece); //awaiting the click seems to be enough for synchronization. Alternative if it ever fails:
                                  //await(waitFor(()=>expect(dropdownSelection.value).toBe('6')));
      });
      it('keyboard', async () => { //tests navigation via combo
        await user.type(dropdownSelection, (fromPiece + increment) + '{Enter}');
        checkPiece(dropdownSelection, paramSpy, '' + toPiece);
      });
      it('list', async () => { //Perhaps don't need this one, the above keyboard case already tests the combo
        await user.type(dropdownSelection, '{ArrowDown}'.repeat(fromPiece + increment - 5) + '{Enter}');
        checkPiece(dropdownSelection, paramSpy, '' + toPiece);
      });
    };
  }

  describe('happy path', () => {
    let user;
    beforeEach(async ()=>{
      user = userEvent.setup();
      routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/7');
    });
    it('renders', async () => {
      checkPiece(dropdownSelection, paramSpy, '7'); //redundant with pre-conditions of below tests, but keeps units self-contained
    });
    describe('back one',    testFromTo(7,  6, -1));
    describe('forward one', testFromTo(7,  8,  1));
    describe('leap',        testFromTo(7, 27, 20)); //testing the buttons doesn't make sense for a leap, but is harmless and easy
    it('back and forth', async () => { //clicking back then forward takes us back to where we started
      await user.click(backButton);
      checkPiece(dropdownSelection, paramSpy, '6');
      await user.click(forwardButton);
      checkPiece(dropdownSelection, paramSpy, '7');
    });
    it('forth and back', async () => { //clicking forward then back takes us back to where we started
      await user.click(forwardButton);
      checkPiece(dropdownSelection, paramSpy, '8');
      await user.click(backButton);
      checkPiece(dropdownSelection, paramSpy, '7');
    });
    it('tooltips', async () => { //seems tricky to test that the tooltips are not there when they should not be, but it is minor functionality, this will do
      await user.hover(backButton);
      expect(await screen.findByText('Back one piece'));
      await user.hover(forwardButton);
      expect(await screen.findByText('Forward one piece'));
      const forwardTip = await screen.getByText('Forward one piece');
    });
  });
  describe('error path', () => {
    let stderrSpy;
    beforeAll(() => {
      stderrSpy = vi.spyOn(console, 'error').mockImplementation(()=>{}); //just to stop the console filling up with garbage
    });
    afterAll(() => {
      stderrSpy.mockReset();
    });
    describe('invalid piece`', () => {
      it('string', async () => {
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/fake');
        expect(await screen.findByText(/^No such piece: fake$/));
      });
      it('zero', async () => {
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/0');
        expect(await screen.findByText(/^No such piece: 0$/));
      });
      it('negative', async () => {
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/-1');
        expect(await screen.findByText(/^No such piece: -1$/));
      });
      it('noninteger', async () => {
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/3.141');
        expect(await screen.findByText(/^No such piece: 3.141$/));
      });
      it('path', async () => {
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/three/3');
        expect(await screen.findByText(/^404 Not Found$/));
      });
      it('deep path', async () => {
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/three/point/one/four/1');
        expect(await screen.findByText(/^404 Not Found$/));
      });
    });
    describe('before first piece', () => {
      it('do not render positive integer below minimum', async () => {
        //These really should be waiting to see the message in an exception
        //But I have spent several hours failing to expect an exception out of this thing
        //Perhaps in part the problem is that this component re-renders a couple of times as the queryStatus changes
        //So catching the text from the ErrorBoundary will have to do
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/2');
        expect(await screen.findByText(/^No such piece: 2$/));
      });
      it('do not select positive integer below minimum', async () => {
        const user = userEvent.setup();
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/5');
        const { dropdownSelection } = await getComponents();
        //console.log(dropdownSelection.value);
        await user.type(dropdownSelection, '2{Enter}');
        console.log('***');
        _checkPiece(paramSpy, dropdownSelection, '12'); //restricted to values available in the dropdown
        __checkPiece('12'); //restricted to values available in the dropdown
        console.log('^^^');
      });
      it('stop at minimum', async () => {
        const user = userEvent.setup();
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/5');
        const { dropdownSelection, backButton } = await getComponents();
        checkPiece(dropdownSelection, paramSpy, '5');
        expect(backButton).toBeDisabled();
      });
    });
    describe('after final piece', () => {
      it('do not render positive integer above maximum', async () => {
        //These really should be waiting to see the message in an exception
        //But I have spent several hours failing to expect an exception out of this thing
        //Perhaps in part the problem is that this component re-renders a couple of times as the queryStatus changes
        //So catching the text from the ErrorBoundary will have to do
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/1100');
        expect(await screen.findByText(/^No such piece: 1100$/));
      });
      it('do not select positive integer above maximum', async () => {
        this.user = userEvent.setup();
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/5');
        const { dropdownSelection } = await getComponents();
        await user.type(dropdownSelection, '1095{Enter}');
        expect(dropdownSelection.value).toBe('1095'); //restricted to values available in the dropdown
        expect(dropdown).toBeDisabled();
      });
      it('stop at maximum', async () => {
        this.user = userEvent.setup();
        routeRender(<RatingsIndexNavigator/>, 'ratings/:piece', '/ratings/1094');
        const { dropdownSelection, forwardButton } = await getComponents();
        expect(dropdownSelection.value).toBe('1094');
        expect(forwardButton).toBeDisabled();
      });
    });
  });
});
  //TODO: Skip missing
