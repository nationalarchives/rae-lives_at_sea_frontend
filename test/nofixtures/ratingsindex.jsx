import { routeRender, screen, within } from './config/testutils';
import userEvent from '@testing-library/user-event';
import RatingsIndex from '@/ratingsindex';
import { describe, expect, it } from 'vitest';

async function getComponents() {
  const pieceBackward = await screen.findByAriaLabel('Back one piece');
  const pieceForward  = await screen.findByAriaLabel('Forward one piece');
  return {
    dropdown: await screen.findByLabelText('Piece'), //will fail if Piece cannot be found, even though it does not explicitly test anything -- means I do not have to do an explicit expect(...).toBeInTheDocument() test.
    pieceForwardArrow:  within(pieceForward).getByTestId('ArrowForwardIosIcon'),
    pieceBackwardArrow: within(pieceBackward).getByTestId('ArrowForwardIosIcon'),
    nextUnfinished: await screen.findByAriaLabel('Next un-cross-checked record'),
  }
}

//beginning of checking expected state
function expectPieceNumber(piece, dropdown) {
  expect(global.window.document.title).toBe('Ratings Progress: ADM 188/' + piece);
  expect(dropdown.value).toBe(piece);
}

//re https://testing-library.com/docs/dom-testing-library/api-async for the async functions
describe('RatingsIndex', () => {
  it('low missing piece', async () => {
    //A little testing around illegal piece number
    //This is really an undefined area, so this is very provisional

    const user = userEvent.setup();
    routeRender(<RatingsIndex/>, 'ratings/:piece', '/ratings/2');
    const { dropdown, pieceForwardArrow } = await getComponents();

    expectPieceNumber('2', dropdown);

    await user.click(pieceForwardArrow);
    expectPieceNumber('5', dropdown);
  });
  it('renders high missing piece', async() => {
    //A little testing around illegal piece number
    //This is really an undefined area, so this is very provisional
    const user = userEvent.setup();
    routeRender(<RatingsIndex/>, 'ratings/:piece', '/ratings/1100');
    const { dropdown, pieceBackwardArrow } = await getComponents();
    expectPieceNumber('1100', dropdown);

    //clicking forward takes us to the first legal piece
    await user.click(pieceBackwardArrow);
    expectPieceNumber('undefined', dropdown); //TODO: Arguably not what should happen, but does it matter?
  });
  it('renders first piece', async () => {
    const user = userEvent.setup();

    routeRender(<RatingsIndex/>, '/ratings/:piece', '/ratings/5');
    const { dropdown, pieceForwardArrow, pieceBackwardArrow } = await getComponents();

    expectPieceNumber('5', dropdown);

    await user.click(pieceForwardArrow);
    expectPieceNumber('6', dropdown);

    await user.click(pieceBackwardArrow);
    expectPieceNumber('5', dropdown);
  });
});
