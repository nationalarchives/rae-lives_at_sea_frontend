import { render, screen } from './config/testutils';
import App from '@/App';

describe('App', () => {
  it('renders App component', async () => {
    render(<App/>);

    //beginning of testing that default routing is as we expect
    expect(global.window.document.title).toBe('Ratings Progress: ADM 188/5');
    expect(global.window.location.pathname).toBe('/ratings/5');
    const dropdown = await screen.findByLabelText('Piece');
    expect(dropdown.value).toBe("5");
  });
});
