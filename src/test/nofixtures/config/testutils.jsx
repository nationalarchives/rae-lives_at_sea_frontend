// following https://testing-library.com/docs/react-testing-library/setup/#custom-render and
// https://testing-library.com/docs/react-testing-library/setup/#add-custom-queries
import { render, queries, within, screen, cleanup } from '@testing-library/react';
import * as customQueries from './testqueries';
import { setupServer } from 'msw/node';
import { handlers } from './testHandlers';
import { createRoutesStub, BrowserRouter, useRouteError } from 'react-router';
import { beforeAll, afterEach, afterAll } from 'vitest';

//re https://mswjs.io/docs/faq#why-do-i-get-stale-responses-with-react-queryswrapolloetc
import { QueryCache, QueryClient, QueryClientProvider } from '@tanstack/react-query';
const queryCache = new QueryCache()
const queryClient = new QueryClient();

const server = setupServer(...handlers);

beforeAll(() => {
  server.listen();
});

afterEach(() => {
  queryCache.clear();
  server.resetHandlers();
  cleanup();
});

afterAll(() => {
  server.close();
});


// redefine queries
const allQueries = {
  ...queries,
  ...customQueries,
}
const customScreen = { ...screen, ...within(document.body, allQueries) };
const customWithin = element => within(element, allQueries);

// redefine render
function commonRender(ui, wrapper, options) {
  return render(ui, {
    queries: allQueries,
    wrapper: wrapper,
    ...options
  });
}

const customRender = (ui, options) => {
  return commonRender(
    ui,
    ({children}) => {
      return (
        <BrowserRouter> //use default application routing (e.g. when rendering App, likely to test the routing itself)
          <QueryClientProvider client={queryClient}>
            {children}
          </QueryClientProvider>
        </BrowserRouter>
      )
    },
    options,
  );
}

function routeRender(ui, route, path, options) {
  return commonRender(
    ui,
    ({children}) => {
      //use custom routing (e.g. when rendering some component)
      const Stub = createRoutesStub([
      {
        path: route,
        Component: () => (
          <QueryClientProvider client={queryClient}>
            {children}
          </QueryClientProvider>
        ),
      }]);
      return (
        <Stub initialEntries={[path]}/>
      );
    },
    options,
  );
}

// re-export with overrides
export * from '@testing-library/react'
export { customScreen as screen, customWithin as within, customRender as render, routeRender }
