// following https://testing-library.com/docs/react-testing-library/setup/#custom-render and
// https://testing-library.com/docs/react-testing-library/setup/#add-custom-queries
import { render, queries, within, screen, cleanup } from '@testing-library/react';
import * as customQueries from './testqueries';
import { setupServer } from 'msw/node';
import { PERSON_MAP, handlers } from './testHandlers';
import { createRoutesStub, BrowserRouter, useRouteError } from 'react-router';
import { beforeAll, afterEach, afterAll, vi } from 'vitest';
import { DialogsProvider } from '@toolpad/core/useDialogs'; //FIXME: Work out what to do if I am wrapping App. Perhaps just move the provider to index.jsx.
import { writeFile } from 'fs/promises';
import { RECORDS } from '@/queries';

//re https://mswjs.io/docs/faq#why-do-i-get-stale-responses-with-react-queryswrapolloetc
import { QueryCache, QueryClient, QueryClientProvider } from '@tanstack/react-query';
const queryClient = new QueryClient();
const queryCache = queryClient.getQueryCache();

const server = setupServer(...handlers);

beforeAll(() => {
  server.listen({
    onUnhandledRequest: 'error',
  });
});

afterEach(() => {
  queryCache.clear();
  server.resetHandlers();
  PERSON_MAP.clear(); //clear server's internal cache of data changes
  RECORDS.clear();  //clear app's local cache of data changes
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
    wrapper: wrapper, /*({children}) => {
      <DialogsProvider>
        {wrapper(children)}
      </DialogsProvider>
    },*/
    ...options
  });
}

const customRender = (ui, options) => {
  return commonRender(
    ui,
    ({children}) => {
      return (
        <DialogsProvider>
          <BrowserRouter> //use default application routing (e.g. when rendering App, likely to test the routing itself)
            <QueryClientProvider client={queryClient}>
              {children}
            </QueryClientProvider>
          </BrowserRouter>
        </DialogsProvider>
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
        <DialogsProvider>
          <Stub initialEntries={[path]}/>
        </DialogsProvider>
      );
    },
    options,
  );
}

async function dump(fnam, stuff) {
  try {
    await writeFile(fnam, stuff);
  }
  catch(e) {
    console.error(`Failed to write ${fnam}: ${e}`);
    throw e;
  }
//  console.log('wrote', stuff);
}
  
// re-export with overrides
export * from '@testing-library/react'
export { customScreen as screen, customWithin as within, customRender as render, routeRender, server as mockServer, dump }
