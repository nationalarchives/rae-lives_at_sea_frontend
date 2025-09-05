//Overcome TypeError: RequestInit: Expected signal ("AbortSignal {}") to be an instance of AbortSignal.
//Re https://github.com/reduxjs/redux-toolkit/issues/4966#issuecomment-3115230061
//Also solves "ReferenceError: TransformStream is not defined", which I previously handled separately with solution at https://github.com/socketio/socket.io/issues/5064#issuecomment-2217149231
import fetchPolyfill, { Request as RequestPolyfill } from 'node-fetch';

Object.defineProperty(global, 'fetch', {
  // MSW will overwrite this to intercept requests
  writable: true,
  value: fetchPolyfill,
});

Object.defineProperty(global, 'Request', {
  writable: false,
  value: RequestPolyfill,
});
