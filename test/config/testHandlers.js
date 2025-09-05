import { http, HttpResponse, passthrough } from 'msw';
import { readFile } from 'fs/promises';
import { isEqual } from 'lodash';
const { diff } = require('node:util');


export const PERSON_MAP = new Map();
 

//To deal with URLs that I just want to pass through, do something like this:
//http.get(import.meta.env.VITE_API_ROOT + 'person', () => passthrough()), //NB An empty callback seems to work just as well as explicit call to passthrough(). So far as I can tell, returning (anything? an HttpResponse?) stops the request gettig sent out to the network, but otherwise it is sent and returned as if it had not been intercepted.

export const handlers = [
  http.get(import.meta.env.VITE_API_ROOT + 'person/lastpost', ({request}) => {
    return HttpResponse.text('Not found', {status: 404}); //TODO: Might want to check what message the API returns, 'Not found' is just a placeholder
  }),
  http.get(import.meta.env.VITE_API_ROOT + 'person', ({request}) => {
    const search_params = new URL(request.url).searchParams;
    if(search_params.has('personid')) {
      if(search_params.getAll('personid').length >= 1) {
        const personid = search_params.getAll('personid').at(-1);
        if(personid.match(/^\d+$/)) {
          if(PERSON_MAP.has(Number(personid))) {
            return HttpResponse.json(PERSON_MAP.get(Number(personid)));
          }
          else {
            const fnam = './test/data/responses/person_personid_' + personid + '.json';
            return readFile(fnam).then(
              (jsonStr) => HttpResponse.json(JSON.parse(jsonStr)),
              () => {
                throw Error(`No such file as ${fnam}: can create with something like curl 'https://.../person?personid=${personid}' > <gitroot>/${fnam}`);
              }
            );
          }
        }
      }
    }
    return HttpResponse.json({message: 'Internal server error'}, {status: 502});
  }),
  http.post(import.meta.env.VITE_API_ROOT + 'person', async ({request}) => {
    const data = await request.json();
    PERSON_MAP.set(data.person.person_id, data);
    return HttpResponse.text('Hello, Lambda');
  }),
  http.get(import.meta.env.VITE_API_ROOT + 'status/piece_summary', ({request}) => {
    const search_params = new URL(request.url).searchParams;
    if(search_params.has('piece_number')) {
      if(search_params.getAll('piece_number').length >= 1) {
        const piece = search_params.getAll('piece_number').at(-1); //if multiple piece numbers are given then we just get the last one
        if(piece.match(/^\d+$/)) {
          return readFile('src/testdata/responses/piece_summary_' + piece + '.json').then(
            (jsonStr) => { return HttpResponse.json(JSON.parse(jsonStr)); },
            () => {
              //Reads a file that returns what the API happens to give when passed an unknown,
              //but broadly legal, number. Fix it up so that the piece_ranges make some sort of
              //sense for the given number.
              //This could be used for testing what the interface render when it sends a bad
              //piece number to the API -- but there should not be supported situations in which
              //that happens, so perhaps we do not need to define that behaviour.
              //It also lets me test navigation in some sense, even if I have not provided test
              //data for a particular piece number.
              return readFile('src/testdata/responses/piece_summary_unknown_number.json').then(
                (jsonStr) => {
                  const json = JSON.parse(jsonStr);
                  json.piece_ranges.this_piece.start_item = (piece - 1) * json.count + 1;
                  json.piece_ranges.this_piece.end_item   =  piece      * json.count;
                  json.piece_ranges.this_piece.next_piece = piece + 1;
                  json.piece_ranges.this_piece.prev_piece = piece - 1;

                  json.piece_ranges.prev_piece.start_item = (piece - 2) * json.count + 1;
                  json.piece_ranges.prev_piece.end_item   = (piece - 1) * json.count;
                  json.piece_ranges.prev_piece.prev_piece = piece - 2;
                  json.piece_ranges.prev_piece.next_piece = piece;

                  json.piece_ranges.next_piece.start_item =  piece      * json.count + 1;
                  json.piece_ranges.next_piece.end_item   = (piece + 1) * json.count;
                  json.piece_ranges.next_piece.prev_piece = piece;
                  json.piece_ranges.next_piece.next_piece = piece + 2;
                  return HttpResponse.json(json);
                }
              );
            }
          );
        }
        //else piece_number present but not a legal piece number, or perhaps we returned an empty list
        //     Either way, can fall through to the internal server error
      }
    }//else no piece number, fall through to the internal server error

    //This is what I get from curl. Depending on server configuration, we might get a CORS error instead.
    return HttpResponse.json({message: 'Internal server error', status: 502});
  }),
];
