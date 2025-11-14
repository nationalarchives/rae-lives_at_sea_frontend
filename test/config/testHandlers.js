import { http, HttpResponse } from 'msw';
import { readFile } from 'fs/promises';

const TRACE = false;

export const PERSON_MAP = new Map();

export function getParam(url, param) {
  const searchParams = new URL(url).searchParams;
  if(searchParams.has(param)) {
    if(searchParams.getAll(param).length >= 1) {
      return searchParams.getAll(param).at(-1); //if multiple are given then we just get the last one
    }
  }
  return null;
}

export function getNumericParam(url, param) {
  const text = getParam(url, param);
  if(text.match(/^\d+$/)) return Number(text);
  else                    return null;
}

export const handlers = [
  http.get(import.meta.env.VITE_API_ROOT + 'person/lastpost', ({request}) => {
    if(TRACE) console.log('GET /person/lastpost');
    return HttpResponse.json({'L@SRecordsReturned': 0}, {status: 404});
  }),
  http.get(import.meta.env.VITE_API_ROOT + 'person', ({request}) => {
    const search_params = new URL(request.url).searchParams;
    if(search_params.has('personid')) {
      if(search_params.getAll('personid').length >= 1) {
        const personid = search_params.getAll('personid').at(-1);
        if(TRACE) console.log('GET /person', personid);
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
    if(TRACE) console.log('POST /person');
    const data = await request.json();
    PERSON_MAP.set(data.person.person_id, data);
    return HttpResponse.text('Hello, Lambda');
  }),
  http.get(import.meta.env.VITE_API_ROOT + 'status/piece_summary/last_piecesummary', ({request}) => {
    if(TRACE) console.log('GET /status/piece_summary/last_piecesummary');
    return HttpResponse.text('Not found', {status: 404}); //TODO: Might want to check what message the API returns, 'Not found' is just a placeholder
  }),
  http.get(import.meta.env.VITE_API_ROOT + 'status/piece_summary', ({request}) => {
    const search_params = new URL(request.url).searchParams;
    if(search_params.has('piece_number')) {
      if(search_params.getAll('piece_number').length >= 1) {
        const pieceStr = search_params.getAll('piece_number').at(-1); //if multiple piece numbers are given then we just get the last one
        if(TRACE) console.log('GET /status/piece_summary', pieceStr);
        if(pieceStr.match(/^\d+$/)) {
          const piece = Number(pieceStr);
          return new Promise((resolve, reject) => {
            readFile('test/data/responses/piece_summary_' + piece + '.json').then(
              (jsonStr) => { resolve(HttpResponse.json(JSON.parse(jsonStr))) },
              () => {
                //Reads a file that returns what the API happens to give when passed an unknown,
                //but broadly legal, number. Fix it up so that the piece_ranges make some sort of
                //sense for the given number.
                //This could be used for testing what the interface render when it sends a bad
                //piece number to the API -- but there should not be supported situations in which
                //that happens, so perhaps we do not need to define that behaviour.
                //It also lets me test navigation in some sense, even if I have not provided test
                //data for a particular piece number.
                const reader = async() => {
                  try {
                    const jsonStr = await readFile('test/data/responses/piece_summary_unknown_number.json');
                    const json = JSON.parse(jsonStr);
                    json.piece_ranges.this_piece.start_item = (piece - 1) * json.count + 1;
                    json.piece_ranges.this_piece.end_item   =  piece      * json.count;
                    json.piece_ranges.this_piece.next_piece =  piece + 1;
                    json.piece_ranges.this_piece.prev_piece =  piece - 1;

                    json.piece_ranges.prev_piece = {
                      start_item: (piece - 2) * json.count + 1,
                      end_item:   (piece - 1) * json.count,
                      prev_piece:  piece - 2,
                      next_piece:  piece,
                    };

                    json.piece_ranges.next_piece.start_item =  piece      * json.count + 1;
                    json.piece_ranges.next_piece.end_item   = (piece + 1) * json.count;
                    json.piece_ranges.next_piece.prev_piece =  piece;
                    json.piece_ranges.next_piece.next_piece =  piece + 2;

                    let current_item = json.piece_ranges.this_piece.start_item;
                    for(const record of json.records) {
                      record.gen_item = current_item;
                      record.source_reference = `ADM^188^${piece}^${current_item}`;
                      current_item += 1;
                    }
                    return HttpResponse.json(json);
                  }
                  catch(err) {
                    if(TRACE) console.log('rejecting unknown piece file', err);
                    return HttpResponse.json({message: 'Internal server error'}, {status: 502});
                  }
                };
                resolve(reader());
              }
            ); //end (first, hopefully known number) readFile
          }); //end Promise
        } //if pieceStr is a number
      } //search params for 'piece_number' is empty list
    }//else no piece number, fall through to the internal server error

    //This is what I get from curl. Depending on server configuration, we might get a CORS error instead.
    return HttpResponse.json({message: 'Internal server error'}, {status: 502});
  }),
  http.post(import.meta.env.VITE_API_ROOT + 'status/piece_summary/last_piecesummary', async ({request}) => {
    const pid = getNumericParam(request.url, 'piece_number');
    if(TRACE) console.log('POST /status/piece_summary/last_piecesummary', pid);
    const data = request.json();
    return HttpResponse.text('Hello, Lambda');
  }),
];
