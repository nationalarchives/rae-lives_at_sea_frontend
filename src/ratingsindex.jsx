import { useEffect } from 'react';
import { useParams, useSearchParams, Link, useNavigate } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { DataGrid } from '@mui/x-data-grid';
import { Alert, Card, CardContent, Stack, Typography, Tooltip, IconButton } from '@mui/material';
import { ElectricBolt } from '@mui/icons-material';
import { pieceQuery } from './queries';
import RatingsIndexNavigator from './ratingsindexnavigator';

const NOT_WW1     =  1;
const ALLOCATED_1 =  2;
const ALLOCATED_2 =  4;
const COMPLETED_1 =  8;
const COMPLETED_2 = 16;
const XCHECKED    = 32;
const MISSING     = 64;

const SQUARE_SIZE = 30;
const SQUARE_GAP = 0.2;

function color(set, state) {
  if(state & XCHECKED)    return 'lightgreen';
  if(state & MISSING) return 'red';
  if(set === 1) {
    if(state & COMPLETED_1) return 'yellow';
    if(state & ALLOCATED_1) return 'pink';
    return 'white';
  }
  if(set === 2) {
    if(state & COMPLETED_2) return 'yellow';
    if(state & ALLOCATED_2) return 'pink';
    return 'white';
  }
  return 'white';
}

function triangle1(pos, state) {
  return(
    <polygon points={`${pos * SQUARE_SIZE},0 ${pos * SQUARE_SIZE + SQUARE_SIZE},0 ${pos * SQUARE_SIZE},${SQUARE_SIZE}`}
             fill={color(1, state)} stroke={color(1, state) === color(2, state) ? color(1, state) : 'black'}/>
  );
}

function triangle2(pos, state) {
  return(
    <polygon points={`${pos * SQUARE_SIZE + SQUARE_SIZE},0 ${pos * SQUARE_SIZE + SQUARE_SIZE},${SQUARE_SIZE} ${pos * SQUARE_SIZE},${SQUARE_SIZE}`}
             fill={color(2, state)} stroke={color(1, state) === color(2, state) ? color(1, state) : 'black'}/>
  );
}

function one(pos) {
  return(
    <text x={pos * SQUARE_SIZE + 0.5 * SQUARE_SIZE} y={2 + 0.5 * SQUARE_SIZE} textAnchor='middle' dominantBaseline='middle' textLength={0.8 * SQUARE_SIZE} lengthAdjust='spacingAndGlyphs' fontWeight='bold' fontFamily='serif'>WWI</text>
  );
 /* //An alternative representation
  return(
    <text x={pos * SQUARE_SIZE + 0.5 * SQUARE_SIZE} y={2 + 0.5 * SQUARE_SIZE} text-anchor='middle' dominant-baseline='middle' textLength={0.4 * SQUARE_SIZE} lengthAdjust='spacingAndGlyphs' font-weight='bold' font-family='serif' stroke='black'>I</text>
  );
  }
  */
}

function dot(pos) {
  return(
    <circle cx={pos * SQUARE_SIZE + 0.5 * SQUARE_SIZE} cy={0.5 * SQUARE_SIZE} r={0.2 * SQUARE_SIZE}/>
  );
}

function box(pos) {
  return (
    <rect x={pos * SQUARE_SIZE}
          y={0}
          width={SQUARE_SIZE}
          height={SQUARE_SIZE}
          stroke='black'
          fill='none'
    />
  );
}

function key() {
  function keyItem(thing, label) {
    return(
      <Stack direction='row' spacing={1} alignItems='center'>
        <svg width={SQUARE_SIZE} height={SQUARE_SIZE + 1}>
          {thing}
        </svg>
        <Typography>{label}</Typography>
      </Stack>
    );
  }

  function squareThing(state1, state2) {
    return(
      <>
        {triangle1(0, state1 | state2)}
        {triangle2(0, state1 | state2)}
        {box(0)}
      </>
    );
  }

  //an invisible triangle will do as a spacer
  const spacer = keyItem(triangle1(0,0),'');

  const ww1 = (
    <>
      {squareThing(XCHECKED, XCHECKED)}
      {one(0)}
    </>
  );

  return(
    <Stack direction='row' spacing={6}>
      <Stack spacing={1}>
        {keyItem(squareThing(0, 0), 'Neither allocated')}
        {keyItem(squareThing(ALLOCATED_1, ALLOCATED_2), 'Both allocated')}
        {keyItem(squareThing(COMPLETED_1, COMPLETED_2), 'Both completed')}
        {spacer}
        {keyItem(squareThing(XCHECKED, XCHECKED), 'Cross-checked')}
        {spacer}
        {keyItem(ww1, 'WW1')}
      </Stack>
      <Stack spacing={1}>
        {keyItem(triangle1(0, COMPLETED_2), '#1 not allocated')}
        {keyItem(triangle1(0, ALLOCATED_1), '#1 allocated')}
        {keyItem(triangle1(0, COMPLETED_1), '#1 completed')}
        {spacer}
        {keyItem(squareThing(MISSING, MISSING), 'Mising/not used')}
        {spacer}
        {keyItem(dot(0), 'Not WW1')}
      </Stack>
      <Stack spacing={1}>
        {keyItem(triangle2(0, COMPLETED_1), '#2 not allocated')}
        {keyItem(triangle2(0, ALLOCATED_2), '#2 allocated')}
        {keyItem(triangle2(0, COMPLETED_2), '#2 completed')}
      </Stack>
    </Stack>
  );
}

function rowWidth(rowBoxes) {
  const gapSum = Math.max(0, SQUARE_SIZE * SQUARE_GAP * ((rowBoxes / 5 + rowBoxes / 10)));
  const squareSum = SQUARE_SIZE * (rowBoxes + 1);
  return gapSum + squareSum + 1; // +1 to cover possibility of rounding errors
}

//TODO: The Tooltip and Link returned here end up inside an <svg> tag -- is that OK?
function statusRow({value: items}) {
  const boxes = [];
  let offset = -1 - SQUARE_GAP;
  for(let i = 0; i < items.length; i++) {
    const state = items[i].state;
    const identifier = items[i].officialnumber === null || (('' + items[i].item) === ('' + items[i].officialnumber))
                       ? items[i].item
                       : `${items[i].item} (${items[i].officialnumber})`;
    offset += 1;
    if(i % 5 === 0) offset += SQUARE_GAP;
    if(i %10 === 0) offset += SQUARE_GAP; //larger gap every 10 cells
    boxes.push(
      <Tooltip key={'tt_' + identifier} title={identifier}>
        <Link to={(state & MISSING) ? '#' : '/person/rating/' + items[i].nameid}>
          {triangle1(offset, state)}
          {triangle2(offset, state)}
          {((state & XCHECKED) || ((state & COMPLETED_1) && (state & COMPLETED_2))) && ((state & NOT_WW1) ? dot(offset) : one(offset))}
          {box(offset)}
        </Link>
      </Tooltip>
    );
  }

  return(
    <Stack direction='row' spacing={0}><svg style={{height: SQUARE_SIZE, width: rowWidth(items.length - 1)}}>{boxes}</svg></Stack>
  );
}

function chunk(data, rowBoxes) {
  const { items, ranges } = data;
  const output = [];
  for(let i = 0; i < items.length; i += rowBoxes) {
    const chunk = items.slice(i, i + rowBoxes);
    output.push({
      range: chunk[0].item + ' - ' + chunk[chunk.length - 1].item,
      items: chunk.map((x) => ({
        nameid: x.nameid,
        state: x.state,
        officialnumber: x.officialnumber,
        item: x.item,
      })),
    });
  }

  // Handle the case where the next piece has items that start more than one nameId after this one ends
  // In this case, we append missing items to the current piece.
  // TODO: Check this one with Bruno
  //       But, does it ever come up in practice?
  //       We know it happens between pieces 88 and 89, but they are not actually in the database (but,
  //       this code will come into play if we force the piece to load by putting 88 into the URL)
  //const thisEnd = ranges.this_piece.end_item;
  const nextStart = ranges.next_piece.start_item;
  const lastOutput = output[output.length - 1];

  //extend the last row that we already have, if necessary
  const lastOutputStart = lastOutput.items[0].item;
  const lastOutputEnd = lastOutput.items.at(-1).item;
  if(lastOutputEnd < nextStart - 1) {
    lastOutput.range = lastOutputStart + ' - ' + (lastOutputStart + rowBoxes - 1);
    for(let i = lastOutputEnd + 1; i < lastOutputStart + rowBoxes; i++) {
      lastOutput.items.push({
        nameid: null,
        state: MISSING,
        officialnumber: null,
        item: i,
      })
    }
  }

  //add as many new rows as we need to end up with an item numbered one below the lowest item of the next piece
  for(let i = lastOutputStart + rowBoxes; i < nextStart; i += rowBoxes) {
    //const start = i;
    const end = Math.min(i + rowBoxes - 1, ranges.next_piece.start_item - 1);
    const newOutput = {
      range: i + ' - ' + end,
      items: []
    };
    for(let j = i; j <= end; j++) {
      newOutput.items.push({
        nameid: null,
        state: MISSING,
        officialnumber: null,
        item: j,
      });
    }
    output.push(newOutput);
  }

  return output;
}

export default function RatingsIndex() {
  const { piece } = useParams();
  const navigate = useNavigate();
  const [ searchParams, ] = useSearchParams({rowBoxes: 20});
  const { data: queryData, status: queryStatus } = useQuery({...pieceQuery(piece), select: (x) => ({
    ranges: x.piece_ranges,
    items: x.records.map((record) => {
      let state = 0;
      if(record.source_reference === null) {
        state = 64;
      }
      else {
        if(record.notww1)     state |=  1;
        if(record.tr1)        state |=  2;
        if(record.tr2)        state |=  4;
        if(record.complete1)  state |=  8;
        if(record.complete2)  state |= 16;
        if(record.reconciled) state |= 32;
      }
      return {
        nameid: record.person_id, //for links (or navigation, anyway)
        state: state, //see just above
        officialnumber: record.officialnumber, //for tooltips, at least
        item: record.gen_item, //for ui -- everything else is Discovery terms
      };
    }),
  })});

  useEffect(()=>{
    document.title = 'Ratings Progress: ADM 188/' + piece;
  }, [piece]);

  if(queryStatus === 'error') {
    return(<Alert severity='error'>Error fetching data</Alert>);
  }

  const unreconciled = [];
  const chunks = queryStatus === 'success' ? chunk(queryData, Number(searchParams.get('rowBoxes'))) : [];
  if(queryStatus === 'success') {
    for(const state of queryData.items) {
      if((state.state & (XCHECKED|MISSING)) === 0) {
        unreconciled.push(state.nameid);
      }
    }
  }

  const columns = [
    {
      field: 'range',
      headerName: 'Item',
      width: 180,
      align: 'right',
    },
    {
      field: 'items',
      headerName: 'State',
      width: rowWidth(Number(searchParams.get('rowBoxes'))),
      align: 'left',
      renderCell: statusRow,
    },
  ];
  const PAGE_ROWS = 100;

  return (
    <Card>
      <CardContent>
        {/* outermost stack: progress view on left, key on right */}
        <Stack direction='row' spacing={8} justifyContent='space-between'>
          {/* progress view */}
          <Stack spacing={0}>
            {/* progress view header (catref, width controls) */}
            <Stack direction='row' alignItems='center' justifyContent='space-between'>
              {/* catref control */}
              <RatingsIndexNavigator/>
              {/* Right-side controls */}
              <Stack direction='row' spacing={2}>
                <Tooltip title='Next un-cross-checked record'>
                  {/* div needed for tooltip to work when button is disabled */}
                  <div>
                    <IconButton color='primary'
                                disabled={unreconciled.length === 0}
                                onClick={()=>navigate('/person/rating/' + unreconciled[0])}
                    >
                      <ElectricBolt/>
                    </IconButton>
                  </div>
                </Tooltip>
              </Stack>
            </Stack>
            <DataGrid
              loading={queryStatus !== 'success'}
              density='compact'
              sx={{
                width: columns.reduce((acc, cur)=>(acc += cur.width), 1 + SQUARE_SIZE/10 /*needs this small increment to avoid horizontal scrollbar*/),
                '& .MuiDataGrid-cell': {border: 'none', height: SQUARE_SIZE, fontSize: 18, outline: 'transparent'},
                '& .MuiDataGrid-cell:focus-within': { outline: 'none' },
                '& .MuiDataGrid-columnHeader': { outline: 'transparent' },
                '& .MuiDataGrid-columnHeader:focus-within': { outline: 'none' },
                '& .MuiDataGrid-row:hover': { backgroundColor: 'inherit' },
              }}
              getRowId={(row) => {return row.range}}
              rows={chunks}
              columns={columns}
              disableColumnSorting
              disableColumnMenu
              disableRowSelectionOnClick
              isRowSelectable={()=>false}
              pageSizeOptions={[PAGE_ROWS]}
              getRowHeight={()=>(SQUARE_SIZE)}
              hideFooter={chunks.length <= PAGE_ROWS}
              localeText={{
                MuiTablePagination: {
                  labelDisplayedRows: ({ count, page }) => {
                    const highPage = Math.ceil(count / 100);
                    if(highPage) return `Page ${highPage ? page + 1 : 0}/${highPage}`;
                    else return '';
                  }
                },
              }}
            />
          </Stack>
          {/* this stack covers the whole right-hand column */}
          <Stack spacing={2}>
            {/* this stack appears just at the top of right-hand column */}
            <Stack spacing={2} alignItems='flex-start' justifyContent='flex-start' sx={{border: 1, px: 2, py: 1}}>
              <Typography><b>Key</b></Typography>
              {key()}
            </Stack>
          </Stack>
        </Stack>
      </CardContent>
    </Card>
  );
}
