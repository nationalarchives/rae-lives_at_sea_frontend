import { useContext } from 'react';
import { useConfirm } from 'material-ui-confirm';
import { LoadingContext } from './loadingcontext';
import { LockedContext } from './lockedcontext';

import { DataGrid, gridClasses, useGridApiRef } from '@mui/x-data-grid';
import Stack from '@mui/material/Stack';
import Tooltip from '@mui/material/Tooltip';
import IconButton from '@mui/material/IconButton';
import InsertAboveIcon from '@mui/icons-material/Publish';
import InsertBelowIcon from '@mui/icons-material/Download';
import DeleteIcon from '@mui/icons-material/DeleteForever';
import { Button, Typography } from '@mui/material';
import { useTheme, alpha } from '@mui/material/styles';
import style from './style';

function checkPrimary(cols, primary) {
  for(const col of cols) {
    if(col.field === primary) {
      if(col.editable) throw new Error('Primary key ' + primary + ' should not be editable');
      return;
    }
  }
  if(cols.length) throw new Error('Primary key ' + primary + ' not defined in columns');
}

//Tables of rows because of the ServiceReconciler use case, where we have two rows but only want to pop up one dialog
export function useEmptyRowOK(tables_of_rows, primary) {
  const confirmDialog = useConfirm();

  function hasEmptyRow(rows, primary) {
    outer: for(const row of rows) {
      for(const [k, v] of Object.entries(row)) {
        if(k === primary) continue;
        if(v) continue outer;
      }
      return true; //this row had no values in any non-primary key
    }
    return false; //all rows had at least one "real" value
  }

  //return of true meaning "ok to save" -- either there are no empty rows, or the user has asked to save them
  return async () => {
    if(tables_of_rows.some((rows) => hasEmptyRow(rows, primary))) {
      return confirmDialog({
        description: 'This record contains empty rows. Save anyway?',
        title: 'Empty rows',
        confirmationText: 'Save',
        cancellationText: 'Cancel',
        allowClose: false,
      }).then((x) => x.confirmed);
    }
    else {
      return true;
    }
  }
}

export function emptyRow(coldefs) {
  const row = {};
  for(const coldef of coldefs) {
    if(coldef.type === 'number') row[coldef.field] = 0;
    else                         row[coldef.field] = null;
  }
  return row;
}

export function DataTable(props) {
  const {rows, columns, onChange, primary, positionalPrimary, extraRowControls, controlCount, sx, ...otherProps} = props;
  const loading = useContext(LoadingContext);
  const [locked, setLocked] = useContext(LockedContext);
  const theme = useTheme();
  const apiRef = useGridApiRef();
  function addButton() {
    return (
      <Stack alignItems='center'>
        <Typography variant='caption'>No rows</Typography>
        <Button data-testid='firstRowButton' variant='outlined' onClick={()=>{
          setLocked(true);
          onChange([{...emptyRow(columns), [primary]: 1}]);
          setLocked(false);
        }}>Add first row</Button>
      </Stack>
    );
  }

  const finalOnChange = (data) => {
    if(positionalPrimary) {
      onChange(data.sort((a,b)=>a[primary] - b[primary]));
    }
    else {
      onChange(data);
    }
  };

  checkPrimary(columns, primary);

  function baseRowControls(params) {
    const {row} = params; //const {row, ...otherParams} = params; would give access to the rest of the params if needed
    const sx = {
      px: 0.2,
      py: 0,
    }
    let insertionButtons = null;
    if(positionalPrimary) {
        function insert(pos) {
          const newRows = rows.map((e) => {
            const clone = structuredClone(e);
            if(e[primary] >= pos) clone[primary] += 1;
            return clone;
          })
          newRows.push({...emptyRow(columns), [primary]: pos});
          return newRows;
        }
        insertionButtons = (<>
          <Tooltip title='Insert row above' placement='top' followCursor arrow>
            <span>
              <IconButton sx={sx} fontSize='inherit' color='primary' data-testid='newRowAboveButton' onClick={()=>{
                setLocked(true);
                finalOnChange(insert(row[primary]));
                setLocked(false);
              }}>
                <InsertAboveIcon/>
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title='Insert row below' placement='top' followCursor arrow>
            <span>
              <IconButton sx={sx} fontSize='inherit' color='primary' data-testid='newRowBelowButton' onClick={()=>{
                setLocked(true);
                finalOnChange(insert(row[primary] + 1));
                setLocked(false);
              }}>
                <InsertBelowIcon/>
              </IconButton>
            </span>
          </Tooltip>
        </>);
    }
    return (
      <Stack direction='row' spacing={0}>
        {insertionButtons}
        <Tooltip title='Delete row' placement='top' followCursor arrow>
          <span>
            <IconButton sx={sx} fontSize='inherit' color='primary' data-testid='deleteRowButton' onClick={()=>{
              setLocked(true);
              const newRows = [];
              for(const x of rows) {
                if(x[primary] === row[primary]) continue;
                const newRow = structuredClone(x);
                if(positionalPrimary && (x[primary] > row[primary])) newRow[primary] -= 1;
                newRows.push(newRow);
              }
              finalOnChange(newRows);
              setLocked(false);
            }}>
              <DeleteIcon/>
            </IconButton>
          </span>
        </Tooltip>
        {typeof(extraRowControls) !== 'undefined' && extraRowControls(params, sx)/*TODO: This feels hairy. Either verify that this is a legit thing to do with this API (a second function looking at the parameters) or at least build in enough testing that we'll know if it breaks.*/}
      </Stack>
    );
  };

  const initialState = {};
  if(positionalPrimary) {
    initialState.sorting = {sortModel: [{field: primary, sort: 'asc'}]};
    if(otherProps.disableColumnSorting === false) throw new Error('Data table with positional primary cannot be sortable');
  }
  else {
    initialState.sorting = {sortModel: [{field: primary, sort: 'desc'}]};
    initialState.columns = {
      columnVisibilityModel: {
        [primary]: false
      },
    }
  }

  //Following https://github.com/mui/mui-x/issues/3016#issuecomment-955079280
  const handleCellKeyDown = (params, evt) => {
    if(typeof(evt) === 'undefined') return;
    if(evt.key !== 'Tab') return;

    evt.preventDefault();
    evt.defaultMuiPrevented = true;

    const visibleCols = apiRef.current.getVisibleColumns();

    let cellCol = apiRef.current.getColumnIndex(params.field);
    let cellRow = params.row.rowid;
    if(evt.shiftKey) {
      if(cellCol <= 1) { //cannot go left if we are in the primary key col OR in the first editable col
        if(cellRow === 1) { //cannot go up if we are in the top col (rowId always starts from 1)
          return; //do nothing if we have run out of table
        }
        cellCol = columns.length - 1; //skip the buttons
        cellRow -= 1;
      }
      else {
        cellCol -= 1;
      }
    }
    else {
      if(cellCol + 1 >= columns.length) { //cannot go right if we are in the button col OR in the final editable col
        if(cellRow === rows.length) {
          return; //do nothing if we have run out of table
        }
        cellCol  = 1; //skip the primary key col
        cellRow += 1; //our rows have consecutive ids
      }
      else {
        cellCol += 1;
      }
    }
    apiRef.current.setCellFocus(cellRow, visibleCols[cellCol].field);
  };

  return(
    <DataGrid
      apiRef={apiRef}
      onCellKeyDown={handleCellKeyDown}
      loading={loading || locked}
      density='compact'
      rows={rows}
      columns={[...columns, {
          field: 'row_controls',
          headerName: '',
          width: theme.typography.fontSize * 2.2 * (controlCount ? controlCount : 3),
          minWidth: theme.typography.fontSize * 2.2 * (controlCount ? controlCount : 3),
          renderCell: baseRowControls,
        },
      ]}
      slots = {{ noRowsOverlay: addButton }}
      initialState={initialState}
      getRowId={(row) => {return row[primary];}}
      processRowUpdate={(updatedRow, originalRow, {rowId}) =>{
        finalOnChange(rows.map((e) => e[primary] === rowId ? structuredClone(updatedRow) : structuredClone(e)));
        return updatedRow;
      }}
      hideFooter={!(rows)}
      onProcessRowUpdateError={(e)=>{alert(e);}}
      disableColumnSorting={positionalPrimary}
      disableColumnMenu={positionalPrimary}
      getRowHeight={()=>'auto'}
      pageSizeOptions={[100]} //only one possibility, to simplify testing (and interface)
      sx={{
        ...sx,
        [`.${gridClasses.cell}`]: {
          display: 'flex',
          alignItems: 'center',
          padding: '0px',
          pl: '3px',
          pr: '3px',
        },
        '& .MuiDataGrid-cell.error': style.errorCell,
        //N.B. The source is really helpful for finding the way through this:
        //https://github.com/mui/mui-x/blob/af50a4a9974f14afc09ce7639da4f326909c7270/packages/grid/_modules_/grid/components/containers/GridRootStyles.ts#L218-L228
        '& .MuiDataGrid-cell, .MuiDataGrid-columnHeader': { py: '3px', border: 1 },
        '& .MuiDataGrid-cell:focus': style.selectedCell, //re https://github.com/MaterialDesignInXAML/MaterialDesignInXamlToolkit/issues/1293#issuecomment-1133818615
        '& .MuiDataGrid-row.Mui-selected': { background: 'rgba(0, 0, 0, 0)'},
        '& .MuiDataGrid-row.Mui-selected:hover': { background: alpha(theme.palette.action.hover, 0.05) },
        '& .MuiDataGrid-columnSeparator': { display: 'none' },
        '& .MuiDataGrid-columnHeaderTitle': { fontWeight: 'bold' },
        '& .MuiDataGrid-editInputCell input': { padding: 0 }, /* re https://stackoverflow.com/q/72281184 */
      }}
      {...otherProps}
    />);
}
