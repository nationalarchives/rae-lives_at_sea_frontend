import { /*useEffect, */useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { DataTable } from './datatable';
import { simpleTableQuery } from './queries';
import { LoadingContext } from './loadingcontext';
import { Alert, CircularProgress, Stack } from '@mui/material';

export default function SimpleEditor({table, primary}) {
  const [rows, setRows] = useState([]);
  const [fields, setFields] = useState([]);
  const { data: queryData, status: queryStatus } = useQuery(simpleTableQuery(table));
  if(queryStatus === 'success') {
    if(queryData && queryData.length) {
      setRows(queryData);
      setFields(Object.keys(queryData[0]));
    }
    else {
      setRows([]);
      setFields([]);
    }
  }

  if(fields.length) {
    const columns = fields.map((e) => ({
      field: e,
      headerName: e[0].toUpperCase() + e.slice(1),
      editable: e !== primary,
      flex: 1 / fields.length,
    }));
    return (
      <LoadingContext value={queryStatus==='pending'}>
        <DataTable
          rows={rows}
          columns={columns}
          primary={primary}
          onChange={setRows}
        />
      </LoadingContext>
    );
  }
  else if(queryStatus === 'error') {
    return (<Alert severity='error'>Error fetching data</Alert>);
  }
  else {
    return (<Stack height='100vh' width='100vw' alignItems='center' justifyContent='center'><CircularProgress size='50vh'/></Stack>);
  } 
}
