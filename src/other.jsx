import { useContext } from 'react';
import { useParams } from 'react-router';
import { failedMutationDialog } from './queries';
import { Alert, Button, Stack } from '@mui/material';
import { useConfirm } from 'material-ui-confirm';
import { LoadingContext } from './loadingcontext';
import { LockedContext } from './lockedcontext';
import { useEmptyRowOK , DataTable } from './datatable';
import { DirtySailorContext } from './dirty';

export default function Other({tag, columns, columnGroupingModel, record}) {
  const { sailorType, nameId } = useParams();
  const { data, setData, mutation, status: queryStatus } = record;
  const dirty = useContext(DirtySailorContext)[tag];
  const loading = useContext(LoadingContext);
  const [locked, setLocked] = useContext(LockedContext);
  const emptyOK = useEmptyRowOK([data], 'row');
  const confirmDialog = useConfirm();

  if(queryStatus === 'error') {
    return (<Alert severity='error'>Error fetching data</Alert>);
  }
  else {
    return(
      <LoadingContext value={queryStatus === 'pending' || mutation.status === 'pending'}>
        <Stack width='90vw' justifyContent='space-between' spacing={2} sx={{padding: 2}}>
          <Stack direction='row' justifyContent='flex-end'>
            <Button variant='outlined' disabled={(!dirty) || loading || locked} onClick={
              async ()=>{
                if(await emptyOK()) {
                  setLocked(true);
                  mutation.mutate(data, {
                    onError: (error, variables) => {
                      failedMutationDialog(confirmDialog, mutation)(error, variables);
                      setLocked(false);
                    },
                    onSuccess: ()=>{setLocked(false)}, //see similar code in persondata.jsx for concerns around use of these callbacks
                  });
                  //It looks like it is possible for the user to mess about with entering extra data
                  //between the emptyOK operation and the mutate. More generally, it looks like it is possible
                  //for a user to enter data between click and mutate, given some delay between click and
                  //mutate. HOWEVER observation shows that the event appears to occur within the context of
                  //the state at the point that the event began: even if the user edits the data between click
                  //and mutate, those edits are simply ignored. Which really is the behaviour that we want for
                  //any sensible kind of atomicity. While not a great user experience it is in practice
                  //nearly impossible for the user to do this anyway.
                }
              }
            }>Enter</Button>
          </Stack>
          <DataTable
            rows={data}
            columns={columns}
            columnGroupingModel={columnGroupingModel}
            primary='row'
            positionalPrimary
            onChange={setData}
          />
        </Stack>
      </LoadingContext>
    );
  }
}
