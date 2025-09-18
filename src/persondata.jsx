import { useContext } from 'react';
import { useParams } from 'react-router';
import { Stack, Card } from '@mui/material';
import { useDialogs } from '@toolpad/core/useDialogs';

import { isNew, RATING_LAYOUT, OFFICER_LAYOUT } from './data_utils';
import { failedMutationDialog } from './queries.js';
import NewPersonControlPanel from './newpersoncontrolpanel';
import ExistingPersonControlPanel from './existingpersoncontrolpanel';
import PersonTableControlPanel from './persontablecontrolpanel';
import PersonTable from './persontable';
import { LockedContext } from './lockedcontext';
import { snapshot } from './snapshot';
import { usePrefs } from './prefs';

export default function PersonData({record, audit}) {
  const dialogs = useDialogs();
  const [, setLocked] = useContext(LockedContext);
  const { sailorType, nameId } = useParams();
  const { data, setData, mutation } = record;
  const screenshot = usePrefs((state)=>state.screenshot);
  return (
    <Stack direction='row' width={0.9} alignItems='flex-start'>
      <Card variant='outlined'>
        <PersonTableControlPanel data={data} onChange={()=>{
          setLocked(true);
          snapshot('sent_name', screenshot, audit, data.person_id);
          mutation.mutate(data, {
            onError: (error, variables) => {
              failedMutationDialog(dialogs, mutation)(error, variables),
              setLocked(false);
            },
            onSuccess: ()=>{setLocked(false);},
            /* There are some confusing issues around both unmount and multiple invocations in respect to these callbacks as passed to the mutate function itself.
             * Re: https://tanstack.com/query/v5/docs/framework/react/guides/mutations#mutation-side-effects (final para of section) and
             *     https://tanstack.com/query/v5/docs/framework/react/guides/mutations#consecutive-mutations (directly beneath on the same page)
             * For unlocking, the worst case is that we fail to unlock, which is acceptable as it is the safe option.
             * For error handling, it may have to be robust to the possibiltiy that we do not report to user??
             *   Though I would hope that we can expect the component to remain mounted
             *   And I also hope that the consecutive thing does not apply here as we have only one call to 'mutate'.
             */
          });
        }}/>
        {
          <PersonTable data={data} onChange={setData} rowCells={8}
            rows={sailorType === 'officer' ?
              OFFICER_LAYOUT :
              isNew(nameId) ?
                [{labels: {'ADM': 2}, fields :{series: 1, piece: 1, nameid: 1}}, ...RATING_LAYOUT] :
                RATING_LAYOUT
            }
          />
        }
      </Card>
      {
      isNew(nameId) ?
        <NewPersonControlPanel data={data} onChange={setData}/>
      :
        <ExistingPersonControlPanel data={data} onChange={setData}/>
      }
    </Stack>
  );
}
