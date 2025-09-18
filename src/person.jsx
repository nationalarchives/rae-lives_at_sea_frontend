import { useState, useEffect } from 'react';
import { useParams, useLocation, useNavigate } from 'react-router';
import Card from '@mui/material/Card';
import Tab from '@mui/material/Tab';
import Tabs from '@mui/material/Tabs';
import Alert from '@mui/material/Alert';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import CircularProgress from '@mui/material/CircularProgress';
import OtherData from './otherdata';
import OtherServices from './otherservices';
import PersonData from './persondata';
import ServiceReconciler from './servicereconciler';
import { LoadingContext } from './loadingcontext';
import { LockedContext } from './lockedcontext';
import { DirtySailorContext, useDirtySailor, useDirtySailorBlocker } from './dirty';
import BlockNavigationDialog from './blocknavigationdialog';
import { isNew, catref, officerref } from './data_utils';
import { useRecord } from './queries';

export default function Person() {
  const { sailorType, nameId, dataType } = useParams();
  const { pathname } = useLocation();
  const navigate = useNavigate();
  const personRecord = useRecord(sailorType, nameId, 'name');
  const serviceRecord = useRecord(sailorType, nameId, 'service');
  const otherServiceRecord = useRecord(sailorType, nameId, 'service_other');
  const otherDataRecord = useRecord(sailorType, nameId, 'data_other');
  const dirty = useDirtySailor(sailorType, nameId);
  const blocker = useDirtySailorBlocker(dirty);
  const [ locked, setLocked ] = useState(false);

  const allStatus = [
    personRecord.status,
    serviceRecord.status,
    otherServiceRecord.status,
    otherDataRecord.status,
    personRecord.mutation.status,
    serviceRecord.mutation.status,
    otherServiceRecord.mutation.status,
    otherDataRecord.mutation.status,
  ];

  useEffect(()=>{
    if(isNew(nameId)) document.title = 'New ' + sailorType;
    else if(personRecord.data) {
      if(sailorType === 'rating') document.title = catref(personRecord.data);
      else if(sailorType === 'officer') document.title = 'Officer #' + officerref(personRecord.data);
      else throw new Error(); //this should never happen
    }
    else document.title = 'Fetching ' + sailorType + ' ' + nameId;
  }, [nameId, personRecord.data, sailorType]);

  if(
    (sailorType !== 'rating' && sailorType !== 'officer') ||
    (dataType !== 'main' && dataType !== 'otherservices' && dataType !== 'otherdata')
  ) {
    return (<Alert severity='error'>Bad location: {pathname}</Alert>);
  }
  else {
    if(allStatus.some((e) => e === 'error')) {
      return (<Alert severity='error'>Error getting or posting data</Alert>);
    }

    const loading = allStatus.some((e) => e === 'pending');
    if(loading) {
      return (<Stack height='100vh' width='100vw' alignItems='center' justifyContent='center'><CircularProgress size='50vh'/></Stack>);
    }
    else { //mutations are in idle (i.e. have never run) or success state. queries are in success state.
     return (
        <LoadingContext value={loading}>{/* Currently hidden by the spinner for allStatus pending above */ }
          <DirtySailorContext value={dirty}>
            <LockedContext value={[locked, setLocked]}>
              <BlockNavigationDialog blocker={blocker}/>
              <Stack spacing={2} width={0.95}>
                <Card style={{backgroundColor: dirty.any() ? '#ff9999' : '#99ff99'}}>
                  <Typography style={{fontWeight: 'bold'}}>{
                    dirty.any() ? "Record has unsaved change(s)"
                                : "Record has no unsaved changes"
                  }</Typography>
                </Card>
              </Stack>
              <Stack direction='row' spacing={2} alignItems='center' justifyContent='space-around' width={0.95}>
                <Stack sx={{alignItems: 'center', justifyContent: 'space-evenly'}} spacing={4}>
                  <PersonData record={personRecord}/>
                  <Stack alignItems='center' spacing={2} width='95vw'>
                    <Tabs value={dataType} onChange={(e,v) => {navigate('/person/' + sailorType + '/' + nameId + '/' + v);}}>
                      {sailorType === 'rating' && <Tab value='main' label='Services' disabled={loading|locked} sx={((dataType !== 'main') && dirty.service) ? { fontWeight: 'bold' } : null }/>}
                      <Tab value='otherservices' label={sailorType === 'rating' ? 'Other Services' : 'Services'} disabled={true|loading|locked} sx={((dataType !== 'otherservices') && dirty.service_other) ? { fontWeight: 'bold' } : null }/>
                      <Tab value='otherdata' label='Data' disabled={true|loading|locked} sx={((dataType !== 'otherdata') && dirty.data_other) ? { fontWeight: 'bold' } : null }/>
                    </Tabs>
                    <Card variant='outlined'>
                      {dataType === 'main' &&          <ServiceReconciler record={serviceRecord}/>}
                      {dataType === 'otherservices' && <OtherServices record={otherServiceRecord}/>}
                      {dataType === 'otherdata' &&     <OtherData record={otherDataRecord}/>}
                    </Card>
                  </Stack>
                </Stack>
              </Stack>
            </LockedContext>
          </DirtySailorContext>
        </LoadingContext>
      );
    }
  }
}
