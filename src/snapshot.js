let SNAP_COUNT = 0;

function get_data(name, data) {
  if(data) {
    let link = document.createElement('a');
    link.download = name + '.json';
    link.href = 'data:text/json;charset=utf-8,' + encodeURIComponent(JSON.stringify(data))
    link.click();
    link.remove();
  }
}

export function snapshot(id, audit, identifier) {
  const prefix = `LatS_${SNAP_COUNT.toString().padStart(7, 0)}_${new Date().toISOString()}_${identifier}_${id}`;
  SNAP_COUNT += 1;

  let  sync_data = {};
  let async_data = {};
  for(const k of Object.getOwnPropertyNames(audit)) {
    if(Object.hasOwn(audit[k], 'data'))       sync_data[k] = audit[k].data;
    if(Object.hasOwn(audit[k], 'queryData')) async_data[k] = audit[k].queryData;
  }
  if(Object.getOwnPropertyNames( sync_data).length === 0)  sync_data = null;
  if(Object.getOwnPropertyNames(async_data).length === 0) async_data = null;

  return new Promise((r) => {
    get_data(`${prefix}__sync`,  sync_data);
    get_data(`${prefix}_async`, async_data);
    r();
  });
}

