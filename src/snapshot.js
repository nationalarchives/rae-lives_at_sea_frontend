let SNAP_COUNT = 0;

async function post_data(stamp, name, data) {
  let year  = stamp.getFullYear();
  let month = stamp.getMonth() + 1; month = month.toString().padStart(2, '0');
  let day   = stamp.getDate().toString().padStart(2, '0');
  return new Promise((resolve, reject) => {
    const fetchData = async() => {
      const response = await fetch(`/raw/${year}-${month}-${day}/${name}.json`, {
        method: 'PUT',
        body: JSON.stringify(data),
      });
      if(!response.ok) {
        let msg = '' + response.status;
        if(response.statusText) msg += `: ${response.statusText}`;
        reject(new Error(msg, { cause: response }));
      }
      else {
        resolve(response);
      }
    }
    fetchData();
  });
}

function get_data(name, data) {
  if(data) {
    let link = document.createElement('a');
    link.download = name + '.json';
    link.href = 'data:text/json;charset=utf-8,' + encodeURIComponent(JSON.stringify(data))
    link.click();
    link.remove();
  }
}

export function snapshot(id, client_snapshots, audit, identifier) {
  let stamp = new Date();
  const prefix = `LatS_${SNAP_COUNT.toString().padStart(7, 0)}_${stamp.toISOString().replaceAll(':', '_')}_${identifier}_${id}`;
  SNAP_COUNT += 1;

  let  sync_data = {};
  let async_data = {};
  for(const k of Object.getOwnPropertyNames(audit)) {
    if(Object.hasOwn(audit[k], 'data'))       sync_data[k] = audit[k].data;
    if(Object.hasOwn(audit[k], 'queryData')) async_data[k] = audit[k].queryData;
  }
  if(Object.getOwnPropertyNames( sync_data).length === 0)  sync_data = null;
  if(Object.getOwnPropertyNames(async_data).length === 0) async_data = null;

  let promises = [post_data(stamp, `${prefix}__sync`,  sync_data), post_data(stamp, `${prefix}_async`, async_data)];
  if(client_snapshots) {
    promises.push(new Promise((r) => {
      get_data(`${prefix}__sync`,  sync_data);
      get_data(`${prefix}_async`, async_data);
      r();
    }));
  }
  return Promise.allSettled(promises);
}

