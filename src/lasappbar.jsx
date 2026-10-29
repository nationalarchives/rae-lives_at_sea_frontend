import { useState } from 'react';
import { AppBar, Toolbar, Typography } from '@mui/material';
import { IconButton, Menu, MenuItem } from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import { usePrefs } from './prefs';

//Menu implementation follows https://www.reddit.com/r/reactjs/comments/vhsijs/material_ui_open_a_single_dropdown_menu_instead/?utm_source=share&utm_medium=web3x&utm_name=web3xcss&utm_term=1&utm_content=share_button
export default function LaSAppBar() {
  const test_mode = usePrefs((state)=>state.test_mode);
  const [menuAnchorEl, setMenuAnchorEl] = useState(null);
  const prefs = usePrefs();
  return (
    <AppBar position='sticky' style={test_mode ? {'background-color': 'crimson'} : {}}>
      <Toolbar>
        <IconButton size='large' edge='start' color='inherit' aria-label='menu'>
          <MenuIcon onClick={(e) => {setMenuAnchorEl(e.currentTarget)}}/>
        </IconButton>
        <Menu anchorEl={menuAnchorEl} onClose={()=>setMenuAnchorEl(null)} onClick={()=>setMenuAnchorEl(null)} open={Boolean(menuAnchorEl)}>
          <MenuItem disabled onClick={prefs.toggleClientSnapshots}>{prefs.client_snapshots ? "Disable client snapshots" : "Enable client snapshots"}</MenuItem>
          <MenuItem onClick={prefs.toggleTestMode}>{prefs.test_mode ? "Switch to production mode" : "Switch to test mode"}</MenuItem>
          <MenuItem disabled>Version: {import.meta.env.VITE_FE_BUILDSTR}</MenuItem>
        </Menu>
        <Typography variant='h6'>Lives at Sea{prefs.test_mode ? " *** TESTING: WORK WILL NOT BE SAVED IN THE REAL DATABASE ***" : ""}</Typography>
      </Toolbar>
    </AppBar>
  );
}
