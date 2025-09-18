import { useState } from 'react';
import { AppBar, IconButton, Menu, MenuItem, Toolbar, Typography } from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import { useAuthenticator } from '@aws-amplify/ui-react';
import { usePrefs } from './prefs';

//Menu implementation follows https://www.reddit.com/r/reactjs/comments/vhsijs/material_ui_open_a_single_dropdown_menu_instead/?utm_source=share&utm_medium=web3x&utm_name=web3xcss&utm_term=1&utm_content=share_button
export default function LaSAppBar() {
  const [menuAnchorEl, setMenuAnchorEl] = useState(null);
  const { signOut } = useAuthenticator();
  const prefs = usePrefs();

  return (
    <AppBar position='sticky'>
      <Toolbar>
        <IconButton size='large' edge='start' color='inherit' aria-label='menu'>
          <MenuIcon onClick={(e) => {setMenuAnchorEl(e.currentTarget)}}/>
        </IconButton>
        <Menu anchorEl={menuAnchorEl} onClose={()=>setMenuAnchorEl(null)} onClick={()=>setMenuAnchorEl(null)} open={Boolean(menuAnchorEl)}>
          <MenuItem onClick={prefs.toggleScreenshot}>{prefs.screenshot ? "Disable screenshot preview" : "Enable screenshot preview"}</MenuItem>
          <MenuItem onClick={signOut}>Sign out</MenuItem>
          <MenuItem onClick={()=>signOut({global:true})}>Sign out everywhere</MenuItem>
        </Menu>
        <Typography variant='h6'>Lives at Sea</Typography>
      </Toolbar>
    </AppBar>
  );
}
