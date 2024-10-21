import * as React from 'react';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import ListItem from '@mui/material/ListItem';
import List from '@mui/material/List';
import Link from '@mui/material/Link';

export default function Header() {

  const menus = [
    {
      description: 'HOME',
      href: '/'
    },
    {
      description: 'ABOUT',
      href: '/about'
    },
    {
      description: 'TUTORIALS',
      href: '/tutorials'
    },
    {
      description: 'CONTACT',
      href: '/contact'
    }
  ];

  return (
    <AppBar position="static">
      <Toolbar sx={{ backgroundColor: '#212121' }}>
        <List sx={{ display: 'flex' }}>
          {menus.map(menu => (
            <ListItem key={menu.href} sx={{ width: 'auto' }}>
              <Link href={menu.href} color="inherit" underline="none">
                {menu.description}
              </Link>
            </ListItem>
          ))}
        </List>
      </Toolbar>
    </AppBar>
  );
}
