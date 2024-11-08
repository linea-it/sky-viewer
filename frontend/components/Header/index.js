"use client";
import * as React from 'react';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import ListItem from '@mui/material/ListItem';
import List from '@mui/material/List';
import Link from '@mui/material/Link';
import IconButton from '@mui/material/IconButton';
import MenuIcon from '@mui/icons-material/Menu';
import Drawer from '@mui/material/Drawer';
import ListItemText from '@mui/material/ListItemText';

export default function Header() {
  const [drawerOpen, setDrawerOpen] = React.useState(false);

  const menus = [
    { description: 'HOME', href: '/skyviewer/' },
    { description: 'ABOUT', href: '/skyviewer/about/' },
    { description: 'TUTORIALS', href: '/skyviewer/tutorials/' },
    { description: 'CONTACT', href: '/skyviewer/contact/' },
  ];

  const toggleDrawer = (open) => () => {
    setDrawerOpen(open);
  };

  const drawerList = () => (
    <List>
      {menus.map(menu => (
        <ListItem button key={menu.href}>
          <Link href={menu.href} color="inherit" underline="none">
            <ListItemText primary={menu.description} />
          </Link>
        </ListItem>
      ))}
    </List>
  );

  return (
    <AppBar position="static">
      <Toolbar sx={{ backgroundColor: '#212121' }}>
        <IconButton
          edge="start"
          color="inherit"
          aria-label="menu"
          onClick={toggleDrawer(true)}
          sx={{ display: { xs: 'block', md: 'none' } }}
        >
          <MenuIcon />
        </IconButton>
        <List sx={{ display: { xs: 'none', md: 'flex' } }}>
          {menus.map(menu => (
            <ListItem key={menu.href} sx={{ width: 'auto' }}>
              <Link href={menu.href} color="inherit" underline="none">
                {menu.description}
              </Link>
            </ListItem>
          ))}
        </List>
      </Toolbar>
      <Drawer anchor="left" open={drawerOpen} onClose={toggleDrawer(false)}>
        {drawerList()}
      </Drawer>
    </AppBar>
  );
}
