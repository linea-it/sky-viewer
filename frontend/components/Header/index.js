"use client";
import * as React from 'react';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import ListItem from '@mui/material/ListItem';
import List from '@mui/material/List';
import Link from 'next/link'
import Image from 'next/image'
import IconButton from '@mui/material/IconButton';
import MenuIcon from '@mui/icons-material/Menu';
import Drawer from '@mui/material/Drawer';
import ListItemText from '@mui/material/ListItemText';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import LoggedUser from '@/components/LoggedUser';
import Chip from '@mui/material/Chip';
import { useAuth } from '@/contexts/AuthContext';


export default function Header() {
  const { user, settings } = useAuth();
  const [drawerOpen, setDrawerOpen] = React.useState(false);

  const navItems = [
    {
      description: 'Home',
      href: '/',
      target: '_self',
    },
    {
      description: 'About',
      href: '/about/',
      target: '_blank',
    },
    {
      description: 'Contact',
      href: settings?.is_dev ? 'https://scienceplatform-dev.linea.org.br/lsp/contact' : 'https://scienceplatform.linea.org.br/lsp/contact',
      target: '_blank',
    },
    {
      description: 'LSP',
      href: settings?.is_dev ? 'https://scienceplatform-dev.linea.org.br/lsp' : 'https://scienceplatform.linea.org.br/lsp',
      target: '_blank',
    },
    {
      description: 'IDAC',
      href: settings?.is_dev ? 'https://scienceplatform-dev.linea.org.br/idac' : 'https://scienceplatform.linea.org.br/idac',
      target: '_blank',
    }
  ];

  const toggleDrawer = (open) => () => {
    setDrawerOpen(open);
  };

  const drawerList = () => (
    <List>
      {navItems.map(item => (
        <ListItem button key={item.href}>
          <Link href={item.href} color="inherit" underline="none" target={item.target}>
            <ListItemText primary={item.description} />
          </Link>
        </ListItem>
      ))}
    </List>
  );

  return (
    <React.Fragment>
      <AppBar position="fixed" sx={{
        background: '-webkit-linear-gradient(120deg,  #31297f, #0989cb)',
      }}>
        <Toolbar>
          <Image src="/linea-symbol.svg" alt="LIneA" width={52} height={40} />
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
            {navItems.map(item => (
              <ListItem key={item.href} sx={{ width: 'auto' }}>
                <Link href={item.href} color="inherit" underline="none" target={item.target}>
                  {item.description}
                </Link>
              </ListItem>
            ))}
          </List>
          <Chip label="Beta" variant="outlined" color="warning" size="small" />

          <Box sx={{ flexGrow: 1 }} />

          <Button href="https://data.linea.org.br/" target="_blank" color="inherit" variant="outlined" sx={{ mr: 2 }} size="small">
            Data
          </Button>
          <Button href="https://docs.linea.org.br/" target="_blank" color="inherit" variant="outlined" size="small" sx={{ mr: 4 }}>
            Docs
          </Button>

          {user && (
            <LoggedUser username={user?.username} />
          )}
          {!user && (
            <Button href={settings.login_url} color="inherit" sx={{ display: { xs: 'none', md: 'block' } }}>
              Sign In
            </Button>
          )}
        </Toolbar>
        <Drawer anchor="left" open={drawerOpen} onClose={toggleDrawer(false)}>
          {drawerList()}
        </Drawer>
      </AppBar >
      {/* https://mui.com/material-ui/react-app-bar/#fixed-placement */}
      {/* The toolbar is used to create space for the AppBar */}
      <Toolbar />
    </React.Fragment>
  );
}
