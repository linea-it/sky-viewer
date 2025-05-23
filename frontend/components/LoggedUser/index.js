"use client";
import React from 'react';
import PropTypes from 'prop-types'
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Typography from '@mui/material/Typography';
import ListItemIcon from '@mui/material/ListItemIcon';
import LogoutIcon from '@mui/icons-material/Logout';
import Button from '@mui/material/Button';

import { useAuth } from "@/contexts/AuthContext";

export default function LoggedUser({ username }) {
  const { logout } = useAuth();

  const [anchorEl, setAnchorEl] = React.useState(null)

  const handleMenu = event => {
    setAnchorEl(event.currentTarget)
  }

  const handleClose = () => {
    setAnchorEl(null)
  }

  return (
    <React.Fragment>
      <Button onClick={handleMenu} color="inherit" sx={{ display: { xs: 'none', md: 'block' } }}>
        {username}
      </Button>
      <Menu
        sx={{ mt: '45px' }}
        id="menu-appbar"
        anchorEl={anchorEl}
        anchorOrigin={{
          vertical: 'top',
          horizontal: 'right'
        }}
        keepMounted
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right'
        }}
        open={Boolean(anchorEl)}
        onClose={handleClose}
      >
        <MenuItem onClick={logout}>
          <ListItemIcon>
            <LogoutIcon fontSize="small" />
          </ListItemIcon>
          <Typography variant="inherit">Logout</Typography>
        </MenuItem>
      </Menu>
    </React.Fragment>
  )
}

LoggedUser.propTypes = {
  username: PropTypes.string.isRequired,
}
