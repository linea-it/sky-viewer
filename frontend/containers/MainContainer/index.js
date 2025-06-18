
"use client";
import React from "react";
import CssBaseline from '@mui/material/CssBaseline';
import Backdrop from '@mui/material/Backdrop';
import CircularProgress from '@mui/material/CircularProgress';
import Box from "@mui/material/Box";
import Header from "@/components/Header";
import { useAuth } from "@/contexts/AuthContext";

export default function MainContainer({ children }) {
  const { settings } = useAuth();

  if (Object.keys(settings).length === 0) {
    return (
      <Backdrop
        sx={(theme) => ({ color: '#fff', zIndex: theme.zIndex.drawer + 1 })}
        open={true}
      >
        <CircularProgress color="inherit" />
      </Backdrop>
    )
  }

  return (
    <React.Fragment>
      <CssBaseline />
      <Header />
      <Box
        component='main'
        sx={{
          paddingLeft: 0,
          paddingRight: 0,
          display: 'flex',
        }}>
        {children}
      </Box>
    </React.Fragment>
  );
}