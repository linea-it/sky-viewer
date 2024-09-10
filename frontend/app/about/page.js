import * as React from 'react';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import NextLink from 'next/link';

export default function About() {
  return (
    <Box
      sx={{
        my: 4,
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        alignItems: 'center',
      }}
    >
      <Typography variant="h4" component="h1" sx={{ mb: 2 }}>
        About Page
      </Typography>
      <Box sx={{ maxWidth: 'sm' }}>
        <Button variant="contained" component={NextLink} href="/">
          Go to the home page
        </Button>
      </Box>
    </Box>
  );
}
