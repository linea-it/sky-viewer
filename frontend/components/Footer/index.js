import * as React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';

export default function Footer() {
  return (
    <Box sx={{ backgroundColor: 'gray' }}>
      <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
        Footer
      </Typography>
    </Box>
  );
}
