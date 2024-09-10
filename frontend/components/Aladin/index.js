import * as React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';

export default function Aladin() {
  return (
    <Box sx={{
      backgroundColor: 'darkgray',
      height: '100%',
      width: '100%',
    }}>
      <Typography variant="h4" component="h1" sx={{ mb: 2 }}>
        Aladin
      </Typography>
    </Box>
  );
}
