import * as React from 'react';
import Typography from '@mui/material/Typography';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid2';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import Link from 'next/link'
import Box from '@mui/material/Box'
import Footer from "@/components/Footer";

export default function Tutorials() {
  return (
    <Container>
      <Box
        component="main"
        sx={{
          display: 'flex',
          flexDirection: 'column',
          minHeight: 'calc(100vh - 64px - 496px)',
          padding: 2
        }}
      >
        <Grid container spacing={2}>
          <Grid item xs={12}>
            <Breadcrumbs aria-label="breadcrumb" sx={{ mt: 2 }}>
              <Link color="inherit" href="/">
                Home
              </Link>
              <Typography color="textPrimary">Tutorials</Typography>
            </Breadcrumbs>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Tutorials
            </Typography>
            <Typography>
              Tutorials page is coming ...
            </Typography>
          </Grid>
        </Grid>
      </Box>
      <Footer />
    </Container>
  );
}
