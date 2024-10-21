import * as React from 'react';
import Typography from '@mui/material/Typography';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid2';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import Link from '@mui/material/Link'

export default function Tutorials() {
  return (
    <Container>
      <Grid container spacing={8}>
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
            Tutorials page is comming ...
          </Typography>
        </Grid>
      </Grid>
    </Container>
  );
}
