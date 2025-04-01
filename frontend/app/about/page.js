import * as React from 'react';
import Typography from '@mui/material/Typography';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid2';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import Link from 'next/link'
import Box from '@mui/material/Box'
import Footer from "@/components/Footer";
import Divider from '@mui/material/Divider';

export default function About() {
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
              <Typography color="textPrimary">About</Typography>
            </Breadcrumbs>
            <Typography variant="h6" sx={{ mb: 2 }}>
              About
            </Typography>
            <Typography variant="body1" component="span">
              The Sky Viewer is a service based on the Aladin visualization tool that displays HIPS images and catalog overlays from various sources, including public data available online and public and private data hosted at LIneA. The list of available datasets depends on the data rights of each user.
            </Typography>
            <Divider sx={{ mt: 4, mb: 4 }} />
            <Typography variant="h6" sx={{ mb: 2 }}>
              About Us
            </Typography>
            <Typography variant="body1">
              LIneA is a multi-user laboratory operated by a non-profit organization with financial support from the Brazilian Ministry of Science, Technology, and Innovation. Our mission is to support the Brazilian astronomical community with computing infrastructure and big data analysis expertise to provide technical conditions for participation in large astronomical surveys, such as SDSS, DES, and LSST.
            </Typography>
            <Typography variant="body1">
              <br />
              If you are a Portuguese speaker, please find more about LIneA on our {' '}
              <Link
                href="https://youtu.be/jC-k85tfd0Y"
                target="_blank"
                rel="noreferrer"
              >
                YouTube Channel
              </Link>{' '}
              and {' '}
              <Link
                href="http://linea.org.br/"
                target="_blank"
                rel="noreferrer"
              >
                Website
              </Link>.
            </Typography>
            <Divider sx={{ mt: 4, mb: 4 }} />
            <Typography variant="h6" sx={{ mb: 2 }}>
              Acknowledgements
            </Typography>
            <Typography variant="body1" component="blockquote" >
              This application has made use of &quot;Aladin sky atlas&quot; developed at CDS, Strasbourg Observatory, France
              →
              {' '}<Link
                href="https://ui.adsabs.harvard.edu/abs/2000A%26AS..143...33B/abstract"
                target="_blank"
                rel="noreferrer"
              >
                2000A&AS..143...33B
              </Link>{' '}
              (Aladin Desktop),
              {' '}<Link
                href="https://ui.adsabs.harvard.edu/abs/2014ASPC..485..277B/abstract"
                target="_blank"
                rel="noreferrer"
              >
                2014ASPC..485..277B
              </Link>{' '} (Aladin Lite v2), and
              {' '}<Link
                href="https://ui.adsabs.harvard.edu/abs/2022ASPC..532....7B/abstract"
                target="_blank"
                rel="noreferrer"
              >
                2022ASPC..532....7B
              </Link>{' '}
              (Aladin Lite v3).

            </Typography>
          </Grid>
        </Grid>
      </Box >
      <Footer />
    </Container>
  );
}
