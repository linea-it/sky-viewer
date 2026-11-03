'use client';

import Backdrop from '@mui/material/Backdrop';
import CircularProgress from '@mui/material/CircularProgress';
import Typography from '@mui/material/Typography';
import { useAladinContext } from './AladinContext';

// Bloqueia o Aladin e o painel até a primeira imagem entrar na pilha
export default function LoadingBackdrop() {
  const { isReady } = useAladinContext();

  return (
    <Backdrop
      open={!isReady}
      sx={{
        position: 'absolute',
        flexDirection: 'column',
        gap: 2,
        color: '#fff',
        zIndex: (theme) => theme.zIndex.drawer + 1,
      }}
    >
      <CircularProgress color="inherit" />
      <Typography variant="body1">Loading images...</Typography>
    </Backdrop>
  );
}
