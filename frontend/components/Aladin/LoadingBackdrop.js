'use client';

import Backdrop from '@mui/material/Backdrop';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import Typography from '@mui/material/Typography';
import { useAladinContext } from './AladinContext';

// Bloqueia o Aladin e o painel até a primeira imagem entrar na pilha.
// Se as imagens não carregarem, avisa e permite tentar de novo.
export default function LoadingBackdrop() {
  const { isReady, loadFailed, retryLoad } = useAladinContext();

  return (
    <Backdrop
      open={!isReady}
      // O MUI marca o backdrop como aria-hidden, mas aqui ele tem conteúdo: aviso e botão
      aria-hidden={false}
      sx={{
        position: 'absolute',
        flexDirection: 'column',
        gap: 2,
        color: '#fff',
        zIndex: (theme) => theme.zIndex.drawer + 1,
      }}
    >
      {loadFailed ? (
        <>
          <Typography variant="body1">Images are currently unavailable.</Typography>
          <Button variant="contained" onClick={retryLoad}>Try again</Button>
        </>
      ) : (
        <>
          <CircularProgress color="inherit" />
          <Typography variant="body1">Loading images...</Typography>
        </>
      )}
    </Backdrop>
  );
}
