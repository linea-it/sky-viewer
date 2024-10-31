'use client';
import Box from '@mui/material/Box';
import Aladin from "@/components/Aladin";


export default function Home() {
  return (
    // tamanho minimo do container principal deve ser
    // Total disponivel 100vg - 64px do header ( O footer deve ficar abaixo do aladin acessado pelo scroll )
    <Box id={'home-box'} sx={{
      flexGrow: 1,
      minHeight: 'calc(100vh - 64px)'
    }}>
      {/* SE tiver mais componentes tipo menu de escolha dos releases vai ficar aqui.*/}
      {typeof window === "undefined" ? (<div>loading...</div>) : (<Aladin />)}
    </Box>
  );
}
