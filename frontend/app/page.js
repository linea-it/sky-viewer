'use client';
import Box from '@mui/material/Box';
// import Aladin from "@/components/Aladin";
import { useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
const Aladin = dynamic(() => import('@/components/Aladin'), { ssr: false })

export default function Home() {
  const [isClient, setIsClient] = useState(false)

  useEffect(() => {
    setIsClient(true)
  }, [])

  return (
    // tamanho minimo do container principal deve ser
    // Total disponivel 100vg - 64px do header ( O footer deve ficar abaixo do aladin acessado pelo scroll )
    <Box id={'home-box'} sx={{
      flexGrow: 1,
      minHeight: 'calc(100vh - 64px)'
    }}>
      {/* SE tiver mais componentes tipo menu de escolha dos releases vai ficar aqui.*/}
      {/* {typeof window !== "undefined" && (<Aladin />)} */}
      {isClient ? <Aladin /> : 'Loading'}
    </Box>
  );
}
