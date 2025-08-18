'use client';
import Box from '@mui/material/Box';
import { useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { useAuth } from "@/contexts/AuthContext";
const Aladin = dynamic(() => import('@/components/Aladin'), { ssr: false })

export default function Home() {
  const { user } = useAuth();
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
      {isClient ? <Aladin userGroups={user?.groups || []} /> : 'Loading'}
    </Box>
  );
}
