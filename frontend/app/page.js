'use client';
import Box from '@mui/material/Box';
import Aladin from "@/components/Aladin";
import dynamic from 'next/dynamic'


// const DynamicAladin = dynamic(() => import('../components/Aladin'), {
//   loading: () => <p>Loading...</p>,
// })

export default function Home() {
  return (
    <Box id={'home-box'} sx={{ flexGrow: 1 }}>
      {/* SE tiver mais componentes tipo menu de escolha dos releases vai ficar aqui.*/}
      {typeof window === "undefined" ? (<div>loading...</div>) : (<Aladin />)}
    </Box>
  );
}
