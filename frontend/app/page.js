import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Aladin from "@/components/Aladin";
export default function Home() {
  return (
    <Box sx={{ flexGrow: 1 }}>
      {/* SE tiver mais componentes tipo menu de escolha dos releases vai ficar aqui.*/}
      <Aladin />
    </Box>
  );
}
