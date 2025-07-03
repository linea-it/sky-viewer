'use client';

import { useAladinContext } from './AladinContext';
import { Button, Typography } from '@mui/material';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import CatalogControls from '@/components/Aladin/CatalogControls';

export default function Controls() {
    const { isReady, setFoV, setTarget, addMarker } = useAladinContext();

    const handleGotoM42 = () => {
        setTarget('M42');
        setFoV(1.0);
        addMarker(83.82208, -5.39111, { popupTitle: 'Orion Nebula' });
    };

    return (
        <Stack spacing={2} m={2}>
            <Typography variant="h6">Surveys</Typography>
            <Button variant="contained" disabled={!isReady} onClick={handleGotoM42}>
                DES DR2 IRG
            </Button>
            <Button variant="contained" disabled={!isReady} onClick={handleGotoM42}>
                LSST DP0.2 IRG
            </Button>
            <hr></hr>
            <CatalogControls />
        </Stack>

    );
}
