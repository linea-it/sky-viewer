'use client';

import { useAladinContext } from './AladinContext';
import { Button, Stack, Typography } from '@mui/material';

export default function CatalogControls() {
    const { isReady, setFoV, setTarget, addMarker } = useAladinContext();

    const handleGotoM42 = () => {
        setTarget('M42');
        setFoV(1.0);
        addMarker(83.82208, -5.39111, { popupTitle: 'Orion Nebula' });
    };

    return (
        <Stack spacing={2}>
            <Typography variant="h6">Surveys</Typography>
            <Button variant="contained" disabled={!isReady} onClick={handleGotoM42}>
                DES DR2 IRG
            </Button>
            <Button variant="contained" disabled={!isReady} onClick={handleGotoM42}>
                LSST DP0.2 IRG
            </Button>
            <hr></hr>
            <Typography variant="h6">Catalogs</Typography>
            <Button variant="contained" disabled={!isReady} onClick={handleGotoM42}>
                Ir para Orion (M42)
            </Button>
        </Stack>
    );
}
