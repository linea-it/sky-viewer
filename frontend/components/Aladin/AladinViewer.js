'use client';

import Box from '@mui/material/Box';
import { useAladinContext } from './AladinContext';

export default function AladinViewer() {
    const { containerRef } = useAladinContext();

    return (
        <Box
            ref={containerRef}
            sx={{
                backgroundColor: 'darkgray',
                height: '100%',
                width: '100%',
            }}
        />
    );
}
