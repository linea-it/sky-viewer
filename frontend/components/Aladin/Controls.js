'use client';

import Stack from '@mui/material/Stack';
import Divider from '@mui/material/Divider';
import SurveyControls from '@/components/Aladin/SurveyControls';
import CatalogControls from '@/components/Aladin/CatalogControls';

export default function Controls() {

    return (
        <Stack spacing={2} m={2}>
            <SurveyControls />
            <Divider />
            <CatalogControls />
        </Stack>

    );
}
