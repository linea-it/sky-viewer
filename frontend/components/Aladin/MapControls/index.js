'use client';

import { useAladinContext } from '@/components/Aladin/AladinContext';
import { Typography } from '@mui/material';
import Stack from '@mui/material/Stack';
import List from '@mui/material/List';
import MapListItem from './MapListItem';

export default function MapControls() {
  const { mapsList } = useAladinContext();

  if (!mapsList || mapsList.length === 0) return null;

  return (
    <Stack spacing={2}>
      <Typography variant="h6">Maps</Typography>
      <List
        sx={{
          width: '100%',
        }}
        component="nav"
      >
        {mapsList.map(mapGroup => (
          <MapListItem key={`map-list-item-${mapGroup.surveyKey}`} mapGroup={mapGroup} />
        ))}
      </List>
    </Stack>

  );
}
