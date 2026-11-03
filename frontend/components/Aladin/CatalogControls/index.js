'use client';

import { useAladinContext } from '@/components/Aladin/AladinContext';
import { Typography } from '@mui/material';
import Stack from '@mui/material/Stack';
import List from '@mui/material/List';
import MenuItem from '@mui/material/MenuItem';
import TextField from '@mui/material/TextField';
import CatalogListItem from './CatalogListItem';

export default function CatalogControls() {
  const { isReady, catalogsRef, activeCatalogIds, addCatalogLayer } = useAladinContext();

  const availableIds = Object.keys(catalogsRef.current || {})
    .filter(id => !activeCatalogIds.includes(id));

  return (
    <Stack spacing={2}>
      <Typography variant="h6">Catalogs</Typography>
      <TextField
        select
        fullWidth
        size="small"
        label="Add catalog"
        value=""
        disabled={!isReady || availableIds.length === 0}
        onChange={(event) => addCatalogLayer(event.target.value)}
      >
        {availableIds.map(id => (
          <MenuItem key={`catalog-option-${id}`} value={id}>
            {catalogsRef.current[id].name}
          </MenuItem>
        ))}
      </TextField>
      <List
        sx={{
          width: '100%',
        }}
        component="nav"
      >
        {activeCatalogIds.map(id => (
          <CatalogListItem key={`catalog-list-item-${id}`} catalogId={id} catalog={catalogsRef.current[id]} />
        ))}
      </List>
    </Stack>

  );
}
