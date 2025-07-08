'use client';

import { useAladinContext } from '@/components/Aladin/AladinContext';
import { Typography } from '@mui/material';
import Stack from '@mui/material/Stack';
import List from '@mui/material/List';
import CatalogListItem from './CatalogListItem';

export default function CatalogControls() {
  const { catalogsRef } = useAladinContext();

  return (
    <Stack spacing={2}>
      <Typography variant="h6">Catalogs</Typography>
      <List
        sx={{
          width: '100%',
        }}
        component="nav"
      >
        {catalogsRef.current && Object.keys(catalogsRef.current).length > 0 && (
          Object.keys(catalogsRef.current).map(key => {
            const catalog = catalogsRef.current[key];
            return (
              <CatalogListItem key={`catalog-list-item-${catalog.name.replace(/ /g, "-")}`} catalog={catalog} />
            )
          }))
        }
      </List>
    </Stack>

  );
}
