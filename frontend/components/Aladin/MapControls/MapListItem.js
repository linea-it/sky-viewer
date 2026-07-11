import React from 'react';
import PropTypes from 'prop-types'
import Checkbox from '@mui/material/Checkbox';
import Collapse from '@mui/material/Collapse';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Divider from '@mui/material/Divider';
import ExpandLess from '@mui/icons-material/ExpandLess';
import ExpandMore from '@mui/icons-material/ExpandMore';
import IconButton from '@mui/material/IconButton';
import Stack from '@mui/material/Stack';
import { useAladinContext } from '@/components/Aladin/AladinContext';
import MapSelect from './MapSelect';
import OpacitySlider from './OpacitySlider';

export default function MapListItem({ mapGroup }) {

  const { setMapOverlay, setMapOpacity, setMapVisibility } = useAladinContext();

  const [open, setOpen] = React.useState(false);
  const [checked, setChecked] = React.useState(false);
  const [mapId, setMapId] = React.useState('');
  const [opacity, setOpacity] = React.useState(1.0);

  const options = React.useMemo(() =>
    mapGroup.categories.flatMap(cat =>
      cat.bands.map(band => ({
        value: `${cat.id}_${band.value}`,
        label: `${cat.label} ${band.label}`,
      }))
    ), [mapGroup]);

  const handleToggle = () => {
    // Sem banda selecionada não há o que mostrar: abre o collapse para escolher
    if (!mapId) {
      setOpen(true);
      return;
    }
    setMapVisibility(mapGroup.surveyKey, !checked);
    setChecked(!checked);
  };

  const handleExtend = () => {
    setOpen(!open);
  };

  const handleMapChange = (value) => {
    setMapId(value);
    setMapOverlay(mapGroup.surveyKey, value, opacity);
    setChecked(true);
  };

  const handleOpacityChange = (value) => {
    setOpacity(value);
    setMapOpacity(mapGroup.surveyKey, value);
  };

  return (
    <React.Fragment>
      <ListItem
        key={`map-option-${mapGroup.surveyKey}`}
        secondaryAction={
          <IconButton edge="end" aria-label="expand" onClick={handleExtend}>
            {open ? <ExpandLess /> : <ExpandMore />}
          </IconButton>
        }
        disablePadding
      >
        <ListItemButton onClick={handleToggle}>
          <ListItemIcon>
            <Checkbox
              edge="start"
              checked={checked}
              tabIndex={-1}
              disableRipple
            />
          </ListItemIcon>
          <ListItemText primary={mapGroup.name} />
        </ListItemButton>
      </ListItem>
      <Collapse in={open} timeout="auto" unmountOnExit>
        <Stack spacing={1} mb={1}>
          <MapSelect
            value={mapId}
            onChange={handleMapChange}
            options={options}
          />
          <OpacitySlider
            value={opacity}
            onChange={handleOpacityChange}
          />
        </Stack>
      </Collapse>
      <Divider />
    </React.Fragment>
  )
}

MapListItem.propTypes = {
  mapGroup: PropTypes.object.isRequired,
}
