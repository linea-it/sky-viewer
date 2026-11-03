import React from 'react';
import PropTypes from 'prop-types'
import { useAladinContext } from '@/components/Aladin/AladinContext';
import LayerListItem from '@/components/Aladin/LayerListItem';
import OpacitySlider from '@/components/Aladin/OpacitySlider';
import MapSelect from './MapSelect';

const DEFAULT_OPACITY = 0.8;

export default function MapListItem({ mapGroup }) {

  const { setMapOverlay, setMapOpacity, setMapVisibility, removeMapOverlay } = useAladinContext();

  const [open, setOpen] = React.useState(false);
  const [visible, setVisible] = React.useState(false);
  const [mapId, setMapId] = React.useState('');
  const [opacity, setOpacity] = React.useState(DEFAULT_OPACITY);

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
    setMapVisibility(mapGroup.surveyKey, !visible);
    setVisible(!visible);
  };

  const handleExtend = () => {
    setOpen(!open);
  };

  const handleMapChange = (value) => {
    setMapId(value);
    setMapOverlay(mapGroup.surveyKey, value, opacity);
    setVisible(true);
  };

  const handleOpacityChange = (value) => {
    setOpacity(value);
    setMapOpacity(mapGroup.surveyKey, value);
  };

  const handleRemove = () => {
    removeMapOverlay(mapGroup.surveyKey);
    setMapId('');
    setVisible(false);
    setOpacity(DEFAULT_OPACITY);
  };

  return (
    <LayerListItem
      name={mapGroup.name}
      visible={visible}
      open={open}
      onToggle={handleToggle}
      onExtend={handleExtend}
      onRemove={handleRemove}
      removeDisabled={!mapId}
    >
      <MapSelect
        value={mapId}
        onChange={handleMapChange}
        options={options}
      />
      <OpacitySlider
        value={opacity}
        onChange={handleOpacityChange}
      />
    </LayerListItem>
  )
}

MapListItem.propTypes = {
  mapGroup: PropTypes.object.isRequired,
}
