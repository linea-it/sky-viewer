import React from 'react';
import PropTypes from 'prop-types'
import Stack from '@mui/material/Stack';
import { useAladinContext } from '@/components/Aladin/AladinContext';
import LayerListItem from '@/components/Aladin/LayerListItem';
import ShapeSelect from './ShapeSelect';
import ColorSelect from './ColorSelect';
import SizeSelect from './SizeSelect';

export default function CatalogListItem({ catalogId, catalog }) {
  const { removeCatalogLayer } = useAladinContext();

  const [open, setOpen] = React.useState(true);
  const [checked, setChecked] = React.useState(catalog.isShowing);
  const [color, setColor] = React.useState(catalog.color);
  const [shape, setShape] = React.useState(catalog.shape);
  const [size, setSize] = React.useState(catalog.sourceSize);

  const handleToggle = () => {
    if (!checked) {
      catalog.show();
    } else {
      catalog.hide();
    }
    setChecked(!checked);
  };

  const handleExtend = () => {
    setOpen(!open);
  };

  const handleColorChange = (value) => {
    catalog.updateShape({ color: value });
    setColor(value);
  };

  const handleSizeChange = (value) => {
    catalog.updateShape({ sourceSize: parseInt(value) });
    setSize(value);
  };

  const handleShapeChange = (value) => {
    catalog.updateShape({ shape: value });
    setShape(value);
  };

  return (
    <LayerListItem
      name={catalog.name}
      visible={checked}
      open={open}
      onToggle={handleToggle}
      onExtend={handleExtend}
      onRemove={() => removeCatalogLayer(catalogId)}
    >
      <Stack direction="row" spacing={1}>
        <ColorSelect
          value={color}
          onChange={handleColorChange}
        />
        <ShapeSelect
          value={shape}
          onChange={handleShapeChange}
        />
      </Stack>
      <SizeSelect
        value={size}
        onChange={handleSizeChange}
      />
    </LayerListItem>
  )
}

CatalogListItem.propTypes = {
  catalogId: PropTypes.string.isRequired,
  catalog: PropTypes.object.isRequired,
}
