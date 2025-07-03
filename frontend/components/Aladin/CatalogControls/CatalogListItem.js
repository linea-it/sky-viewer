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
import ShapeSelect from './ShapeSelect';
import ColorSelect from './ColorSelect';
import SizeSelect from './SizeSelect';
export default function CatalogListItem({ catalog }) {

  const [open, setOpen] = React.useState(false);
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
    <React.Fragment>
      <ListItem
        key={`catalog-option-${catalog.name.replace(/ /g, "-")}`}
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
          <ListItemText primary={catalog.name} />
        </ListItemButton>
      </ListItem>
      <Collapse in={open} timeout="auto" unmountOnExit>
        <Stack direction="row" spacing={1} mb={1}>
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
      </Collapse>
      <Divider />
    </React.Fragment>
  )
}

CatalogListItem.propTypes = {
  catalog: PropTypes.object.isRequired,
}
