import React from 'react';
import PropTypes from 'prop-types'
import Collapse from '@mui/material/Collapse';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Divider from '@mui/material/Divider';
import ExpandLess from '@mui/icons-material/ExpandLess';
import ExpandMore from '@mui/icons-material/ExpandMore';
import Visibility from '@mui/icons-material/Visibility';
import VisibilityOff from '@mui/icons-material/VisibilityOff';
import DeleteIcon from '@mui/icons-material/Delete';
import IconButton from '@mui/material/IconButton';
import Stack from '@mui/material/Stack';

// Linha de uma camada (survey, mapa ou catálogo): visibilidade, remoção e controles no collapse
export default function LayerListItem({ name, visible, open, onToggle, onExtend, onRemove, removeDisabled = false, children }) {

  return (
    <React.Fragment>
      <ListItem
        secondaryAction={
          <React.Fragment>
            <IconButton aria-label="remove" onClick={onRemove} disabled={removeDisabled}>
              <DeleteIcon />
            </IconButton>
            <IconButton edge="end" aria-label="expand" onClick={onExtend}>
              {open ? <ExpandLess /> : <ExpandMore />}
            </IconButton>
          </React.Fragment>
        }
        disablePadding
      >
        {/* Reserva o espaço dos 2 botões da secondaryAction para o nome não passar por baixo deles */}
        <ListItemButton onClick={onToggle} sx={{ pl: 1, '&&': { pr: 11 } }}>
          <ListItemIcon sx={{ minWidth: 36 }}>
            {visible ? <Visibility /> : <VisibilityOff />}
          </ListItemIcon>
          <ListItemText primary={name} title={name} primaryTypographyProps={{ noWrap: true }} />
        </ListItemButton>
      </ListItem>
      <Collapse in={open} timeout="auto" unmountOnExit>
        <Stack spacing={1} mb={1}>
          {children}
        </Stack>
      </Collapse>
      <Divider />
    </React.Fragment>
  )
}

LayerListItem.propTypes = {
  name: PropTypes.string.isRequired,
  visible: PropTypes.bool.isRequired,
  open: PropTypes.bool.isRequired,
  onToggle: PropTypes.func.isRequired,
  onExtend: PropTypes.func.isRequired,
  onRemove: PropTypes.func.isRequired,
  removeDisabled: PropTypes.bool,
  children: PropTypes.node,
}
