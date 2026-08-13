import React from 'react';
import PropTypes from 'prop-types'
import MenuItem from '@mui/material/MenuItem';
import TextField from '@mui/material/TextField';

export default function MapSelect({ value, onChange, options }) {

  const handleChange = (event) => {
    onChange(event.target.value);
  }

  return (
    <TextField
      select
      label="Map"
      value={value}
      onChange={handleChange}
      fullWidth
    >
      {options.map((option) => (
        <MenuItem key={option.value} value={option.value}>
          {option.label}
        </MenuItem>
      ))}
    </TextField>
  )
}

MapSelect.propTypes = {
  value: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
  options: PropTypes.arrayOf(PropTypes.shape({
    value: PropTypes.string.isRequired,
    label: PropTypes.string.isRequired,
  })).isRequired,
}
