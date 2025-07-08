import React from 'react';
import PropTypes from 'prop-types'
import MenuItem from '@mui/material/MenuItem';
import TextField from '@mui/material/TextField';

export default function ShapeSelect({ value, onChange }) {

  const handleChange = (event) => {
    onChange(event.target.value);
  }

  const options = [
    { value: 'square', label: 'Square' },
    { value: 'circle', label: 'Circle' },
    { value: 'plus', label: 'Plus' },
    { value: 'cross', label: 'Cross' },
    { value: 'triangle', label: 'Triangle' },
    { value: 'rhomb', label: 'Diamond' },
  ];

  return (
    <TextField
      select
      label="Shape"
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

ShapeSelect.propTypes = {
  value: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
}
