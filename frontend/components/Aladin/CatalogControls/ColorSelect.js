import React from 'react';
import PropTypes from 'prop-types'
import TextField from '@mui/material/TextField';

export default function ColorSelect({ value, onChange }) {

  const handleChange = (event) => {
    onChange(event.target.value);
  }

  return (

    <TextField
      label="Color"
      variant="outlined"
      fullWidth
      slotProps={{ htmlInput: { type: "color" } }}
      value={value}
      onChange={handleChange} />
  )
}

ColorSelect.propTypes = {
  value: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
}
