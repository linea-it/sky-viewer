import React from 'react';
import PropTypes from 'prop-types'
import Box from '@mui/material/Box';
import Slider from '@mui/material/Slider';
import Typography from '@mui/material/Typography';

export default function SizeSelect({ value, onChange }) {

  const handleChange = (event) => {
    onChange(event.target.value);
  }

  return (
    <Box>
      <Typography gutterBottom variant="body2" color={'text.secondary'} >Size</Typography>
      <Slider defaultValue={8} value={value} step={1} min={4} max={28} valueLabelDisplay="auto" onChange={handleChange} />
    </Box>
  )
}

SizeSelect.propTypes = {
  value: PropTypes.number.isRequired,
  onChange: PropTypes.func.isRequired,
}
