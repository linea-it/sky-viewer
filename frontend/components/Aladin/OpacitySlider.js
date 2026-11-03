import React from 'react';
import PropTypes from 'prop-types'
import Box from '@mui/material/Box';
import Slider from '@mui/material/Slider';
import Typography from '@mui/material/Typography';

export default function OpacitySlider({ value, onChange }) {

  const handleChange = (event) => {
    onChange(event.target.value);
  }

  return (
    <Box>
      <Typography gutterBottom variant="body2" color={'text.secondary'} >Opacity</Typography>
      <Slider value={value} step={0.05} min={0} max={1} valueLabelDisplay="auto" onChange={handleChange} />
    </Box>
  )
}

OpacitySlider.propTypes = {
  value: PropTypes.number.isRequired,
  onChange: PropTypes.func.isRequired,
}
