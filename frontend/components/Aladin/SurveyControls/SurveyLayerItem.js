import React from 'react';
import PropTypes from 'prop-types'
import { useAladinContext } from '@/components/Aladin/AladinContext';
import LayerListItem from '@/components/Aladin/LayerListItem';
import OpacitySlider from '@/components/Aladin/OpacitySlider';

export default function SurveyLayerItem({ surveyLayer, removeDisabled, isTop }) {
  const { setSurveyLayerOpacity, setSurveyLayerVisibility, removeSurveyLayer, centerOnSurvey } = useAladinContext();

  const [open, setOpen] = React.useState(false);

  return (
    <LayerListItem
      name={surveyLayer.label}
      visible={surveyLayer.visible}
      open={open}
      onToggle={() => setSurveyLayerVisibility(surveyLayer.name, !surveyLayer.visible)}
      onExtend={() => setOpen(!open)}
      onRemove={() => removeSurveyLayer(surveyLayer.name)}
      removeDisabled={removeDisabled}
      onCenter={() => centerOnSurvey(surveyLayer.id)}
      centerDisabled={!surveyLayer.hasTarget}
      selected={isTop}
    >
      <OpacitySlider
        value={surveyLayer.opacity}
        onChange={(value) => setSurveyLayerOpacity(surveyLayer.name, value)}
      />
    </LayerListItem>
  )
}

SurveyLayerItem.propTypes = {
  surveyLayer: PropTypes.object.isRequired,
  removeDisabled: PropTypes.bool.isRequired,
  isTop: PropTypes.bool.isRequired,
}
