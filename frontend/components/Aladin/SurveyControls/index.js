'use client';

import { useAladinContext } from '@/components/Aladin/AladinContext';
import { Typography } from '@mui/material';
import Stack from '@mui/material/Stack';
import List from '@mui/material/List';
import MenuItem from '@mui/material/MenuItem';
import TextField from '@mui/material/TextField';
import SurveyLayerItem from './SurveyLayerItem';

export default function SurveyControls() {
  const { isReady, surveysRef, surveyLayers, addSurveyLayer } = useAladinContext();

  // Imagem que aparece por cima: a primeira linha visível. Com um survey só não há o que destacar.
  const topLayer = surveyLayers.length > 1
    ? surveyLayers.find(surveyLayer => surveyLayer.visible && surveyLayer.opacity > 0)
    : null;

  const activeIds = surveyLayers.map(surveyLayer => surveyLayer.id);
  const availableSurveys = Object.values(surveysRef.current || {})
    .filter(survey => !activeIds.includes(survey.id));

  return (
    <Stack spacing={2}>
      <Typography variant="h6">Images</Typography>
      <TextField
        select
        fullWidth
        size="small"
        label="Add survey"
        value=""
        disabled={!isReady || availableSurveys.length === 0}
        onChange={(event) => addSurveyLayer(event.target.value)}
      >
        {availableSurveys.map(survey => (
          <MenuItem key={`survey-option-${survey.id}`} value={survey.id}>
            {survey.name}
          </MenuItem>
        ))}
      </TextField>
      <List
        sx={{
          width: '100%',
        }}
        component="nav"
      >
        {surveyLayers.map(surveyLayer => (
          <SurveyLayerItem
            key={`survey-layer-item-${surveyLayer.name}`}
            surveyLayer={surveyLayer}
            removeDisabled={surveyLayers.length <= 1}
            isTop={surveyLayer.name === topLayer?.name}
          />
        ))}
      </List>
    </Stack>

  );
}
