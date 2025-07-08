import React from 'react';
import PropTypes from 'prop-types'
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import CheckIcon from '@mui/icons-material/Check';

import { useAladinContext } from '@/components/Aladin/AladinContext';

export default function SurveyListItem({ survey }) {
  const { setImageSurvey, currentSurveyId } = useAladinContext();

  const handleToggle = (event) => {
    setImageSurvey(survey.id);
  };

  return (
    <React.Fragment>
      <ListItem
        key={`survey-option-${survey.id}`}
        disablePadding
      >
        <ListItemButton
          selected={currentSurveyId === survey.id}
          onClick={handleToggle}
        >
          <ListItemIcon>
            {currentSurveyId === survey.id && (<CheckIcon />)}
          </ListItemIcon>
          <ListItemText primary={survey.name} />
        </ListItemButton>
      </ListItem>

    </React.Fragment>
  )
}

SurveyListItem.propTypes = {
  survey: PropTypes.object.isRequired,
}
