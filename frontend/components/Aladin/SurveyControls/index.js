'use client';

import { useAladinContext } from '@/components/Aladin/AladinContext';
import { Typography } from '@mui/material';
import Stack from '@mui/material/Stack';
import List from '@mui/material/List';
import SurveyListItem from './SurveyListItem';

export default function SurveyControls() {
  const { surveysRef } = useAladinContext();

  return (
    <Stack spacing={2}>
      <Typography variant="h6">Images</Typography>
      <List
        sx={{
          width: '100%',
        }}
        component="nav"
      >
        {surveysRef.current && Object.keys(surveysRef.current).length > 0 && (
          Object.keys(surveysRef.current).map(key => {
            const survey = surveysRef.current[key];
            return (
              <SurveyListItem key={`survey-list-item-${survey.id}`} survey={survey} />
            )
          }))
        }
      </List>
    </Stack>

  );
}
