'use client';

import { AladinProvider } from './AladinProvider';
import AladinViewer from './AladinViewer';
import CatalogControls from './CatalogControls';

export default function Aladin({ userGroups = [] }) {
  return (
    <AladinProvider
      aladinParams={{
        fov: 1.5,
        target: "04 08 35.53 -37 06 27.6",
        projection: "AIT",
        showGotoControl: true,
        showFullscreenControl: true,
        showSimbadPointerControl: true,
        realFullscreen: true,
        showCooGridControl: true,
        showContextMenu: true,
        showSettingsControl: true,
      }}
      userGroups={userGroups}
    >
      <div style={{ display: 'flex', height: '100vh' }}>
        <div style={{ flex: 1 }}>
          <AladinViewer />
        </div>
        <div style={{ width: '300px', padding: '1rem', background: '#eee' }}>
          <CatalogControls />
        </div>
      </div>

    </AladinProvider>
  );
}












// 'use client';

// import { useEffect } from 'react';
// import Box from '@mui/material/Box';
// import { useAladin } from './useAladin';

// export default function AladinViewer({ userGroups = [] }) {
//   const {
//     containerRef,
//     createSurvey,
//     setSurvey,
//     isReady,
//   } = useAladin({
//     fov: 1.5,
//     target: "04 08 35.53 -37 06 27.6",
//     projection: "AIT",
//     showGotoControl: true,
//     showFullscreenControl: true,
//     showSimbadPointerControl: true,
//     realFullscreen: true,
//     showCooGridControl: true,
//     showContextMenu: true,
//     showSettingsControl: true,
//   });

//   useEffect(() => {
//     if (!isReady) return;

//     const des_dr2 = createSurvey(
//       "DES_DR2_IRG_LIneA",
//       "DES DR2 IRG at LIneA",
//       "https://datasets.linea.org.br/data/releases/des/dr2/images/hips/",
//       "equatorial"
//     );
//     setSurvey(des_dr2);

//     if (userGroups.includes('dp02')) {
//       const lsst_dp02 = createSurvey(
//         "LSST_DP02_IRG_LIneA",
//         "LSST DP0.2 IRG at LIneA",
//         "https://skyviewer-dev.linea.org.br/data/releases/lsst/dp02/images/hips/",
//         "equatorial"
//       );
//       setSurvey(lsst_dp02);
//     }
//   }, [isReady, userGroups, createSurvey, setSurvey]);

//   return (
//     <Box
//       ref={containerRef}
//       sx={{
//         backgroundColor: 'darkgray',
//         height: '100%',
//         width: '100%',
//       }}
//     />
//   );
// }
