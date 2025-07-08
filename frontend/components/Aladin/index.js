'use client';

import { AladinProvider } from './AladinProvider';
import AladinViewer from './AladinViewer';
import Controls from '@/components/Aladin/Controls';
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
        reticleColor: '#41b332'
      }}
      userGroups={userGroups}
    >
      <div style={{
        display: 'flex',
        height: '100%',
        width: '100%',
      }}>
        <div style={{ flex: 1 }}>
          <AladinViewer />
        </div>
        <div style={{
          width: '300px',
          background: '#eee'
        }}>
          <Controls />
        </div>
      </div>
    </AladinProvider>
  );
}
