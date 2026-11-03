'use client';

import { AladinProvider } from './AladinProvider';
import AladinViewer from './AladinViewer';
import Controls from '@/components/Aladin/Controls';
import LoadingBackdrop from './LoadingBackdrop';
export default function Aladin({ userGroups = [], baseHost, isDev }) {
  return (
    <AladinProvider
      // Aladin Lite options
      // See available options at:
      // https://cds-astro.github.io/aladin-lite/global.html#AladinOptions
      aladinParams={{
        fov: 360,
        // Sem survey padrão: o Aladin só adiciona uma camada depois que todas as anteriores
        // terminam de carregar, então a primeira imagem ficaria esperando o DSS do CDS.
        survey: [],
        // target: "04 08 35.53 -37 06 27.6", // Coordenadas DES. 
        // target: "12 26 53.27 +08 56 49.0",
        projection: "AIT",
        cooFrame: "ICRSd",
        showGotoControl: true,
        showFullscreenControl: true,
        showSimbadPointerControl: true,
        realFullscreen: true,
        showCooGridControl: true,
        showContextMenu: true,
        showSettingsControl: true,
        reticleColor: '#00ff04',
        selector: {
          color: '#00ff04' // Cor do campo de busca, OBS não funcionou por parametro a cor está hardcoded no css .aladin-input-text.search.
        }
      }}
      userGroups={userGroups}
      baseHost={baseHost}
      isDev={isDev}
    >
      <div style={{
        display: 'flex',
        height: '100%',
        width: '100%',
        position: 'relative',
      }}>
        <div style={{ flex: 1 }}>
          <AladinViewer />
        </div>
        <div style={{
          width: '300px',
          background: '#eee',
          overflowY: 'auto'
        }}>
          <Controls />
        </div>
        <LoadingBackdrop />
      </div>
    </AladinProvider>
  );
}
