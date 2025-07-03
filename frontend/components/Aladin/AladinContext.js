'use client';

import { createContext, useContext } from 'react';

/**
 * Criação do contexto do Aladin.
 */
export const AladinContext = createContext({
  containerRef: null,
  aladinRef: null,
  catalogsRef: {},
  isReady: false,
  setFoV: () => { },
  setTarget: () => { },
  setSurvey: () => { },
  createSurvey: () => { },
  addCatalog: () => { },
  toggleCatalogVisibility: () => { },
  addMarker: () => { },
});

/**
 * Hook para acessar facilmente o contexto do Aladin.
 */
export function useAladinContext() {
  return useContext(AladinContext);
}
