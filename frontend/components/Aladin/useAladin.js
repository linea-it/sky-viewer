'use client';
import A from 'aladin-lite';
import { useEffect, useRef, useCallback, useState } from 'react';

/**
 * Hook para controlar o Aladin Lite, aguardando a lib A estar disponível.
 */
export function useAladin(aladinParams = {}, userGroups = []) {
  const containerRef = useRef(null);
  const aladinRef = useRef(null);
  const [isReady, setIsReady] = useState(false);
  const catalogsRef = useRef({})

  useEffect(() => {
    let isCancelled = false;

    if (!containerRef.current) return;

    // Aguarda o carregamento completo da lib
    A.init.then(() => {
      if (isCancelled) return;

      aladinRef.current = A.aladin(containerRef.current, aladinParams);
      setIsReady(true);


      // Adiciona imagem do DES DR2 (pública)
      const des_dr2 = aladinRef.current.createImageSurvey(
        "DES_DR2_IRG_LIneA",
        "DES DR2 IRG at LIneA",
        "https://datasets.linea.org.br/data/releases/des/dr2/images/hips/",
        "equatorial",
      );
      aladinRef.current.setImageSurvey(des_dr2, {
        // imgFormat: 'hips',
        requestCredentials: 'include',
        requestMode: 'cors',
      });

      // Exemplo de catálogo HiPS (público)
      const des_dr2_cat = A.catalogHiPS(
        'https://datasets.linea.org.br/data/releases/des/dr2/catalogs/hips/',
        {
          name: 'DES DR2',
          onClick: 'showTable',
          color: '#33ff42',
        }
      );
      des_dr2_cat.hide()
      aladinRef.current.addCatalog(des_dr2_cat);
      catalogsRef.current['des_dr2'] = des_dr2_cat;

      // Adiciona HIPScat LSST DP0.2 (privado)
      const lsst_dp02_cat = A.catalogHiPS(
        'https://datasets.linea.org.br/data/releases/des/dr2/catalogs/hips/',
        {
          name: 'LSST DP0.2',
          onClick: 'showTable',
          color: '#2BC7EE',

        }
      );
      lsst_dp02_cat.hide()
      aladinRef.current.addCatalog(lsst_dp02_cat);
      catalogsRef.current['lsst_dp02'] = lsst_dp02_cat;

      // Verifica grupos para liberar acesso privado
      console.log("User groups:", userGroups);
      if (userGroups.includes('dp02')) {
        const lsst_dp02 = aladinRef.current.createImageSurvey(
          "LSST_DP02_IRG_LIneA",
          "LSST DP0.2 IRG at LIneA",
          "https://skyviewer-dev.linea.org.br/data/releases/lsst/dp02/images/hips/",
          "equatorial",
        );
        aladinRef.current.setImageSurvey(lsst_dp02, {
          // imgFormat: 'hips',
          requestCredentials: 'include',
          requestMode: 'cors',
        });
        console.log("LSST DP0.2 IRG HIPS IMAGE added");
      }

    });

    return () => {
      isCancelled = true;
      if (containerRef.current) {
        containerRef.current.innerHTML = '';
      }
      aladinRef.current = null;
      setIsReady(false);
    };
  }, [aladinParams]);

  // Métodos utilitários

  const setFoV = useCallback((fov) => {
    aladinRef.current?.setFov(fov);
  }, []);

  const setTarget = useCallback((target) => {
    aladinRef.current?.gotoObject(target);
  }, []);

  const setSurvey = useCallback((survey) => {
    aladinRef.current?.setImageSurvey(survey);
  }, []);

  const createSurvey = useCallback((id, name, url, frame = 'equatorial', options = {}) => {
    return aladinRef.current?.createImageSurvey(id, name, url, frame, options);
  }, []);

  const addCatalog = useCallback((catalog) => {
    aladinRef.current?.addCatalog(catalog);
  }, []);

  const toggleCatalogVisibility = useCallback((id, visible) => {
    const catalog = catalogsRef.current?.[id];
    if (!catalog) return;
    if (visible) {
      catalog.show();
      console.log(`Catalog ${id} is now visible`);
      console.log(catalog);
    } else {
      catalog.hide();
      console.log(`Catalog ${id} is now hidden`);
      console.log(catalog);
    }
  }, []);

  const addMarker = useCallback((ra, dec, options = {}) => {
    const overlay = aladinRef.current?.createOverlay();
    if (overlay) {
      overlay.addMarker(ra, dec, options);
      return overlay;
    }
    return null;
  }, []);

  return {
    containerRef,
    aladinRef,
    catalogsRef,
    isReady, // Importante: indica se o Aladin está pronto
    setFoV,
    setTarget,
    setSurvey,
    createSurvey,
    addCatalog,
    toggleCatalogVisibility,
    addMarker,
  };
}
