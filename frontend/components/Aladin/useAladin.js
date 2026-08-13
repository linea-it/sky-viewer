'use client';
import A from 'aladin-lite';
import { useEffect, useRef, useCallback, useState } from 'react';

/**
 * Hook para controlar o Aladin Lite, aguardando a lib A estar disponível.
 */
export function useAladin(aladinParams = {}, userGroups = [], baseHost, isDev) {
  const containerRef = useRef(null);
  const aladinRef = useRef(null);
  const [isReady, setIsReady] = useState(false);
  const surveysRef = useRef({})
  const catalogsRef = useRef({})
  const mapSurveysRef = useRef({}) // { [surveyKey]: { [mapId]: hips } } - surveys de mapa pré-registrados
  const mapLayersRef = useRef({}) // { [surveyKey]: { mapId, hips, opacity, visible } }
  const lastBaseSurveyIdRef = useRef(null)
  const [currentSurveyId, setCurrentSurveyId] = useState(null);

  const surveys = [
    // Adiciona imagem do DES DR2 (pública)
    {
      id: "DES_DR2_IRG_LIneA",
      name: "DES DR2 IRG at LIneA",
      url: "https://datasets.linea.org.br/data/releases/des/dr2/images/hips/",
      cooFrame: "equatorial",
      // HipsOptions: https://cds-astro.github.io/aladin-lite/global.html#HiPSOptions
      options: {
        requestCredentials: 'include',
        requestMode: 'cors',
      },
    },
    // Adiciona imagem do LSST DP0.2 (privada, requer grupo 'dp02')
    {
      id: "LSST_DP02_IRG_LIneA",
      name: "LSST DP0.2 IRG at LIneA",
      url: `${baseHost}/data/releases/lsst/dp02/images/hips/`,
      cooFrame: "equatorial",
      options: {
        requestCredentials: 'include',
        requestMode: 'cors',
      },
      requireGroup: 'lsst_dp0.2', // Grupo necessário para acesso
    },
    // Adiciona imagem do LSST DP1 (privada, requer grupo 'lsst_dp1')
    {
      id: "LSST_DP1_IRG_LIneA",
      name: "LSST DP1 IRG at LIneA",
      url: `${baseHost}/data/releases/lsst/dp1/images/hips`,
      cooFrame: "equatorial",
      options: {
        requestCredentials: 'include',
        requestMode: 'cors',
      },
      requireGroup: 'lsst_dp1', // Grupo necessário para acesso
      devOnly: false,
    },
    // Rubin First Look (pública)
    {
      id: "RUBIN_FIRST_LOOK_UGRI",
      name: "RUBIN FIRST LOOK",
      url: "https://images.rubinobservatory.org/hips/asteroids/color_ugri/",
      cooFrame: "equatorial",
      options: {},
    }]

  // catálogos HiPScat
  const catalogs = [
    // Adiciona catálogo DES DR2 (público)
    {
      id: 'des_dr2',
      name: 'DES DR2 at LIneA',
      url: 'https://datasets.linea.org.br/data/releases/des/dr2/catalogs/hips/',
      options: { color: '#33ff42' }
    },
    {
      id: 'des_dr2_teste',
      name: 'DES DR2 SAMPLE TEST',
      url: `${baseHost}/data/releases/des/dr2/images/hips/`,
      options: {
        color: '#2BC7EE',
        requestCredentials: 'include',
        requestMode: 'cors',
      },
      devOnly: true,
    },
    {
      id: 'lsst_dp1',
      name: 'LSST DP1',
      url: `${baseHost}/data/releases/lsst/dp1/catalogs/hips`,
      options: {
        color: '#2BC7EE',
        requestCredentials: 'include',
        requestMode: 'cors',
      },
      requireGroup: 'lsst_dp1',
      devOnly: false,
    },
    // // Adiciona catálogo LSST DP0.2 (privado)
    // {
    //   id: 'lsst_dp02',
    //   name: 'LSST DP0.2 at LIneA',
    //   url: 'https://datasets.linea.org.br/data/releases/des/dr2/catalogs/hips/', // TODO: Url temporaria, deve ser alterada para o catálogo correto
    //   options: { color: '#2BC7EE' },
    //   requireGroup: 'lsst_dp0.2', // Grupo necessário para acesso
    // },
    // Adiciona Catalogos default do Aladin ( Simbad, Gaia DR3, 2MASS )
    {
      id: 'simbad',
      name: 'SIMBAD',
      url: 'https://hipscat.cds.unistra.fr/HiPSCatService/SIMBAD',
      options: {
        shape: 'circle', sourceSize: 8, color: '#318d80'
      }
    },
    {
      id: 'gaia-dr3',
      name: 'Gaia DR3',
      url: 'https://hipscat.cds.unistra.fr/HiPSCatService/I/355/gaiadr3',
      options: { shape: 'square', sourceSize: 8, color: '#6baed6' }
    },
    {
      id: '2mass',
      name: '2MASS',
      url: 'https://hipscat.cds.unistra.fr/HiPSCatService/II/246/out',
      options: { shape: 'plus', sourceSize: 8, color: '#dd2233' }
    }
  ]

  // Mapas HiPS (overlays), agrupados por survey
  // Atenção: não usar requestCredentials 'include' aqui - o servidor responde
  // Access-Control-Allow-Origin: * e o navegador bloqueia requests com credenciais
  const maps = [
    // Mapas sistemáticos do DES DR2 (públicos)
    {
      surveyKey: 'des_dr2', // layer name do overlay: `map-des_dr2`
      name: 'DES DR2',
      // TODO: o properties destes mapas declara hips_frame=galactic e a imagem fica
      // desalinhada com o survey - provável problema na geração do HiPS no servidor
      cooFrame: "equatorial",
      categories: [
        {
          id: 'frac_detection',
          label: 'Fracdet',
          baseUrl: 'https://datasets.linea.org.br/data/releases/des/dr2/maps/systematic_maps/frac_detection',
          bands: [
            { value: 'g', label: 'g' },
            { value: 'r', label: 'r' },
            { value: 'i', label: 'i' },
            { value: 'z', label: 'z' },
            { value: 'y', label: 'Y' },
          ],
        },
      ],
    },
  ]

  // Surveys com mapas disponíveis para o usuário atual
  const mapsList = maps.filter(m =>
    (!m.requireGroup || userGroups.includes(m.requireGroup)) &&
    (m.devOnly !== true || isDev === true)
  );

  const defaultTargets = {
    "DES_DR2_IRG_LIneA": "02 32 44.09 -35 57 39.5",
    "RUBIN_FIRST_LOOK_UGRI": "12 26 53.27 +08 56 49.0",
    "LSST_DP02_IRG_LIneA": "04 08 29.07 -37 02 47.9",
    "LSST_DP1_IRG_LIneA": "02 39 35.55 -34 30 38.3",
  }

  useEffect(() => {
    let isCancelled = false;

    if (!containerRef.current) return;

    // Aguarda o carregamento completo da lib
    A.init.then(() => {
      if (isCancelled) return;

      aladinRef.current = A.aladin(containerRef.current, aladinParams);
      setIsReady(true);

      // aladinRef.current.addListener('AL:zoom.changed', function (e) { console.log('Zoom changed', e); });
      // aladinRef.current.addListener('AL:HiPSLayer.added', function (e) { console.log('Hips added', e); });
      // aladinRef.current.addListener('AL:HiPSLayer.changed', function (e) { console.log('Hips changed', e); });
      // aladinRef.current.addListener('AL:HiPSLayer.swap', function (e) { console.log('Hips swaped', e); });

      // Evento disparado toda vez que uma imagem HIPS é selecionada ou alterada
      aladinRef.current.addListener('AL:HiPSLayer.added', () => {
        // console.log('Survey changed');
        const currentSurvey = aladinRef.current.getBaseImageLayer();
        if (currentSurvey) {
          setCurrentSurveyId(currentSurvey.id);
          // Só re-centraliza quando a imagem base mudou de fato
          // (o evento também dispara ao adicionar overlays, ex: mapas)
          if (currentSurvey.id !== lastBaseSurveyIdRef.current) {
            lastBaseSurveyIdRef.current = currentSurvey.id;
            const target = defaultTargets[currentSurvey.id];
            if (target) {
              // Goto the target of the current survey
              aladinRef.current.gotoObject(target);
            }
          }
        }
      });


      // Adiciona as imagens HIPS
      surveys.forEach(survey => {

        // Verifica se o usuário tem acesso ao survey
        if (survey.requireGroup && !userGroups.includes(survey.requireGroup)) {
          // console.warn(`User does not have access to survey: ${survey.name}`);
          return; // Não adiciona o survey se o usuário não tiver acesso
        }

        if (survey.devOnly == true && isDev == false) {
          // console.warn(`Survey ${survey.name} is only available in dev mode.`);
          return; // Não adiciona o survey se não estiver em modo dev
        }

        const hips_survey = aladinRef.current.createImageSurvey(survey.id, survey.name, survey.url, survey.cooFrame);

        aladinRef.current.setImageSurvey(hips_survey, survey.options || {});

        surveysRef.current[survey.id] = hips_survey;
        // console.log(`${survey.name} HIPS IMAGE added`);
      })

      // Adiciona os catálogos HiPScat
      catalogs.forEach(cat => {
        if (cat.requireGroup && !userGroups.includes(cat.requireGroup)) {
          // console.warn(`User does not have access to catalog: ${cat.name}`);
          return; // Não adiciona o catálogo se o usuário não tiver acesso
        }

        if (cat.devOnly == true && isDev == false) {
          // console.warn(`Survey ${survey.name} is only available in dev mode.`);
          return; // Não adiciona o survey se não estiver em modo dev
        }

        const hips_cat = A.catalogHiPS(cat.url, {
          name: cat.name,
          onClick: 'showTable',
          ...cat.options,
        });

        hips_cat.hide(); // Esconde o catálogo inicialmente
        aladinRef.current.addCatalog(hips_cat);
        catalogsRef.current[cat.id] = hips_cat;
        // console.log(`${cat.name} HiPS catalog added`);
      })

      // Pré-registra os mapas HiPS no cache do Aladin, como é feito com os surveys.
      // Assim eles ficam disponíveis no menu nativo de surveys do Aladin.
      maps.forEach(mapGroup => {
        if (mapGroup.requireGroup && !userGroups.includes(mapGroup.requireGroup)) {
          return; // Não adiciona os mapas se o usuário não tiver acesso
        }

        if (mapGroup.devOnly == true && isDev == false) {
          return; // Não adiciona os mapas se não estiver em modo dev
        }

        mapSurveysRef.current[mapGroup.surveyKey] = {};
        mapGroup.categories.forEach(cat => {
          cat.bands.forEach(band => {
            const mapId = `${cat.id}_${band.value}`;
            const hips_map = aladinRef.current.createImageSurvey(
              `${mapGroup.surveyKey}_${mapId}`,
              `${mapGroup.name} ${cat.label} ${band.label}`,
              `${cat.baseUrl}/hips_${band.value}/`,
              mapGroup.cooFrame
            );
            mapSurveysRef.current[mapGroup.surveyKey][mapId] = hips_map;
            // console.log(`${mapGroup.name} ${cat.label} ${band.label} HIPS MAP added`);
          });
        });
      })
    });

    return () => {
      isCancelled = true;
      if (containerRef.current) {
        containerRef.current.innerHTML = '';
      }
      aladinRef.current = null;
      mapSurveysRef.current = {};
      mapLayersRef.current = {};
      lastBaseSurveyIdRef.current = null;
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

  const setImageSurvey = useCallback((survey) => {
    aladinRef.current?.setImageSurvey(survey);
  }, []);

  const toggleCatalogVisibility = useCallback((id, visible) => {
    const catalog = catalogsRef.current?.[id];
    if (!catalog) return;
    if (visible) {
      catalog.show();
    } else {
      catalog.hide();
    }
  }, []);

  // Aplica/substitui o overlay de mapa do survey; sempre torna o mapa visível.
  // Mesmo comportamento do botão "+ Surveys" nativo: adiciona uma nova layer de imagem
  const setMapOverlay = useCallback((surveyKey, mapId, opacity = 0.8) => {
    if (!aladinRef.current) return;
    const hips_map = mapSurveysRef.current[surveyKey]?.[mapId];
    if (!hips_map) return;
    // Mesmo layer name => substitui o overlay anterior deste survey
    const layer = aladinRef.current.setOverlayImageLayer(hips_map, `map-${surveyKey}`);
    layer.setOpacity(opacity);
    mapLayersRef.current[surveyKey] = { mapId, hips: layer, opacity, visible: true };
  }, []);

  const setMapOpacity = useCallback((surveyKey, opacity) => {
    const layer = mapLayersRef.current[surveyKey];
    if (!layer) return;
    layer.opacity = opacity;
    if (layer.visible) layer.hips.setOpacity(opacity);
  }, []);

  // Mostra/oculta o mapa sem perder banda nem opacidade (opacity 0 <-> valor guardado)
  const setMapVisibility = useCallback((surveyKey, visible) => {
    const layer = mapLayersRef.current[surveyKey];
    if (!layer) return;
    layer.visible = visible;
    layer.hips.setOpacity(visible ? layer.opacity : 0);
  }, []);

  // Remove a layer de mapa do survey (equivalente ao botão de lixeira do Aladin)
  const removeMapOverlay = useCallback((surveyKey) => {
    if (!aladinRef.current) return;
    if (!mapLayersRef.current[surveyKey]) return;
    aladinRef.current.removeImageLayer(`map-${surveyKey}`);
    delete mapLayersRef.current[surveyKey];
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
    surveysRef,
    catalogsRef,
    isReady, // Importante: indica se o Aladin está pronto
    currentSurveyId, // ID do survey atual
    setFoV,
    setTarget,
    setImageSurvey,
    toggleCatalogVisibility,
    addMarker,
    mapsList, // Surveys com mapas disponíveis
    setMapOverlay,
    setMapOpacity,
    setMapVisibility,
    removeMapOverlay,
  };
}
