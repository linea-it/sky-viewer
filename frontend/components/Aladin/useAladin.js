'use client';
import A from 'aladin-lite';
import { useEffect, useRef, useCallback, useState } from 'react';

// Imagem base pública, disponível para todos. Mesmo default do Canvas.
export const DEFAULT_SURVEY_ID = 'DES_DR2_IRG_LIneA';

// Imagem base inicial: a primeira que o usuário pode acessar, das mais recentes para as mais antigas.
const INITIAL_SURVEY_PRIORITY = [
  'LSST_DP2_IRG_LIneA',
  'LSST_DP1_IRG_LIneA',
  'LSST_DP02_IRG_LIneA',
  DEFAULT_SURVEY_ID,
];

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
  const imageLayersRef = useRef({}) // { [layerName]: { layer, opacity, visible } } - surveys e mapas
  const lastBaseSurveyIdRef = useRef(null)
  const [currentSurveyId, setCurrentSurveyId] = useState(null);

  const surveys = [
    // Adiciona imagem do LSST DP2 (privada, requer grupo 'lsst_dp2')
    {
      id: "LSST_DP2_IRG_LIneA",
      name: "LSST DP2 IRG at LIneA",
      url: `${baseHost}/data/releases/lsst/dp2/images/hips`,
      cooFrame: "equatorial",
      options: {
        requestCredentials: 'include',
        requestMode: 'cors',
      },
      requireGroup: 'lsst_dp2', // Grupo necessário para acesso
      devOnly: false,
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
    // Rubin First Look (pública)
    {
      id: "RUBIN_FIRST_LOOK_UGRI",
      name: "RUBIN FIRST LOOK",
      url: "https://images.rubinobservatory.org/hips/asteroids/color_ugri/",
      cooFrame: "equatorial",
      options: {},
    },
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
    }
  ]

  // catálogos HiPScat
  const catalogs = [
    {
      id: 'lsst_dp2',
      name: 'LSST DP2',
      url: `${baseHost}/data/releases/lsst/dp2/catalogs/hips`,
      options: {
        color: '#f524e3',
        requestCredentials: 'include',
        requestMode: 'cors',
      },
      requireGroup: 'lsst_dp2',
      devOnly: false,
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
    "LSST_DP2_IRG_LIneA": "19 15 07.18 -20 23 51.4",
  }

  useEffect(() => {
    let isCancelled = false;

    if (!containerRef.current) return;

    // Aguarda o carregamento completo da lib
    A.init.then(() => {
      if (isCancelled) return;

      const aladin = A.aladin(containerRef.current, aladinParams);
      aladinRef.current = aladin;

      const loadInitialSurvey = () => {
        const surveyId = INITIAL_SURVEY_PRIORITY.find(id => surveysRef.current[id]);
        if (surveyId) applyLayer('base', surveyId, 1);
      };

      // O Aladin tira do hipsCache o survey que falha, então ele também sai do select.
      // Se ainda não há imagem base, tenta a próxima da lista de prioridade.
      const handleSurveyError = (surveyId) => {
        if (aladinRef.current !== aladin) return;
        delete surveysRef.current[surveyId];
        if (lastBaseSurveyIdRef.current === null) loadInitialSurvey();
      };

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
          // Só centraliza na primeira imagem base: depois disso a visão não se move
          // (o evento também dispara ao trocar a base e ao adicionar overlays)
          if (lastBaseSurveyIdRef.current === null) {
            lastBaseSurveyIdRef.current = currentSurvey.id;
            const target = defaultTargets[currentSurvey.id];
            if (target) {
              // Goto the target of the current survey
              aladinRef.current.gotoObject(target);
            }
          }
          // Indica que o Aladin e a primeira imagem estão prontos
          setIsReady(true);
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

        const hips_survey = aladinRef.current.createImageSurvey(survey.id, survey.name, survey.url, survey.cooFrame, undefined, {
          errorCallback: () => handleSurveyError(survey.id),
        });

        // Só registra (hipsCache + menu nativo), sem trocar a imagem base.
        aladinRef.current.addHiPSToFavorites(hips_survey);

        surveysRef.current[survey.id] = hips_survey;
        // console.log(`${survey.name} HIPS IMAGE added`);
      })

      // Uma única troca de imagem base no init, independente da ordem do array.
      loadInitialSurvey();

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
      imageLayersRef.current = {};
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

  // Estado da layer; layers que entraram pelo menu nativo são registradas no primeiro uso
  const getLayerEntry = useCallback((layerName) => {
    if (!imageLayersRef.current[layerName]) {
      const layer = aladinRef.current?.getOverlayImageLayer(layerName);
      if (!layer) return null;
      imageLayersRef.current[layerName] = { layer, opacity: layer.getOpacity(), visible: true };
    }
    return imageLayersRef.current[layerName];
  }, []);

  // Aplica/substitui a layer. Mesmo layer name => substitui no mesmo lugar da pilha.
  // Mesmo comportamento do botão "+ Surveys" nativo: adiciona uma nova layer de imagem
  const applyLayer = useCallback((layerName, hipsOrId, opacity, visible = true) => {
    const layer = aladinRef.current.setOverlayImageLayer(hipsOrId, layerName);
    layer.setOpacity(visible ? opacity : 0);
    imageLayersRef.current[layerName] = { layer, opacity, visible };
    return layer;
  }, []);

  const setLayerOpacity = useCallback((layerName, opacity) => {
    const entry = getLayerEntry(layerName);
    if (!entry) return;
    entry.opacity = opacity;
    if (entry.visible) entry.layer.setOpacity(opacity);
  }, [getLayerEntry]);

  // Mostra/oculta a layer sem perder a opacidade (opacity 0 <-> valor guardado)
  const setLayerVisibility = useCallback((layerName, visible) => {
    const entry = getLayerEntry(layerName);
    if (!entry) return;
    entry.visible = visible;
    entry.layer.setOpacity(visible ? entry.opacity : 0);
  }, [getLayerEntry]);

  // Remove a layer (equivalente ao botão de lixeira do Aladin)
  const removeLayer = useCallback((layerName) => {
    aladinRef.current?.removeImageLayer(layerName);
    delete imageLayersRef.current[layerName];
  }, []);

  // Aplica/substitui o overlay de mapa do survey; sempre torna o mapa visível.
  const setMapOverlay = useCallback((surveyKey, mapId, opacity = 0.8) => {
    if (!aladinRef.current) return;
    const hips_map = mapSurveysRef.current[surveyKey]?.[mapId];
    if (!hips_map) return;
    // Mesmo layer name => substitui o overlay anterior deste survey
    applyLayer(`map-${surveyKey}`, hips_map, opacity);
  }, [applyLayer]);

  const setMapOpacity = useCallback((surveyKey, opacity) => {
    setLayerOpacity(`map-${surveyKey}`, opacity);
  }, [setLayerOpacity]);

  const setMapVisibility = useCallback((surveyKey, visible) => {
    setLayerVisibility(`map-${surveyKey}`, visible);
  }, [setLayerVisibility]);

  const removeMapOverlay = useCallback((surveyKey) => {
    removeLayer(`map-${surveyKey}`);
  }, [removeLayer]);

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
