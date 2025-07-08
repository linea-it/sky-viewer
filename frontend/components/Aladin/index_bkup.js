'use client';
import React from 'react'
import Box from '@mui/material/Box';
import A from 'aladin-lite'
// import { v4 } from "uuid";

// export default function Aladin() {

//   let aladin

//   useEffect(() => {
//     console.log("I have been mounted")
//     A.init.then(() => {
//       aladin = A.aladin('aladin-container', {
//         survey: 'P/allWISE/color', // set initial image survey
//         // survey: 'P/DSS2/color', // set initial image survey
//         projection: 'SIN', // set a projection
//         fov: 0.12, // initial field of view in degrees
//         // target: 'NGC 2175', // initial target
//         cooFrame: 'ICRS', // set galactic frame reticleColor: '#ff89ff', // change reticle color
//         showReticle: false,
//         showCooGrid: false,
//         fullScreen: false
//       })
//     })

//   }, [])

//   return (
//     <>
//       {typeof window !== "undefined" && (
//         <Box
//           id='aladin-container'
//           sx={{
//             backgroundColor: 'darkgray',
//             height: '100%',
//             width: '100%',
//           }}>
//         </Box>
//       )}
//     </>
//     // <Box
//     //   id='aladin-container'
//     //   sx={{
//     //     backgroundColor: 'darkgray',
//     //     height: '100%',
//     //     width: '100%',
//     //   }}>
//     // </Box>
//   );
// }
const target = "04 08 35.53 -37 06 27.6"
const fov = 1.5
const projection = "AIT"

const aladinParams = {
  fov: fov,
  target: target,
  showGotoControl: true,
  showFullscreenControl: true,
  showSimbadPointerControl: true,
  // showShareControl: true,
  realFullscreen: true,
  showCooGridControl: true,
  projection: projection,
  showContextMenu: true,
  showSettingsControl: true
};

export default class Aladin extends React.Component {
  // Importante tentei criar o componente com Hooks mas não funcionou corretamente.
  // Usando Class based Component funcionou como esperado.
  // Acredito que seja pelo comportamento do didmount.
  // Aladin precisa que a div já exista antes da função ser chamada.
  //
  // Exemplos que usei como base:
  // https://blog.logrocket.com/complete-guide-react-refs/
  // https://legacy.reactjs.org/docs/hooks-effect.html
  //

  // Aladin Lite Repository
  // https://github.com/cds-astro/aladin-lite/tree/master#

  // Complete List with all Options for Aladin
  // https://aladin.cds.unistra.fr/AladinLite/doc/API/
  // Lista de Exemplos:
  // https://aladin.cds.unistra.fr/AladinLite/doc/API/examples/

  constructor(props) {
    // console.log("Aladin - constructor")
    super(props)

    //    console.log("User Groups: ", props.userGroups)

    // Cria um ID unico para div que vai receber o aladin
    // this.id = `aladin-container-${v4()}`
    this.id = `aladin-container`

  }

  componentDidMount() {
    A.init.then(() => {
      // this.aladin = A.aladin(`#${this.id}`, {
      //   survey: 'P/allWISE/color', // set initial image survey
      //   // survey: 'P/DSS2/color', // set initial image survey
      //   projection: 'SIN', // set a projection
      //   fov: 0.12, // initial field of view in degrees
      //   // target: 'NGC 2175', // initial target
      //   cooFrame: 'ICRS', // set galactic frame reticleColor: '#ff89ff', // change reticle color
      //   showReticle: false,
      //   showCooGrid: false,
      //   fullScreen: false
      // })

      // this.aladin = A.aladin(`#${this.id}`, {
      //   // projection: 'SIN', // set a projection
      //   // fov: 0.12, // initial field of view in degrees
      //   // cooFrame: 'ICRS', // set galactic frame reticleColor: '#ff89ff', // change reticle color
      //   showReticle: false,
      //   showCooGrid: false,
      //   fullScreen: false
      // })

      // this.aladin = A.aladin(`#${this.id}`, {
      //   // target: "239.306940 -47.5654074"
      //   target: "04 08 35.53 -37 06 27.6",
      //   fov: 0.5,
      // })

      this.aladin = A.aladin(`#${this.id}`, aladinParams)


      // PUBLIC RELEASES
      // ----------------------------------------------------------
      // DES DR2 IRG HIPS IMAGE
      // https://aladin.cds.unistra.fr/AladinLite/doc/API/#image-layers
      // https://aladin.cds.unistra.fr/AladinLite/doc/API/
      // this.aladin.setImageSurvey(this.aladin.createImageSurvey(
      //   "DES_DR2_IRG_LIneA",
      //   "DES DR2 IRG at LIneA",
      //   "https://datasets.linea.org.br/data/releases/des/dr2/images/hips/",
      //   "equatorial",
      // ), { imgFormat: 'hips', requestCredentials: 'include', requestMode: 'cors' })

      const des_dr2 = this.aladin.createImageSurvey(
        "DES_DR2_IRG_LIneA",
        "DES DR2 IRG at LIneA",
        "https://datasets.linea.org.br/data/releases/des/dr2/images/hips/",
        "equatorial",
      )
      this.aladin.setImageSurvey(des_dr2, { imgFormat: 'hips', requestCredentials: 'include', requestMode: 'cors' })


      // Adiciona a imagem mas não seleciona como imagem principal
      // this.aladin.addNewImageLayer(A.HiPS(
      //   "https://datasets.linea.org.br/data/releases/des/dr2/images/hips/",
      //   {
      //     name: "DES DR2 IRG at LIneA",
      //     imgFormat: 'jpg',
      //     requestCredentials: 'include',
      //     requestMode: 'cors'
      //     // "DES DR2 IRG at LIneA",
      //     // "equatorial",
      //   }
      // ))

      // this.aladin.setImageSurvey(this.aladin.createImageSurvey(
      //   "DES_DR2_IRG_LIneA",
      //   "DES DR2 Teste Credentials",
      //   "https://skyviewer-dev.linea.org.br/data/releases/des/dr2/images/hips/",
      //   "equatorial",
      // ), { imgFormat: 'hips', requestCredentials: 'include', requestMode: 'cors' })

      //  PUBLIC CATALOGS
      // ----------------------------------------------------------
      // DES DR2 Catalog HIPScat/HATS
      // https://aladin.cds.unistra.fr/AladinLite/doc/API/examples/catalog-hips-filter/
      // https://hipscat.cds.unistra.fr/HiPSCatService/I/345/gaia2/
      // https://aladin.cds.unistra.fr/AladinLite/doc/tutorials/interactive-finding-chart/
      var hips = A.catalogHiPS(
        'https://datasets.linea.org.br/data/releases/des/dr2/catalogs/hips/',
        {
          onClick: 'showTable',
          color: '#33ff42',
          name: 'DES DR2',
        });
      // this.aladin.addCatalog(hips);



      // PRIVATE RELEASES
      // ----------------------------------------------------------

      // LSST DP0.2 IRG HIPS IMAGE
      if (Array.isArray(this.props.userGroups) && this.props.userGroups.includes('dp02')) {
        // "https://datasets.linea.org.br/data/releases/lsst/dp02/images/hips/",
        // this.aladin.setImageSurvey(this.aladin.createImageSurvey(
        //   "LSST_DP02_IRG_LIneA",
        //   "LSST DP0.2 IRG at LIneA",
        //   "https://skyviewer-dev.linea.org.br/data/releases/lsst/dp02/images/hips/",
        //   "equatorial",
        // ), { imgFormat: 'hips', requestCredentials: 'include', requestMode: 'cors' })
        const lsst_dp02 = this.aladin.createImageSurvey(
          "LSST_DP02_IRG_LIneA",
          "LSST DP0.2 IRG at LIneA",
          "https://skyviewer-dev.linea.org.br/data/releases/lsst/dp02/images/hips/",
          "equatorial",
        )
        this.aladin.setImageSurvey(lsst_dp02, { imgFormat: 'hips', requestCredentials: 'include', requestMode: 'cors' })
        console.log("LSST DP0.2 IRG HIPS IMAGE added")
      }


      // console.log(this.aladin)

      // var hips = A.catalogHiPS(
      //   'https://scienceserver-dev.linea.org.br/data/releases/lsst/dp02/catalogs/',
      //   {
      //     onClick: 'showTable',
      //     color: '#8e44ad',
      //     name: 'LSST DP0.2',
      //   });
      // this.aladin.addCatalog(hips);

      //   // // Cria um catalogo com um unico source
      //   // this.drawCatalog()
      //   // // Centraliza a imagem na posição
      //   // this.goToPosition(this.props.ra, this.props.dec)

    })
  }

  componentWillUnmount() { }


  render() {
    return (
      <>
        {typeof window !== "undefined" && (
          <Box
            id={this.id}
            sx={{
              backgroundColor: 'darkgray',
              height: '100%',
              width: '100%',
            }}>
            <div class="aladin-tooltip-container aladin-share-control top">
              <button class="aladin-btn aladin-dark-theme medium-sized-icon" style={{ backgroundPosition: 'center center', cursor: 'pointer' }}>
                <div class="aladin-icon-monochrome aladin-icon aladin-dark-theme">
                  {/* <img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCFET0NUWVBFIHN2ZyBQVUJMSUMgIi0vL1czQy8vRFREIFNWRyAxLjEvL0VOIiAiaHR0cDovL3d3dy53My5vcmcvR3JhcGhpY3MvU1ZHLzEuMS9EVEQvc3ZnMTEuZHRkIj4NCjwhLS0gVXBsb2FkZWQgdG86IFNWRyBSZXBvLCB3d3cuc3ZncmVwby5jb20sIEdlbmVyYXRvcjogU1ZHIFJlcG8gTWl4ZXIgVG9vbHMgLS0+DQo8c3ZnIGZpbGw9IiMwMDAwMDAiICB2ZXJzaW9uPSIxLjEiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiICB3aWR0aD0iODAwcHgiDQoJIGhlaWdodD0iODAwcHgiIHZpZXdCb3g9IjAgMCA1MTIgNTEyIiB4bWw6c3BhY2U9InByZXNlcnZlIj4NCg0KPGcgaWQ9Ijc5MzVlYzk1YzQyMWNlZTZkODZlYjIyZWNkMTJiNWJiIj4NCg0KPHBhdGggc3R5bGU9ImRpc3BsYXk6IGlubGluZTsiIGQ9Ik01MDUuNzA1LDQyMS44NTFjMCw0OS41MjgtNDAuMTQ2LDg5LjY0OS04OS42MzcsODkuNjQ5Yy00OS41MjcsMC04OS42NjItNDAuMTIxLTg5LjY2Mi04OS42NDkNCgkJYzAtMS42MjIsMC4xNDgtMy4yMDYsMC4yMzYtNC44MTVsLTE3Ny40NjQtOTAuNDc0Yy0xNC44ODMsMTEuMDI4LTMzLjI3MiwxNy42NDEtNTMuMjIxLDE3LjY0MQ0KCQljLTQ5LjUyOCwwLTg5LjY2Mi00MC4xMzQtODkuNjYyLTg5LjY0OXM0MC4xMzQtODkuNjQ5LDg5LjY2Mi04OS42NDljMjIuMTY5LDAsNDIuNDI5LDguMDk3LDU4LjA4NiwyMS40MzNsMTcyLjc3NC04OC4wOQ0KCQljLTAuMjUtMi42ODItMC40MTItNS4zNjQtMC40MTItOC4wOTdjMC00OS41MDMsNDAuMTM1LTg5LjY0OSw4OS42NjItODkuNjQ5YzQ5LjQ5LDAsODkuNjM3LDQwLjE0Niw4OS42MzcsODkuNjQ5DQoJCWMwLDQ5LjUxNi00MC4xNDYsODkuNjUtODkuNjM3LDg5LjY1Yy0yMi4wODIsMC00Mi4yNDItOC4wMDktNTcuODYxLTIxLjIyMWwtMTcyLjk5OSw4OC4yMTVjMC4yMjQsMi41NTgsMC4zODcsNS4xNCwwLjM4Nyw3Ljc2DQoJCWMwLDQuNjUzLTAuNDc0LDkuMTgyLTEuMTQ4LDEzLjY0OGwxNzEuMzg5LDg3LjM3OWMxNS45Mi0xNC40NzIsMzcuMDA0LTIzLjM3OSw2MC4yMzItMjMuMzc5DQoJCUM0NjUuNTU5LDMzMi4yMDEsNTA1LjcwNSwzNzIuMzQ4LDUwNS43MDUsNDIxLjg1MXoiPg0KDQo8L3BhdGg+DQoNCjwvZz4NCg0KPC9zdmc+" /> */}
                  <img src="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' version='1.1' width='20' height='20' viewBox='0 0 64 64'%3E%3Cpath d='M20 8h-8c-2.2 0-4 1.8-4 4v8c0 2.2 1.8 4 4 4h8c2.2 0 4-1.8 4-4v-8c0-2.2-1.8-4-4-4z' fill='%23505050'/%3E%3Cpath d='M44 24h8c2.2 0 4-1.8 4-4v-8c0-2.2-1.8-4-4-4h-8c-2.2 0-4 1.8-4 4v8c0 2.2 1.8 4 4 4zM44 12h8v8h-8v-8z' fill='%23505050'/%3E%3Cpath d='M20 40h-8c-2.2 0-4 1.8-4 4v8c0 2.2 1.8 4 4 4h8c2.2 0 4-1.8 4-4v-8c0-2.2-1.8-4-4-4zM20 52h-8v-8h8v8z' fill='%23505050'/%3E%3Cpath d='M52 40h-8c-2.2 0-4 1.8-4 4v8c0 2.2 1.8 4 4 4h8c2.2 0 4-1.8 4-4v-8c0-2.2-1.8-4-4-4z' fill='%23505050'/%3E%3Cpath d='M56 32h-4c-5.342 0-10.365-2.080-14.142-5.858s-5.858-8.8-5.858-14.142v-4c0-4.4-3.6-8-8-8h-16c-4.4 0-8 3.6-8 8v16c0 4.4 3.6 8 8 8h4c5.342 0 10.365 2.080 14.142 5.858s5.858 8.8 5.858 14.142v4c0 4.4 3.6 8 8 8h16c4.4 0 8-3.6 8-8v-16c0-4.4-3.6-8-8-8zM60 56c0 1.060-0.419 2.062-1.178 2.822s-1.762 1.178-2.822 1.178h-16c-1.060 0-2.062-0.419-2.822-1.178s-1.178-1.762-1.178-2.822v-4c0-13.255-10.745-24-24-24h-4c-1.060 0-2.062-0.419-2.822-1.178s-1.178-1.762-1.178-2.822v-16c0-1.060 0.418-2.062 1.178-2.822s1.762-1.178 2.822-1.178h16c1.060 0 2.062 0.418 2.822 1.178s1.178 1.762 1.178 2.822v4c0 13.255 10.745 24 24 24h4c1.060 0 2.062 0.419 2.822 1.178s1.178 1.762 1.178 2.822v16z' fill='%23505050'/%3E%3C/svg%3E" />

                </div>
              </button>
              <span class="aladin-tooltip aladin-dark-theme">
                <div>You can share/export your view into many ways</div>
              </span>
            </div>
          </Box>
        )}
      </>
    )
  }
}
