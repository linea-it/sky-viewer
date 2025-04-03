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

      this.aladin = A.aladin(`#${this.id}`, {
        // target: "239.306940 -47.5654074"
        target: "04 08 35.53 -37 06 27.6",
        fov: 0.5,
      })

      this.aladin.setImageSurvey(this.aladin.createImageSurvey(
        "LSST_DP02_IRG_LIneA",
        "LSST DP0.2 IRG at LIneA",
        "https://scienceserver-dev.linea.org.br/data/releases/lsst/dp02/hips/",
        "equatorial",
      ), { imgFormat: 'hips' })

      // https://aladin.cds.unistra.fr/AladinLite/doc/API/#image-layers
      // https://aladin.cds.unistra.fr/AladinLite/doc/API/
      this.aladin.setImageSurvey(this.aladin.createImageSurvey(
        "DES_DR2_IRG_LIneA",
        "DES DR2 IRG at LIneA",
        "https://scienceserver-dev.linea.org.br/secondary/images/coadd/hips_rgb/",
        "equatorial",

      ), { imgFormat: 'hips' })

      // DES DR2 Catalog HIPScat/HATS
      // https://aladin.cds.unistra.fr/AladinLite/doc/API/examples/catalog-hips-filter/
      // https://hipscat.cds.unistra.fr/HiPSCatService/I/345/gaia2/
      // https://aladin.cds.unistra.fr/AladinLite/doc/tutorials/interactive-finding-chart/
      var hips = A.catalogHiPS(
        'https://scienceserver-dev.linea.org.br/data/releases/dr2/catalogs/',
        {
          onClick: 'showTable',
          color: '#33ff42',
          name: 'DES DR2',
        });
      this.aladin.addCatalog(hips);
      // console.log(this.aladin)

      {/* aladin.setImageSurvey(
        aladin.createImageSurvey(
          "DSS blue band",
          "Color DSS blue HiPS",
          "http://alasky.cds.unistra.fr/DSS/DSS2-blue-XJ-S/",
          "equatorial",
          9,
          {imgFormat: 'fits'})
        ); // setting a custom HiPS */}

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
          </Box>
        )}
      </>
    )
  }
}
