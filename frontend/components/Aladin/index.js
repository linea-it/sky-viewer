'use client';
import React, { useEffect } from 'react'
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import A from 'aladin-lite'

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
  // Complete List with all Options for Aladin
  // https://aladin.cds.unistra.fr/AladinLite/doc/API/
  // Lista de Exemplos:
  // https://aladin.cds.unistra.fr/AladinLite/doc/API/examples/

  constructor(props) {
    console.log("Aladin - constructor")
    super(props)

    // Cria um ID unico para div que vai receber o aladin
    // this.id = `aladin-container-${uuidv4()}`
    this.id = `aladin-container`

    // // Instancia do Aladin linkado com a div
    // this.aladin = undefined

    // // Verificar se a lib Aladin esta disponivel
    // this.libA = A
  }

  componentDidMount() {
    A.init.then(() => {
      this.aladin = A.aladin(`#${this.id}`, {
        survey: 'P/allWISE/color', // set initial image survey
        // survey: 'P/DSS2/color', // set initial image survey
        projection: 'SIN', // set a projection
        fov: 0.12, // initial field of view in degrees
        // target: 'NGC 2175', // initial target
        cooFrame: 'ICRS', // set galactic frame reticleColor: '#ff89ff', // change reticle color
        showReticle: false,
        showCooGrid: false,
        fullScreen: false
      })

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
