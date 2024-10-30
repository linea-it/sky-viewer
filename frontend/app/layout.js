import "./globals.css";
import '@fontsource/roboto/300.css';
import '@fontsource/roboto/400.css';
import '@fontsource/roboto/500.css';
import '@fontsource/roboto/700.css';
import { AppRouterCacheProvider } from '@mui/material-nextjs/v13-appRouter';
import { ThemeProvider } from '@mui/material/styles';
import Box from "@mui/material/Box";
import theme from './theme';
import Header from "@/components/Header";
import Footer from "@/components/Footer";
import { Inter } from "next/font/google";
import CssBaseline from '@mui/material/CssBaseline';
const inter = Inter({ subsets: ["latin"] });

export const metadata = {
  title: "Sky Viewer",
  description: "Sky Viewer by LIneA",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">

      {/* <Script
        src="https://aladin.cds.unistra.fr/AladinLite/api/v3/latest/aladin.js"
        charset="utf-8"
        strategy="afterInteractive"
      // onLoad={() =>
      //   console.log(`script loaded correctly, window.A has been created`)
      // }
      /> */}
      <body>
        <AppRouterCacheProvider options={{ enableCssLayer: true }}>
          <ThemeProvider theme={theme}>
            <CssBaseline />
            <Header />
            <Box
              component='main'
              sx={{
                paddingLeft: 0,
                paddingRight: 0,
                display: 'flex',
                // TODO: tamanho minimo do container principal deve ser
                // Total disponivel 100vg - 64px do header - 495.56px do footer (porém está impactando no aladin da home, que ao passar esse valor, ele comprime)
                // minHeight: 'calc(100vh - 64px - 32px)'
              }}>
              {children}
            </Box>
            <Footer />
          </ThemeProvider>
        </AppRouterCacheProvider>
      </body>
    </html >
  );
}
