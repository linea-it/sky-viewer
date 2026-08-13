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
import { Inter } from "next/font/google";
import CssBaseline from '@mui/material/CssBaseline';
import { AuthProvider } from "@/contexts/AuthContext";
import MainContainer from "@/containers/MainContainer";
const inter = Inter({ subsets: ["latin"] });

export const metadata = {
  title: "SKY VIEWER",
  description: "Sky Viewer by LIneA",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <head>
        {/* Google tag (gtag.js) */}
        <script async src="https://www.googletagmanager.com/gtag/js?id=G-MCJDRDYW7W" />
        <script
          dangerouslySetInnerHTML={{
            __html: `
              window.dataLayer = window.dataLayer || [];
              function gtag(){dataLayer.push(arguments);}
              gtag('js', new Date());
              gtag('config', 'G-MCJDRDYW7W');
            `
          }}
        />
      </head>
      <body>
        <AppRouterCacheProvider options={{ enableCssLayer: true }}>
          <ThemeProvider theme={theme}>
            <AuthProvider>
              <MainContainer>
                {children}
              </MainContainer>
            </AuthProvider>
          </ThemeProvider>
        </AppRouterCacheProvider>
      </body>
    </html >
  );
}
