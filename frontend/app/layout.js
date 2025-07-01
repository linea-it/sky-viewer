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
