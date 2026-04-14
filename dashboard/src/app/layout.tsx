import type { Metadata, Viewport } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" });

export const metadata: Metadata = {
  title: "Roderick",
  description: "Roderick operator console",
  manifest: "/manifest.webmanifest",
  applicationName: "Roderick",
  appleWebApp: {
    capable: true,
    statusBarStyle: "black-translucent",
    title: "Roderick",
  },
  formatDetection: {
    telephone: false,
  },
  icons: {
    apple: "/apple-icon",
    icon: "/icon",
  },
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
  themeColor: "#030806",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={inter.variable}>
      <body>{children}</body>
    </html>
  );
}
