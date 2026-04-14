import type { MetadataRoute } from "next";

export default function manifest(): MetadataRoute.Manifest {
  return {
    name: "Roderick Operator Console",
    short_name: "Roderick",
    description: "Mobile-friendly operator console for the local AI ecosystem.",
    start_url: "/",
    display: "standalone",
    background_color: "#030806",
    theme_color: "#030806",
    icons: [
      {
        src: "/icon",
        sizes: "512x512",
        type: "image/png",
      },
      {
        src: "/apple-icon",
        sizes: "180x180",
        type: "image/png",
      },
    ],
  };
}
