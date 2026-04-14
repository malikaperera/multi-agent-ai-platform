import { ImageResponse } from "next/og";

export const size = {
  width: 180,
  height: 180,
};

export const contentType = "image/png";

export default function AppleIcon() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          background: "linear-gradient(160deg, #03110b 0%, #020604 100%)",
          color: "#ecfff5",
          fontFamily: "Inter, sans-serif",
          borderRadius: 36,
        }}
      >
        <div
          style={{
            width: 112,
            height: 112,
            borderRadius: 999,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "3px solid #5dff9f",
            background: "radial-gradient(circle, rgba(93,255,159,0.25) 0%, rgba(93,255,159,0.05) 60%, rgba(93,255,159,0) 100%)",
            boxShadow: "0 0 30px rgba(93,255,159,0.18)",
            fontSize: 64,
            fontWeight: 700,
          }}
        >
          R
        </div>
      </div>
    ),
    size,
  );
}
