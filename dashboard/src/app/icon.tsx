import { ImageResponse } from "next/og";

export const size = {
  width: 512,
  height: 512,
};

export const contentType = "image/png";

export default function Icon() {
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
          position: "relative",
        }}
      >
        <div
          style={{
            position: "absolute",
            inset: 36,
            borderRadius: 64,
            border: "2px solid rgba(93,255,159,0.18)",
            background: "linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.01))",
            boxShadow: "0 0 80px rgba(93,255,159,0.12) inset",
          }}
        />
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            gap: 22,
            zIndex: 1,
          }}
        >
          <div
            style={{
              width: 172,
              height: 172,
              borderRadius: 999,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              border: "3px solid #5dff9f",
              background: "radial-gradient(circle, rgba(93,255,159,0.2) 0%, rgba(93,255,159,0.05) 60%, rgba(93,255,159,0) 100%)",
              boxShadow: "0 0 50px rgba(93,255,159,0.18)",
              fontSize: 92,
              fontWeight: 700,
            }}
          >
            R
          </div>
          <div
            style={{
              fontSize: 50,
              fontWeight: 700,
              letterSpacing: -1.5,
            }}
          >
            Roderick
          </div>
        </div>
      </div>
    ),
    size,
  );
}
