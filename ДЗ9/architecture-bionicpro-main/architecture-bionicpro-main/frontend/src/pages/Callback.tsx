import React, { useEffect } from "react";
import { AUTH_CONFIG } from "../auth/config";

export default function Callback() {
  useEffect(() => {
    (async () => {
      const code = new URLSearchParams(window.location.search).get("code");
      const verifier = sessionStorage.getItem("pkce_verifier");
      if (!code || !verifier) return;

      const tokenUrl = `${AUTH_CONFIG.KC_URL}/realms/${AUTH_CONFIG.REALM}/protocol/openid-connect/token`;

      const body = new URLSearchParams();
      body.append("grant_type", "authorization_code");
      body.append("client_id", AUTH_CONFIG.CLIENT_ID);
      body.append("redirect_uri", AUTH_CONFIG.REDIRECT_URI);
      body.append("code", code);
      body.append("code_verifier", verifier);

      await fetch(tokenUrl, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
      });

      sessionStorage.removeItem("pkce_verifier");
      window.location.href = "/";
    })();
  }, []);

  return <div>Logging in (PKCE)…</div>;
}
