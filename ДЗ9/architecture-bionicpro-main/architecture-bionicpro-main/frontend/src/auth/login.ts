import { randomString, pkceChallengeFromVerifier } from "./pkce";
import { AUTH_CONFIG } from "./config";

export async function loginWithPkce() {
  const verifier = randomString(64);
  const challenge = await pkceChallengeFromVerifier(verifier);

  sessionStorage.setItem("pkce_verifier", verifier);

  const url = new URL(
    `${AUTH_CONFIG.KC_URL}/realms/${AUTH_CONFIG.REALM}/protocol/openid-connect/auth`
  );

  url.searchParams.set("client_id", AUTH_CONFIG.CLIENT_ID);
  url.searchParams.set("redirect_uri", AUTH_CONFIG.REDIRECT_URI);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("scope", "openid");
  url.searchParams.set("code_challenge", challenge);
  url.searchParams.set("code_challenge_method", "S256");

  window.location.href = url.toString();
}
