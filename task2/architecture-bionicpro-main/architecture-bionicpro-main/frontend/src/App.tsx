import React from "react";
import ReportPage from "./components/ReportPage";
import Callback from "./pages/Callback";
import { loginWithPkce } from "./auth/login";

export default function App() {
  const path = window.location.pathname;

  if (path === "/callback") {
    return <Callback />;
  }

  return (
    <div style={{ padding: 24 }}>
      <button onClick={() => loginWithPkce()}>Login (PKCE)</button>
      <ReportPage />
    </div>
  );
}
