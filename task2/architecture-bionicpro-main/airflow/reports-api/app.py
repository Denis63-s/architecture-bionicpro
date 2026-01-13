from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt

app = FastAPI(title="Reports API")

auth_scheme = HTTPBearer()

# ⚠️ Для учебки можно не валидировать подпись строго.
# Для "правильно": надо JWKS Keycloak и проверка aud/iss.
JWT_ALG = "RS256"  # у Keycloak обычно RS256
# PUBLIC_KEY / JWKS - можно добавить позже


def get_current_user(creds: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    token = creds.credentials
    try:
        # учебный упрощённый декод без проверки подписи:
        payload = jwt.get_unverified_claims(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_sub = payload.get("sub")
    if not user_sub:
        raise HTTPException(status_code=401, detail="No sub in token")

    return {"sub": user_sub, "payload": payload}


def query_olap_report_for_user(user_id: str):
    # TODO: заменить на реальный запрос к OLAP:
    # SELECT * FROM reports_mart WHERE user_id = :user_id ORDER BY period_end DESC LIMIT 1
    return {
        "user_id": user_id,
        "period_start": "2025-01-01",
        "period_end": "2025-01-02",
        "avg_steps": 1000,
        "avg_load": 0.35,
        "max_load": 0.5,
        "alerts_count": 1,
    }


@app.get("/reports")
def get_my_report(user=Depends(get_current_user)):
    user_id = user["sub"]
    report = query_olap_report_for_user(user_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    return report
