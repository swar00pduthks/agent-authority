"""Authentication: OIDC bearer tokens (Microsoft Entra ID or any OIDC provider) and an ingest API key.

Configuration (environment):

    AUTHORITY_OIDC_ISSUER      e.g. https://login.microsoftonline.com/<tenant-id>/v2.0
    AUTHORITY_OIDC_AUDIENCE    e.g. api://agent-authority  (the app registration's Application ID URI)
    AUTHORITY_OIDC_JWKS_URL    optional; discovered from the issuer when unset
    AUTHORITY_OIDC_READ_ROLE   optional app role required to read, e.g. Authority.Read
    AUTHORITY_OIDC_INGEST_ROLE optional app role allowed to ingest, e.g. Authority.Ingest
    AUTHORITY_API_KEY          optional static key for emitters that can't get tokens (Spark, Airflow)

With no issuer configured the read API is open (local demo mode) and the server logs a warning.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("agent_authority.auth")

PUBLIC_PATHS = ("/api/authority/health",)


class AuthError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass
class Principal:
    subject: str
    name: str | None = None
    roles: list[str] = field(default_factory=list)
    via: str = "oidc"  # oidc | api-key | anonymous


@dataclass
class AuthConfig:
    issuer: str | None = None
    audience: str | None = None
    jwks_url: str | None = None
    read_role: str | None = None
    ingest_role: str | None = None
    api_key: str | None = None
    jwks: dict[str, Any] | None = None  # inject keys directly (tests, air-gapped setups)
    leeway_s: int = 60

    @classmethod
    def from_env(cls) -> AuthConfig:
        return cls(
            issuer=os.getenv("AUTHORITY_OIDC_ISSUER") or None,
            audience=os.getenv("AUTHORITY_OIDC_AUDIENCE") or None,
            jwks_url=os.getenv("AUTHORITY_OIDC_JWKS_URL") or None,
            read_role=os.getenv("AUTHORITY_OIDC_READ_ROLE") or None,
            ingest_role=os.getenv("AUTHORITY_OIDC_INGEST_ROLE") or None,
            api_key=os.getenv("AUTHORITY_API_KEY") or None,
        )

    @property
    def oidc_enabled(self) -> bool:
        return bool(self.issuer)


class TokenVerifier:
    """Validates RS256/ES256 JWTs against the issuer's JWKS (cached, refreshed on unknown key ids)."""

    def __init__(self, cfg: AuthConfig) -> None:
        import jwt  # PyJWT

        self._jwt = jwt
        self.cfg = cfg
        self._keys: dict[str, Any] = {}
        self._fetched_at = 0.0
        self._lock = threading.Lock()
        if cfg.jwks:
            self._load(cfg.jwks)

    def _load(self, jwks: dict[str, Any]) -> None:
        keys = {}
        for k in jwks.get("keys", []):
            jwk = self._jwt.PyJWK(k)
            keys[k.get("kid", "")] = jwk.key
        self._keys = keys
        self._fetched_at = time.time()

    def _jwks_url(self) -> str:
        if self.cfg.jwks_url:
            return self.cfg.jwks_url
        url = self.cfg.issuer.rstrip("/") + "/.well-known/openid-configuration"
        with urllib.request.urlopen(url, timeout=10) as r:
            self.cfg.jwks_url = json.load(r)["jwks_uri"]
        return self.cfg.jwks_url

    def _key(self, kid: str) -> Any:
        if kid in self._keys:
            return self._keys[kid]
        with self._lock:  # unknown kid: keys may have rotated; refetch at most once a minute
            if kid not in self._keys and not self.cfg.jwks and time.time() - self._fetched_at > 60:
                with urllib.request.urlopen(self._jwks_url(), timeout=10) as r:
                    self._load(json.load(r))
        if kid not in self._keys:
            raise AuthError(401, "token signed with an unknown key")
        return self._keys[kid]

    def verify(self, token: str) -> Principal:
        jwt = self._jwt
        try:
            kid = jwt.get_unverified_header(token).get("kid", "")
            claims = jwt.decode(
                token, self._key(kid), algorithms=["RS256", "RS384", "RS512", "ES256", "ES384"],
                audience=self.cfg.audience, issuer=self.cfg.issuer, leeway=self.cfg.leeway_s,
                options={"require": ["exp", "iss", "sub"], "verify_aud": bool(self.cfg.audience)},
            )
        except AuthError:
            raise
        except jwt.ExpiredSignatureError as exc:
            raise AuthError(401, "token expired") from exc
        except jwt.PyJWTError as exc:
            raise AuthError(401, f"invalid token: {exc}") from exc
        roles = claims.get("roles") or []
        if isinstance(roles, str):
            roles = [roles]
        name = claims.get("preferred_username") or claims.get("upn") or claims.get("email") or claims.get("name")
        return Principal(subject=claims.get("oid") or claims["sub"], name=name, roles=list(roles))


class Authenticator:
    def __init__(self, cfg: AuthConfig) -> None:
        self.cfg = cfg
        self.verifier = TokenVerifier(cfg) if cfg.oidc_enabled else None
        if not cfg.oidc_enabled:
            log.warning("OIDC is not configured: the read API is open. Set AUTHORITY_OIDC_ISSUER for any shared deployment.")

    @staticmethod
    def _bearer(authorization: str | None) -> str | None:
        if authorization and authorization.lower().startswith("bearer "):
            return authorization[7:].strip()
        return None

    def authenticate(self, path: str, method: str, authorization: str | None) -> Principal:
        if path in PUBLIC_PATHS:
            return Principal("anonymous", via="anonymous")
        token = self._bearer(authorization)
        ingest = method == "POST" and path == "/api/v1/lineage"

        if ingest and self.cfg.api_key and token == self.cfg.api_key:
            return Principal("api-key", via="api-key")
        if self.verifier is None:
            if ingest and self.cfg.api_key:
                raise AuthError(401, "invalid API key")
            return Principal("anonymous", via="anonymous")
        if not token:
            raise AuthError(401, "missing bearer token")
        p = self.verifier.verify(token)
        role = self.cfg.ingest_role if ingest else self.cfg.read_role
        if role and role not in p.roles:
            raise AuthError(403, f"token lacks the {role} role")
        return p
