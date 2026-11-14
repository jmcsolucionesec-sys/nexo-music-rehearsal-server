# NEXO Music · servidor de ensayo en grupo

Sesiones en tiempo real para el ensayo en grupo de la app NEXO Music (no se
transmite audio). FastAPI + WebSocket, todo en memoria.

- `GET /health`
- `POST /rehearsal/sessions` · `POST /rehearsal/sessions/{id o código}/join`
- WebSocket `/rehearsal/sessions/{id}/ws?token=…`

Variables opcionales: `REHEARSAL_MAX_SESSIONS` (100), `REHEARSAL_CODE_TTL_SECONDS`,
`REHEARSAL_SESSION_TTL_SECONDS`. Escucha en `$PORT` (10000 por defecto).

```
docker build -t nexo-rehearsal . && docker run -p 10000:10000 nexo-rehearsal
```
