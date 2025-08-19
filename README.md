# Bridge Dux (Backend puente)

Backend mínimo para integrar ChatGPT Actions con la API de Dux ERP respetando seguridad y rate-limit.

## Requisitos
- Node 18+
- Variables en `.env` (ver `.env.example`):
  - `PORT`
  - `API_KEY` (para tu backend)
  - `DUX_TOKEN` (token de Dux)
  - `DUX_BASE` (base de Dux, por defecto https://erp.duxsoftware.com.ar/WSERP/rest/services)

## Setup
```bash
npm i
cp .env.example .env  # completar valores reales
npm run dev
```
Probar:
```bash
curl -H "Authorization: Bearer super-secreto-del-backend" http://localhost:3000/duxc/items
curl -X POST http://localhost:3000/duxc/pedido   -H "Authorization: Bearer super-secreto-del-backend" -H "Content-Type: application/json"   -d '{"clienteId":123,"items":[{"itemId":789,"cantidad":2}],"externalId":"orden-001"}'
```

## OpenAPI para ChatGPT Actions
Servido en `GET /openapi.yaml`. Publicalo en tu dominio y luego importalo desde **Create GPT → Actions → Add Action → Import from URL**.

## Producción
- Hosting con HTTPS (Render/Railway/Fly/Vercel/EC2).
- Rotación de `API_KEY` y `DUX_TOKEN`.
- Logs/observabilidad.
- Persistir idempotencia y colas en Redis/DB si hay alto volumen.
