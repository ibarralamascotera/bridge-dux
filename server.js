import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import fetch from 'node-fetch';
import PQueue from 'p-queue';
import fs from 'fs';

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(morgan('tiny'));

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY;
const DUX_BASE = process.env.DUX_BASE || 'https://erp.duxsoftware.com.ar/WSERP/rest/services';
const DUX_TOKEN = process.env.DUX_TOKEN;

// --- Auth simple (Bearer API Key) ---
app.use((req, res, next) => {
  const auth = req.headers.authorization || '';
  if (!auth.startsWith('Bearer ')) return res.status(401).json({ error: 'Falta Authorization' });
  const key = auth.replace('Bearer ', '').trim();
  if (key !== API_KEY) return res.status(403).json({ error: 'API Key inválida' });
  next();
});

// --- Cola: 1 tarea cada 5s (rate limit Dux) ---
const queue = new PQueue({
  interval: 5000,
  intervalCap: 1
});

async function callDux(path, { method = 'GET', body } = {}) {
  const url = `${DUX_BASE}${path}`;
  const headers = {
    Authorization: `Bearer ${DUX_TOKEN}`,
    'Content-Type': 'application/json'
  };
  const run = async (attempt = 1) => {
    const r = await fetch(url, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined
    });
    if (!r.ok && (r.status >= 500 || r.status === 429)) {
      if (attempt >= 3) {
        const text = await r.text().catch(() => '');
        throw new Error(`DUX ${r.status}: ${text || r.statusText}`);
      }
      const backoff = 500 * Math.pow(2, attempt - 1);
      await new Promise((res) => setTimeout(res, backoff));
      return run(attempt + 1);
    }
    const contentType = r.headers.get('content-type') || '';
    if (contentType.includes('application/json')) return r.json();
    return r.text();
  };
  return queue.add(() => run());
}

// --- Endpoints puente ---
app.get('/duxc/items', async (req, res) => {
  try {
    const data = await callDux('/items', { method: 'GET' });
    res.json({ items: data });
  } catch (e) {
    res.status(502).json({ error: 'Error consultando Dux /items', detail: e.message });
  }
});

// body: { clienteId, items:[{ itemId, cantidad, precio? }], observaciones? , externalId? }
const seenExternalIds = new Set();
app.post('/duxc/pedido', async (req, res) => {
  try {
    const { clienteId, items, observaciones, externalId } = req.body || {};
    if (!clienteId || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: 'Faltan campos: clienteId, items[]' });
    }
    if (externalId) {
      if (seenExternalIds.has(externalId)) {
        return res.status(200).json({ estado: 'duplicado', mensaje: 'Pedido ya procesado' });
      }
    }
    const payload = { clienteId, items, observaciones };
    const data = await callDux('/pedido/nuevopedido', { method: 'POST', body: payload });
    if (externalId) seenExternalIds.add(externalId);
    res.json({ estado: 'ok', resultado: data });
  } catch (e) {
    res.status(502).json({ error: 'Error creando pedido en Dux', detail: e.message });
  }
});

// Servir OpenAPI
app.get('/openapi.yaml', (_req, res) => {
  res.setHeader('Content-Type', 'text/yaml');
  res.send(fs.readFileSync('./openapi.yaml', 'utf8'));
});

app.listen(PORT, () => console.log(`Bridge Dux escuchando en :${PORT}`));
