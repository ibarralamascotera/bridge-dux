// server.js
import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import axios from 'axios';
import PQueue from 'p-queue';
import fs from 'fs';

const DUX_PATHS = {
  items: '/items',
  compras: '/compras',
  depositos: '/deposito',        // antes estaba '/deposito'
  empresas: '/empresas',
  facturas: '/facturas',
  pedidos: '/pedidos',
  listasPrecioVenta: '/listaprecioventa',
  localidades: '/localidades',    // antes estaba '/localidad'
  percepciones: '/percepcionesImpuestos',
  personal: '/personal',
  provincias: '/provincias',
  rubros: '/rubros',
  subrubros: '/subrubros',
  sucursales: '/sucursales',      // antes estaba '/sucursal'
};

const app = express();
const idemStore = new Map(); // key -> resultado

async function withIdempotency(key, handler) {
  if (!key) return handler(); // si no mandan clave, procesa normal
  if (idemStore.has(key)) return idemStore.get(key);
  const result = await handler();
  idemStore.set(key, result);
  return result;
}



// ID de request para trazabilidad
app.use((req, res, next) => {
  const rid = req.headers['x-request-id'] || crypto.randomUUID();
  req.id = rid;
  res.setHeader('X-Request-Id', rid);
  next();
});

function sendProblem(res, {
  status = 500,
  title = 'Internal Server Error',
  detail = 'Error interno',
  type = 'about:blank',
  instance,
  requestId,
  extras = {}
}) {
  res.status(status)
    .type('application/problem+json')
    .json({
      type,
      title,
      status,
      detail,
      instance: instance || res.req?.originalUrl,
      requestId: requestId || res.req?.id,
      ...extras,            // campos adicionales (p.ej. code)
    });
}


// === Config ===
const PORT      = process.env.PORT || 3000;
const API_KEY   = process.env.API_KEY; // tu API Key del backend (para proteger el bridge)
const DUX_BASE  = process.env.DUX_BASE || 'https://erp.duxsoftware.com.ar/WSERP/rest/services';
const DUX_TOKEN = process.env.DUX_TOKEN; // token de Dux (va en header "authorization")

if (!API_KEY)  console.warn('[WARN] Falta API_KEY en .env');
if (!DUX_TOKEN) console.warn('[WARN] Falta DUX_TOKEN en .env');



// === Rutas públicas (no requieren API_KEY) ===
app.get('/', (_req, res) => res.json({ name: 'Bridge Dux', base: DUX_BASE, ok: true }));
app.get('/health', (_req, res) => res.json({ ok: true }));
app.get('/openapi.yaml', (_req, res) => {
  try {
    const file = fs.readFileSync('./openapi.yaml', 'utf8');
    res.setHeader('Content-Type', 'text/yaml');
    res.send(file);
  } catch {
    res.status(404).send('# openapi.yaml no encontrado');
  }
});

// === Middleware de auth (se aplica a lo demás) ===
app.use((req, res, next) => {
  const auth = req.headers.authorization || '';
  if (!auth.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Falta Authorization' });
  }
  const key = auth.replace('Bearer ', '').trim();
  if (key !== API_KEY) {
    return res.status(403).json({ error: 'API Key inválida' });
  }
  next();
});

// === Auth del Backend (Bearer API Key) ===
// Requiere: Authorization: Bearer <API_KEY>
app.use((req, res, next) => {
  const auth = req.headers.authorization || '';
  if (!auth.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Falta Authorization' });
  }
  const key = auth.replace('Bearer ', '').trim();
  if (key !== API_KEY) {
    return res.status(403).json({ error: 'API Key inválida' });
  }
  next();
});

// === Cliente base hacia Dux ===
const dux = axios.create({
  baseURL: DUX_BASE,
  timeout: 30000,
});

// Armado de headers para Dux
function buildDuxHeaders() {
  return {
    'Content-Type': 'application/json',
    accept: 'application/json',
    authorization: DUX_TOKEN, // <- Dux espera este header sin "Bearer"
  };
}

// === Rate Limit y reintentos ===
// 1 request cada 5s; reintentos con backoff para 429/5xx
const queue = new PQueue({ interval: 5000, intervalCap: 1 });

async function callDux(path, { method = 'GET', data, params } = {}) {
  return queue.add(async () => {
    const headers = buildDuxHeaders();
    let attempt = 1;
    const maxAttempts = 3;

    while (true) {
      try {
        const resp = await dux.request({ url: path, method, data, params, headers });
        return resp.data;
      } catch (err) {
        const ax = err; // AxiosError
        const status = ax.response?.status;
        const body = ax.response?.data;
        const retryAfter = ax.response?.headers?.['retry-after'];
        const netCode = ax.code; // 'ECONNABORTED', 'ETIMEDOUT', etc.
        const msg = body ? (typeof body === 'string' ? body : JSON.stringify(body)) : ax.message;

        // 401/403 de Dux: credenciales inválidas del DUX_TOKEN => no reintentar
        if (status === 401 || status === 403) {
          const e = new Error(`DUX ${status}: ${msg}`);
          e.isDux = true;
          e.status = 502;                 // Gateway error hacia el cliente
          e.code = 'DUX_UNAUTHORIZED';
          e.detail = 'El ERP rechazó las credenciales (DUX_TOKEN).';
          throw e;
        }

        // 429 o 5xx: reintentos con backoff
        if ((status === 429 || (status && status >= 500)) && attempt < maxAttempts) {
          const backoff = 500 * Math.pow(2, attempt - 1);
          await new Promise(r => setTimeout(r, backoff));
          attempt++;
          continue;
        }

        // Errores de red/timeout
        if (!status) {
          const e = new Error(`DUX NET: ${netCode || 'NETWORK_ERROR'} - ${msg}`);
          e.isDux = true;
          e.status = 504;                 // Gateway Timeout
          e.code = netCode || 'NETWORK_ERROR';
          e.detail = 'Error de red al contactar ERP.';
          throw e;
        }

        // Otros HTTP 4xx/5xx sin reintentos
        const e = new Error(`DUX ${status}: ${msg}`);
        e.isDux = true;
        e.status = status === 429 ? 429 : 502; // 429 se respeta; resto -> 502
        e.code = status === 429 ? 'DUX_RATE_LIMIT' : 'DUX_ERROR';
        e.detail = typeof body === 'string' ? body : 'Error desde ERP.';
        if (retryAfter) e.retryAfter = retryAfter;
        throw e;
      }
    }
  });
}


// === Helper para GET de passthrough con req.query ===
function makeGetProxy(localPath, duxPath) {
  app.get(localPath, async (req, res) => {
    try {
      const data = await callDux(duxPath, { method: 'GET', params: req.query });
      res.json(data);
    } catch (e) {
      res.status(502).json({ error: `Error consultando Dux ${duxPath}`, detail: e.message });
    }
  });
}

// === Endpoints GET (consulta) ===
// Usá query params tal cual los pida Dux. Ej: ?offset=0&limit=20
makeGetProxy('/duxc/items',               DUX_PATHS.items);
makeGetProxy('/duxc/compras',             DUX_PATHS.compras);
makeGetProxy('/duxc/depositos',           DUX_PATHS.depositos);
makeGetProxy('/duxc/empresas',            DUX_PATHS.empresas);
makeGetProxy('/duxc/facturas',            DUX_PATHS.facturas);
makeGetProxy('/duxc/pedidos',             DUX_PATHS.pedidos);
makeGetProxy('/duxc/listas-precio-venta', DUX_PATHS.listasPrecioVenta);
makeGetProxy('/duxc/localidades',         DUX_PATHS.localidades);
makeGetProxy('/duxc/percepciones',        DUX_PATHS.percepciones);
makeGetProxy('/duxc/personal',            DUX_PATHS.personal);
makeGetProxy('/duxc/provincias',          DUX_PATHS.provincias);
makeGetProxy('/duxc/rubros',              DUX_PATHS.rubros);
makeGetProxy('/duxc/subrubros',           DUX_PATHS.subrubros);
makeGetProxy('/duxc/sucursales',          DUX_PATHS.sucursales);


// === Endpoints de estado de jobs (si Dux los expone así) ===
makeGetProxy('/duxc/factura/estado',      '/obtenerEstadoFactura');
makeGetProxy('/duxc/items/estado',        '/obtenerEstadoItems');

// justo debajo de makeGetProxy(...)
app.get('/duxc/items', async (req, res) => {
  try {
    const limit  = Number(req.query.limit)  || 20;  // seguro para Actions
    const offset = Number(req.query.offset) || 0;

    const data = await callDux('/items', {
      method: 'GET',
      params: { ...req.query, limit, offset }
    });

    res.json(data);
  } catch (e) {
    res.status(502).json({ error: 'Error consultando Dux /items', detail: e.message });
  }
});

// === Endpoints POST (operaciones) ===
// Idempotencia simple en memoria para externalId (en prod: Redis/DB)
const seenExternalIds = new Set();

// Helper para tomar la key de idempotencia
function getIdemKey(req) {
  return req.headers['idempotency-key'] || req.body?.externalId || null;
}

// GET /analytics/top-vendidos?fechaDesde=YYYY-MM-DD&fechaHasta=YYYY-MM-DD&idEmpresa=####&idSucursal=##&top=5&source=facturas|pedidos
app.get('/analytics/top-vendidos', async (req, res) => {
  try {
    const { fechaDesde, fechaHasta, idEmpresa, idSucursal } = req.query;
    const source = (req.query.source || 'facturas').toLowerCase(); // 'facturas' | 'pedidos'
    const top = Math.max(1, Math.min(100, Number(req.query.top) || 10));
    const pageSize = Math.max(50, Math.min(500, Number(req.query.pageSize) || 200));

    if (!idEmpresa) {
      return res.status(400).json({ error: 'Falta idEmpresa' });
    }

    const duxPath = source === 'pedidos' ? '/pedidos' : '/facturas';

    // Helpers de normalización
    const pickDetailArray = (row) => {
      if (!row || typeof row !== 'object') return [];
      // candidatos más comunes
      const candidates = [
        'detalle', 'detalles', 'renglones', 'items', 'lineas', 'líneas',
        'detalleFactura', 'detalle_factura', 'renglon', 'productos', 'articulos'
      ];
      for (const k of candidates) {
        if (Array.isArray(row[k])) return row[k];
      }
      // si alguna key apunta a array de objetos, tomarla
      for (const [k, v] of Object.entries(row)) {
        if (Array.isArray(v) && v.length && typeof v[0] === 'object') {
          return v;
        }
      }
      return [];
    };

    const pickItemId = (it) =>
      it.itemId ?? it.idItem ?? it.idArticulo ?? it.articuloId ?? it.id ?? it.codigoArticulo ?? null;

    const pickCantidad = (it) => {
      const candidates = [
        it.cantidad, it.cant, it.cantidadFacturada, it.cantidadVendida, it.unidades
      ];
      for (const v of candidates) {
        const n = Number(v);
        if (!Number.isNaN(n) && n) return n;
      }
      return 0;
    };

    const pickNombre = (it) =>
      it.descripcion ?? it.nombre ?? it.detalle ?? it.descripcionArticulo ?? it.nombreArticulo ?? null;

    // Acumulador
    const acumulado = new Map(); // itemId -> { cantidad, nombre }
    let offset = 0;
    let totalFilas = 0;
    let firstRowKeys = null;
    let firstLineKeys = null;

    while (true) {
      const params = {
        idEmpresa,
        limit: pageSize,
        offset,
        ...(fechaDesde ? { fechaDesde } : {}),
        ...(fechaHasta ? { fechaHasta } : {}),
        ...(idSucursal ? { idSucursal } : {}),
      };

      const page = await callDux(duxPath, { method: 'GET', params });

      // Normalizar listado raíz
      const rows =
        Array.isArray(page)
          ? page
          : (page?.data || page?.facturas || page?.pedidos || page?.resultado || []);

      if (!rows.length) break;

      totalFilas += rows.length;
      if (!firstRowKeys && rows[0] && typeof rows[0] === 'object') {
        firstRowKeys = Object.keys(rows[0]);
      }

      for (const row of rows) {
        const detalle = pickDetailArray(row);
        if (detalle.length && !firstLineKeys && typeof detalle[0] === 'object') {
          firstLineKeys = Object.keys(detalle[0]);
        }
        for (const it of detalle) {
          const itemId = pickItemId(it);
          const cant = pickCantidad(it);
          if (!itemId || !cant) continue;

          const prev = acumulado.get(itemId) || { cantidad: 0, nombre: pickNombre(it) };
          prev.cantidad += cant;
          if (!prev.nombre) prev.nombre = pickNombre(it);
          acumulado.set(itemId, prev);
        }
      }

      if (rows.length < pageSize) break;
      offset += pageSize;
    }

    // Armar ranking
    const ranking = [...acumulado.entries()]
      .map(([itemId, v]) => ({ itemId, nombre: v.nombre || null, cantidad: v.cantidad }))
      .sort((a, b) => b.cantidad - a.cantidad)
      .slice(0, top);

    res.json({
      top: ranking,
      total_items: acumulado.size,
      filas_procesadas: totalFilas,
      rango: { fechaDesde, fechaHasta },
      idEmpresa: String(idEmpresa),
      ...(idSucursal ? { idSucursal: String(idSucursal) } : {}),
      source,
      debug_sample: {
        first_row_keys: firstRowKeys || [],
        first_line_keys: firstLineKeys || []
      }
    });
  } catch (e) {
    res.status(502).json({
      error: 'Error calculando top-vendidos',
      detail: String(e?.message || e)
    });
  }
});



// Helpers
function normalizeDate(s) {
  // si viene 'YYYY-MM-DD' => 'DD/MM/YYYY'; si ya viene 'DD/MM/YYYY' la deja.
  if (!s) return s;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const [y, m, d] = s.split('-');
    return `${d}/${m}/${y}`;
  }
  return s;
}
function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}



// Ruta de prueba (no pega a Dux)
app.post('/tests/idem', async (req, res) => {
  try {
    const key = req.headers['idempotency-key'] || req.body?.externalId || null;
    const result = await withIdempotency(key, async () => {
      // "Trabajo caro" simulado
      const opId = Math.random().toString(36).slice(2);
      return { ok: true, opId, note: 'operación simulada' };
    });
    res.json(result);
  } catch (e) {
    res.sendProblem?.(500, 'Error interno', e.message) || res.status(500).json({ error: e.message });
  }
});

// Crear Pedido
// Body esperado (ejemplo):
// { clienteId, items:[{ itemId, cantidad, precio? }], observaciones?, externalId? }
app.post('/duxc/pedido', async (req, res) => {
  const key = getIdemKey(req);
  try {
    const { clienteId, items } = req.body || {};
    if (!clienteId || !Array.isArray(items) || items.length === 0) {
      return res.sendProblem(
        400,
        'Solicitud inválida',
        'Faltan campos obligatorios.',
        { invalidParams: ['clienteId', 'items[]'] }
      );
    }
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Pedido ya procesado' });
    }
    const resultado = await callDux('/pedido/nuevopedido', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando pedido', String(e?.message || e));
  }
});


// Crear Factura
// Body: conforme al payload que requiera Dux
app.post('/duxc/factura', async (req, res) => {
  const key = getIdemKey(req);
  try {
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Factura ya procesada' });
    }
    const resultado = await callDux('/factura/nuevaFactura', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando factura', String(e?.message || e));
  }
});

// Modificar/Crear Item (según API Dux: /item/nuevoItem)
app.post('/duxc/items/modificar', async (req, res) => {
  try {
    const resultado = await callDux('/item/nuevoItem', { method: 'POST', data: req.body });
    res.json(resultado);
  } catch (e) {
    res.status(502).json({ error: 'Error modificando/creando item', detail: e.message });
  }
});



// Nota de Crédito
app.post('/duxc/nota-credito', async (req, res) => {
  const key = getIdemKey(req);
  try {
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Nota de crédito ya procesada' });
    }
    const resultado = await callDux('/notaCredito/nuevaNotaCredito', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando nota de crédito', String(e?.message || e));
  }
});

// Nota de Débito
app.post('/duxc/nota-debito', async (req, res) => {
  const key = getIdemKey(req);
  try {
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Nota de débito ya procesada' });
    }
    const resultado = await callDux('/notaDebito/nuevaNotaDebito', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando nota de débito', String(e?.message || e));
  }
});

// Cobranza
app.post('/duxc/cobranza', async (req, res) => {
  const key = getIdemKey(req);
  try {
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Cobranza ya procesada' });
    }
    const resultado = await callDux('/cobranza/nuevaCobranza', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando cobranza', String(e?.message || e));
  }
});

// Pago
app.post('/duxc/pago', async (req, res) => {
  const key = getIdemKey(req);
  try {
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Pago ya procesado' });
    }
    const resultado = await callDux('/pago/nuevoPago', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando pago', String(e?.message || e));
  }
});

// Remito
app.post('/duxc/remito', async (req, res) => {
  const key = getIdemKey(req);
  try {
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Remito ya procesado' });
    }
    const resultado = await callDux('/remito/nuevoRemito', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando remito', String(e?.message || e));
  }
});

// Transferencia de depósitos
app.post('/duxc/transferencia', async (req, res) => {
  const key = getIdemKey(req);
  try {
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Transferencia ya procesada' });
    }
    const resultado = await callDux('/transferencia/nuevaTransferencia', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando transferencia', String(e?.message || e));
  }
});

// Ajuste de Stock
app.post('/duxc/ajuste-stock', async (req, res) => {
  const key = getIdemKey(req);
  try {
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Ajuste de stock ya procesado' });
    }
    const resultado = await callDux('/ajusteStock/nuevoAjusteStock', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando ajuste de stock', String(e?.message || e));
  }
});

// Movimiento de Stock
app.post('/duxc/movimiento-stock', async (req, res) => {
  const key = getIdemKey(req);
  try {
    if (key && seenExternalIds.has(key)) {
      return res.json({ estado: 'duplicado', mensaje: 'Movimiento de stock ya procesado' });
    }
    const resultado = await callDux('/movimientoStock/nuevoMovimientoStock', { method: 'POST', data: req.body });
    if (key) seenExternalIds.add(key);
    res.json(resultado);
  } catch (e) {
    res.sendProblem(502, 'Error creando movimiento de stock', String(e?.message || e));
  }
});


// === OpenAPI (opcional) ===
app.get('/openapi.yaml', (_req, res) => {
  try {
    const file = fs.readFileSync('./openapi.yaml', 'utf8');
    res.setHeader('Content-Type', 'text/yaml');
    res.send(file);
  } catch {
    res.status(404).send('# openapi.yaml no encontrado');
  }
});

// === Healthcheck ===
app.get('/health', (_req, res) => res.json({ ok: true }));

app.use((err, req, res, _next) => {
  // Evitar exponer detalles internos en prod
  const status = err.status || 500;
  const code = err.code || 'UNHANDLED_ERROR';

  if (!res.headersSent) {
    sendProblem(res, {
      status,
      title: status >= 500 ? 'Internal Server Error' : 'Bad Request',
      detail: err.detail || err.message || 'Error no manejado',
      extras: { code },
    });
  }
});

// === 404 fallback ===
app.use((req, res) =>
  sendProblem(res, { status: 404, title: 'Not Found', detail: 'Recurso no encontrado' })
);

// === Start ===
app.listen(PORT, () =>
  console.log(`Bridge Dux escuchando en :${PORT} | base=${DUX_BASE}`)
);
