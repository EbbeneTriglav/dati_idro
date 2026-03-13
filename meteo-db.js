/**
 * meteo-db.js — libreria condivisa per tutte le pagine del progetto meteo
 * Gestisce IndexedDB, parsing CSV/Excel, utilità comuni.
 * Incluso come <script src="meteo-db.js"> in ogni pagina.
 */
'use strict';

// ─── IndexedDB ───────────────────────────────────────────────────────────────
const DB_NAME    = 'meteoDB';
const DB_VERSION = 3;
let _db = null;

async function openDB() {
  if (_db) return _db;
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = e => {
      const db = e.target.result;
      // Stazioni registrate
      if (!db.objectStoreNames.contains('stations')) {
        const st = db.createObjectStore('stations', { keyPath: 'id' });
        st.createIndex('name', 'name', { unique: false });
      }
      // Dati giornalieri precipitazione: chiave = "stationId|YYYY-MM-DD"
      if (!db.objectStoreNames.contains('precip_daily')) {
        const pd = db.createObjectStore('precip_daily', { keyPath: 'key' });
        pd.createIndex('station', 'stationId');
        pd.createIndex('date',    'date');
      }
      // Dati intensità massima oraria giornaliera: chiave = "stationId|YYYY-MM-DD"
      if (!db.objectStoreNames.contains('intensity_daily')) {
        const id = db.createObjectStore('intensity_daily', { keyPath: 'key' });
        id.createIndex('station', 'stationId');
      }
      // Dati temperatura (min, max, mean) giornalieri
      if (!db.objectStoreNames.contains('temp_daily')) {
        const td = db.createObjectStore('temp_daily', { keyPath: 'key' });
        td.createIndex('station', 'stationId');
      }
      // Dati API live (salvati per cache offline): chiave = "stationId|sensorId|YYYY-MM-DD"
      if (!db.objectStoreNames.contains('live_cache')) {
        const lc = db.createObjectStore('live_cache', { keyPath: 'key' });
        lc.createIndex('station', 'stationId');
        lc.createIndex('savedAt', 'savedAt');
      }
      // Metadati upload (nome file, data, statistiche)
      if (!db.objectStoreNames.contains('uploads')) {
        db.createObjectStore('uploads', { keyPath: 'id', autoIncrement: true });
      }
    };
    req.onsuccess = e => { _db = e.target.result; resolve(_db); };
    req.onerror   = e => reject(e.target.error);
  });
}

async function txGet(store, key) {
  const db = await openDB();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readonly');
    const req = tx.objectStore(store).get(key);
    req.onsuccess = () => res(req.result);
    req.onerror   = () => rej(req.error);
  });
}

async function txGetAll(store, indexName, indexValue) {
  const db = await openDB();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readonly');
    const os = tx.objectStore(store);
    const req = indexName ? os.index(indexName).getAll(indexValue) : os.getAll();
    req.onsuccess = () => res(req.result);
    req.onerror   = () => rej(req.error);
  });
}

async function txPutAll(store, records) {
  if (!records.length) return;
  const db = await openDB();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    const os = tx.objectStore(store);
    records.forEach(r => os.put(r));
    tx.oncomplete = () => res();
    tx.onerror    = () => rej(tx.error);
  });
}

async function txDelete(store, key) {
  const db = await openDB();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    tx.objectStore(store).delete(key);
    tx.oncomplete = () => res();
    tx.onerror    = () => rej(tx.error);
  });
}

async function txClearByIndex(store, indexName, indexValue) {
  const db = await openDB();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    const os = tx.objectStore(store);
    const req = os.index(indexName).openCursor(IDBKeyRange.only(indexValue));
    req.onsuccess = e => {
      const cursor = e.target.result;
      if (cursor) { cursor.delete(); cursor.continue(); }
      else res();
    };
    req.onerror = () => rej(req.error);
  });
}

// ─── Stazioni ─────────────────────────────────────────────────────────────────
const BUILTIN_STATIONS = {
  'cornalita': {
    id: 'cornalita', name: 'San Giovanni Bianco – Cornalita',
    lat: 45.7155, lon: 9.6567, alt: 622,
    province: 'BG', comune: 'San Giovanni Bianco',
    page: 'cornalita.html',
    sensors: {
      PP:  { id: 2278, label: 'Precipitazione',    unit: 'mm',   color: '#4fc3f7' },
      T:   { id: 2270, label: 'Temperatura',        unit: '°C',   color: '#ffb347' },
      UR:  { id: 2271, label: 'Umidità Relativa',   unit: '%',    color: '#4caf82' },
      RG:  { id: 2275, label: 'Radiazione Globale', unit: 'W/m²', color: '#ffd54f' },
      DV:  { id: 2277, label: 'Direzione Vento',    unit: '°',    color: '#b39ddb' },
      VV:  { id:11743, label: 'Velocità Vento',     unit: 'm/s',  color: '#80cbc4' },
    },
    builtin: true
  },
  'brembilla': {
    id: 'brembilla', name: 'Brembilla',
    lat: 45.8254, lon: 9.6117, alt: 468,
    province: 'BG', comune: 'Brembilla',
    page: 'brembilla.html',
    sensors: {
      PP: { id: 14499, label: 'Precipitazione', unit: 'mm',  color: '#4fc3f7' },
      T:  { id: 14500, label: 'Temperatura',    unit: '°C',  color: '#ffb347' },
    },
    builtin: true
  }
};

async function getAllStations() {
  const custom = await txGetAll('stations');
  const map = { ...BUILTIN_STATIONS };
  custom.forEach(s => { map[s.id] = s; });
  return Object.values(map);
}

async function saveStation(station) {
  await txPutAll('stations', [station]);
}

async function deleteStation(stationId) {
  await txDelete('stations', stationId);
  for (const store of ['precip_daily','intensity_daily','temp_daily','live_cache']) {
    await txClearByIndex(store, 'station', stationId);
  }
}

// ─── Dati precipitazione ──────────────────────────────────────────────────────

/**
 * Salva array di record precipitazione in DB.
 * records = [{stationId, date:'YYYY-MM-DD', mm, src:'csv'|'live'|'api'}]
 * src='live' ha priorità e sovrascrive; src='csv' solo riempie gap.
 */
async function savePrecipRecords(stationId, records, src='csv') {
  const existing = await getPrecipByStation(stationId);
  const existMap = {};
  existing.forEach(r => existMap[r.date] = r);

  const toSave = [];
  for (const r of records) {
    if (!r.date || r.mm === null || isNaN(r.mm)) continue;
    if (src === 'live' || existMap[r.date] === undefined) {
      toSave.push({ key: `${stationId}|${r.date}`, stationId, date: r.date, mm: r.mm, src });
    }
  }
  await txPutAll('precip_daily', toSave);
}

async function getPrecipByStation(stationId) {
  return txGetAll('precip_daily', 'station', stationId);
}

async function saveLiveCache(stationId, sensorId, records) {
  // records = [{date:'YYYY-MM-DD', value, unit}]
  const savedAt = new Date().toISOString();
  const toSave = records.map(r => ({
    key: `${stationId}|${sensorId}|${r.date}`,
    stationId, sensorId, date: r.date, value: r.value, unit: r.unit, savedAt
  }));
  await txPutAll('live_cache', toSave);
}

async function getLiveCache(stationId, sensorId) {
  return txGetAll('live_cache', 'station', stationId)
    .then(rows => rows.filter(r => r.sensorId === sensorId));
}

// Intensità massima
async function saveIntensityRecords(stationId, records) {
  const toSave = records.map(r => ({
    key: `${stationId}|${r.date}`, stationId, date: r.date,
    maxIntensity: r.maxIntensity, src: r.src || 'csv'
  })).filter(r => r.date && !isNaN(r.maxIntensity));
  await txPutAll('intensity_daily', toSave);
}

async function getIntensityByStation(stationId) {
  return txGetAll('intensity_daily', 'station', stationId);
}

// Temperatura
async function saveTempRecords(stationId, records) {
  const toSave = records.map(r => ({
    key: `${stationId}|${r.date}`, stationId, date: r.date,
    tMin: r.tMin ?? null, tMax: r.tMax ?? null, tMean: r.tMean ?? null, src: r.src || 'csv'
  })).filter(r => r.date);
  await txPutAll('temp_daily', toSave);
}

async function getTempByStation(stationId) {
  return txGetAll('temp_daily', 'station', stationId);
}

// ─── Qualità dati — filtro anni invalidi ─────────────────────────────────────
/**
 * Dato un oggetto {year → [mm per mese]}, ritorna un Set di anni da escludere.
 * Criteri:
 *  - Anno con totale annuo = 0
 *  - Anno con ≥ 3 mesi a zero precipitazione (in mesi tipicamente piovosi)
 *  - Anno con ≥ 2 mesi consecutivi a zero in qualsiasi stagione
 */
function detectInvalidYears(ppByYearMonth) {
  const invalid = new Set();
  for (const [y, months] of Object.entries(ppByYearMonth)) {
    const total = months.reduce((a,b) => a + (b||0), 0);
    if (total === 0) { invalid.add(Number(y)); continue; }

    const zeroMonths = months.filter(v => (v||0) === 0).length;
    if (zeroMonths >= 4) { invalid.add(Number(y)); continue; }

    // 2+ mesi consecutivi a zero (esclude luglio-agosto nelle Alpi)
    let consecutive = 0;
    for (let m = 0; m < 12; m++) {
      if ((months[m]||0) === 0) { consecutive++; if (consecutive >= 3) break; }
      else consecutive = 0;
    }
    if (consecutive >= 3) { invalid.add(Number(y)); continue; }
  }
  return invalid;
}

// ─── Parser CSV/Excel ─────────────────────────────────────────────────────────

/**
 * Rileva automaticamente il tipo di file e lo parsifica.
 * Ritorna { precip, intensity, temp } → array di record per stationId.
 */
async function parseUploadedFile(file) {
  const ext = file.name.toLowerCase().split('.').pop();
  if (ext === 'csv' || ext === 'txt') {
    const text = await file.text();
    return parseCSVAuto(text, file.name);
  } else {
    const buf = await file.arrayBuffer();
    return parseXLSXAuto(new Uint8Array(buf), file.name);
  }
}

function parseCSVAuto(text, filename='') {
  const lines = text.trim().split('\n').filter(l => l.trim());
  if (!lines.length) return { precip:[], intensity:[], temp:[] };

  const sep = lines[0].includes(';') ? ';' : ',';
  const hdr = lines[0].split(sep).map(h => h.replace(/"/g,'').trim().toLowerCase());

  const iDate  = findColIdx(hdr, ['data','date','giorno','day']);
  const iMm    = findColIdx(hdr, ['valore','value','mm','precipit','pioggia','rain','prec']);
  const iIntens= findColIdx(hdr, ['intensit','maxint','int_max','imax','max_int','intensità']);
  const iTMin  = findColIdx(hdr, ['tmin','t_min','temp_min','temperatura_min','min']);
  const iTMax  = findColIdx(hdr, ['tmax','t_max','temp_max','temperatura_max','max']);
  const iTMean = findColIdx(hdr, ['tmean','tmedia','t_mean','temp_med','media','mean']);

  const precip = [], intensity = [], temp = [];

  for (let i=1; i<lines.length; i++) {
    const cols = lines[i].split(sep).map(c => c.replace(/"/g,'').trim());
    if (!cols[0]) continue;

    let dateStr = iDate >= 0 ? cols[iDate] : cols[0];
    dateStr = normalizeDate(dateStr.split(' ')[0]);
    if (!dateStr) continue;

    if (iMm >= 0) {
      const mm = parseFloat((cols[iMm]||'').replace(',','.'));
      if (!isNaN(mm) && mm >= 0) precip.push({ date: dateStr, mm });
    }
    if (iIntens >= 0) {
      const v = parseFloat((cols[iIntens]||'').replace(',','.'));
      if (!isNaN(v) && v >= 0) intensity.push({ date: dateStr, maxIntensity: v });
    }
    if (iTMin >= 0 || iTMax >= 0 || iTMean >= 0) {
      const tMin  = iTMin  >= 0 ? parseFloat((cols[iTMin]||'').replace(',','.'))  : null;
      const tMax  = iTMax  >= 0 ? parseFloat((cols[iTMax]||'').replace(',','.'))  : null;
      const tMean = iTMean >= 0 ? parseFloat((cols[iTMean]||'').replace(',','.')) : null;
      if (tMin !== null || tMax !== null || tMean !== null) {
        temp.push({ date: dateStr,
          tMin:  !isNaN(tMin)  ? tMin  : null,
          tMax:  !isNaN(tMax)  ? tMax  : null,
          tMean: !isNaN(tMean) ? tMean : null });
      }
    }
  }
  return { precip, intensity, temp };
}

function parseXLSXAuto(buffer, filename='') {
  const wb   = XLSX.read(buffer, { type: 'array' });
  const ws   = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1 });
  if (!rows.length) return { precip:[], intensity:[], temp:[] };

  const hdr = rows[0].map(h => String(h||'').toLowerCase().trim());
  const iDate  = findColIdx(hdr, ['data','date','giorno','day']);
  const iMm    = findColIdx(hdr, ['valore','value','mm','precipit','pioggia','rain','prec']);
  const iIntens= findColIdx(hdr, ['intensit','maxint','int_max','imax','max_int','intensità']);
  const iTMin  = findColIdx(hdr, ['tmin','t_min','temp_min','temperatura_min','min']);
  const iTMax  = findColIdx(hdr, ['tmax','t_max','temp_max','temperatura_max','max']);
  const iTMean = findColIdx(hdr, ['tmean','tmedia','t_mean','temp_med','media','mean']);

  const precip=[], intensity=[], temp=[];

  for (let i=1; i<rows.length; i++) {
    const row = rows[i];
    let rawDate = iDate >= 0 ? row[iDate] : row[0];
    if (!rawDate) continue;

    // Excel serial
    if (typeof rawDate === 'number') {
      const d = XLSX.SSF.parse_date_code(rawDate);
      rawDate = `${d.y}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;
    } else {
      rawDate = normalizeDate(String(rawDate).trim().split(' ')[0]);
    }
    if (!rawDate) continue;

    if (iMm >= 0) {
      const mm = parseFloat(String(row[iMm]||0).replace(',','.'));
      if (!isNaN(mm) && mm >= 0) precip.push({ date: rawDate, mm });
    }
    if (iIntens >= 0) {
      const v = parseFloat(String(row[iIntens]||0).replace(',','.'));
      if (!isNaN(v) && v >= 0) intensity.push({ date: rawDate, maxIntensity: v });
    }
    if (iTMin >= 0 || iTMax >= 0 || iTMean >= 0) {
      const tMin  = iTMin  >= 0 ? parseFloat(String(row[iTMin] ||'').replace(',','.')) : null;
      const tMax  = iTMax  >= 0 ? parseFloat(String(row[iTMax] ||'').replace(',','.')) : null;
      const tMean = iTMean >= 0 ? parseFloat(String(row[iTMean]||'').replace(',','.')) : null;
      temp.push({ date: rawDate,
        tMin:  tMin  !== null && !isNaN(tMin)  ? tMin  : null,
        tMax:  tMax  !== null && !isNaN(tMax)  ? tMax  : null,
        tMean: tMean !== null && !isNaN(tMean) ? tMean : null });
    }
  }
  return { precip, intensity, temp };
}

function findColIdx(hdr, keywords) {
  for (const kw of keywords) {
    const i = hdr.findIndex(h => h.includes(kw));
    if (i >= 0) return i;
  }
  return -1;
}

function normalizeDate(s) {
  if (!s) return null;
  s = s.trim();
  // dd/MM/yyyy
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
    const [d,m,y] = s.split('/');
    return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
  }
  // yyyy/MM/dd
  if (/^\d{4}\/\d{2}\/\d{2}$/.test(s)) return s.replace(/\//g,'-');
  // yyyy-MM-dd already
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  // dd-MM-yyyy
  if (/^\d{1,2}-\d{1,2}-\d{4}$/.test(s)) {
    const [d,m,y] = s.split('-');
    return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
  }
  return null;
}

// ─── Download CSV / Excel ─────────────────────────────────────────────────────

function downloadCSV(data, filename) {
  const blob = new Blob([data], { type: 'text/csv;charset=utf-8;' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a'); a.href=url; a.download=filename; a.click();
  setTimeout(() => URL.revokeObjectURL(url), 2000);
}

function downloadXLSX(sheets, filename) {
  // sheets = [{name, data: [[row]]}]
  const wb = XLSX.utils.book_new();
  sheets.forEach(s => {
    const ws = XLSX.utils.aoa_to_sheet(s.data);
    XLSX.utils.book_append_sheet(wb, ws, s.name);
  });
  XLSX.writeFile(wb, filename);
}

// ─── Utilità date ─────────────────────────────────────────────────────────────
const MONTHS_IT = ['Gen','Feb','Mar','Apr','Mag','Giu','Lug','Ago','Set','Ott','Nov','Dic'];
const fmt  = (v, d=1) => v == null || isNaN(v) ? '—' : Number(v).toFixed(d);
const isoToday = () => new Date().toISOString().slice(0,10);

function isoNDaysAgo(n) {
  const d = new Date(); d.setDate(d.getDate()-n);
  return d.toISOString().slice(0,10);
}

// ─── Socrata API ─────────────────────────────────────────────────────────────
const SOCRATA_BASE = 'https://www.dati.lombardia.it/resource/647i-nhxk.json';

async function socrataLastN(sensorId, n=50000) {
  const url = `${SOCRATA_BASE}?idsensore=${sensorId}&$limit=${n}&$order=data+DESC`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Socrata HTTP ${r.status} sensor ${sensorId}`);
  return r.json();
}

async function socrataRange(sensorId, startDate, endDate, limit=50000) {
  const where = encodeURIComponent(
    `idsensore=${sensorId} AND data >= '${startDate}T00:00:00.000' AND data <= '${endDate}T23:59:59.999'`
  );
  const url = `${SOCRATA_BASE}?$where=${where}&$limit=${limit}&$order=data ASC`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Socrata HTTP ${r.status}`);
  return r.json();
}

/**
 * Aggrega righe Socrata (orarie/sub-orarie) in giornaliero.
 * Per precipitazione: somma, per temperatura: min/max/mean.
 */
function aggregateSocrataDaily(rows, type='precip') {
  const byDate = {};
  for (const r of rows) {
    const d = (r.data||'').slice(0,10);
    const v = parseFloat(r.valore ?? 0);
    if (!d || isNaN(v)) continue;
    if (!byDate[d]) byDate[d] = [];
    byDate[d].push(v);
  }
  return Object.entries(byDate).map(([date, vals]) => {
    if (type === 'precip') {
      return { date, mm: vals.reduce((a,b)=>a+b,0) };
    } else {
      const sum = vals.reduce((a,b)=>a+b,0);
      return { date, tMin: Math.min(...vals), tMax: Math.max(...vals), tMean: sum/vals.length };
    }
  });
}

// Esponi tutto su window
window.MeteoDb = {
  openDB, getAllStations, saveStation, deleteStation,
  savePrecipRecords, getPrecipByStation,
  saveIntensityRecords, getIntensityByStation,
  saveTempRecords, getTempByStation,
  saveLiveCache, getLiveCache,
  detectInvalidYears,
  parseUploadedFile,
  aggregateSocrataDaily,
  socrataLastN, socrataRange,
  downloadCSV, downloadXLSX,
  MONTHS_IT, fmt, isoToday, isoNDaysAgo,
  BUILTIN_STATIONS,
};
