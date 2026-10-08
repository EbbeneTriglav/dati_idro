#!/usr/bin/env python3
"""
update_arpa_giornaliero.py — archivio giornaliero delle stazioni ARPA Lombardia.

PERCHE'
  Le pagine (es. cornalita.html) leggevano gli ultimi mesi direttamente da ARPA
  Socrata dal browser. Socrata risponde spesso HTTP 429 (troppe richieste) alle
  chiamate anonime: in quel caso la pagina restava ferma all'ultimo file storico
  (per Cornalita: gennaio 2026) e l'anno in corso spariva dalle statistiche.
  Questo script fa le query da GitHub Actions, con retry, e salva i totali
  giornalieri nel repo: la pagina li legge come un normale file di dati.

OUTPUT  dati/<Nome>_arpa_giornaliero.csv  (separatore ';')
  data;precip_mm;intensita_max_mmh;tmin;tmax;tmean;n_obs_pp
  - giorno = data del campo 'data' di ARPA (ora solare, come lo legge la pagina);
  - precip_mm = somma misure 10' valide; vuoto se il giorno ha meno di
    MIN_OBS misure (giorno incompleto: meglio "mancante" che un totale falso);
  - intensita_max_mmh = massima somma su ora intera del giorno;
  - n_obs_pp = numero misure pluviometro del giorno (144 = giorno completo).
  A ogni run ricalcola gli ultimi REFRESH_DAYS giorni (ARPA pubblica con ritardo)
  e conserva i giorni precedenti. Se il file non esiste parte da START.

Stazioni: STATIONS qui sotto (nome file -> sensori ARPA). Per aggiungerne una
basta una riga e aggiungere il file a 'file_dati' in index.json.
Variabile opzionale SOCRATA_APP_TOKEN (secret): token gratuito dati.lombardia.it,
elimina i 429. Mai stampato.
"""

import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

SOCRATA = 'https://www.dati.lombardia.it/resource/647i-nhxk.json'
# Il dataset "realtime" 647i-nhxk contiene solo gli ULTIMI ~7 MESI. I dati piu'
# vecchi stanno nei dataset storici per grandezza (dal 2021), stesso schema
# (idsensore, data, valore). Fonte degli id: pacchetto R ARPALData (CRAN).
HISTORICAL = {
    'PP': 'https://www.dati.lombardia.it/resource/pstb-pga6.json',   # Precipitazione dal 2021
    'T':  'https://www.dati.lombardia.it/resource/w9wd-u6jh.json',   # Temperatura dal 2021
}
REALTIME_DAYS = 200      # sotto questa eta' basta il realtime (margine sui ~7 mesi)
ROOT = Path(__file__).resolve().parents[1]
DATI = ROOT / 'dati'

STATIONS = {
    'Cornalita': {'PP': 2278, 'T': 2270},
}
START = date.fromisoformat(os.environ.get('ARPA_START', '2026-01-01'))
REFRESH_DAYS = 10
MIN_OBS = 120            # su 144 attese (10'): sotto, il totale del giorno resta vuoto
FIELDS = ['data', 'precip_mm', 'intensita_max_mmh', 'tmin', 'tmax', 'tmean', 'n_obs_pp']
SOLAR = timezone(timedelta(hours=1))


def log(m):
    print(m, flush=True)


def get_json(url, tries=5):
    headers = {'User-Agent': 'dati-idro-arpa-daily', 'Accept': 'application/json'}
    tok = (os.environ.get('SOCRATA_APP_TOKEN') or '').strip()
    if tok:
        headers['X-App-Token'] = tok
    last = None
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=90) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = e
            if e.code not in (429, 500, 502, 503, 504):
                raise
            try:
                wait = float(e.headers.get('Retry-After') or 0)
            except (TypeError, ValueError):
                wait = 0
            wait = min(max(wait, 15 * (i + 1)), 90)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last, wait = e, 15 * (i + 1)
        if i < tries - 1:
            log(f'  Socrata {type(last).__name__} {getattr(last, "code", "")}: riprovo tra {wait:.0f}s')
            time.sleep(wait)
    raise last


def _query(base, sensor, cur, nxt):
    where = f"data >= '{cur.isoformat()}T00:00:00' AND data < '{nxt.isoformat()}T00:00:00'"
    qs = urllib.parse.urlencode({'idsensore': sensor, '$where': where,
                                 '$order': 'data', '$limit': 10000})
    out = {}
    for r in get_json(f'{base}?{qs}'):
        ts = str(r.get('data') or '')[:19]
        try:
            v = float(r.get('valore'))
        except (TypeError, ValueError):
            continue
        if len(ts) >= 13 and v > -900:              # -999 = dato mancante ARPA
            out[ts] = v
    return out


def fetch(sensor, d0, d1, kind='PP', today=None):
    """Misure [(giorno 'YYYY-MM-DD', ora 'YYYY-MM-DDTHH', valore)] con d0 <= giorno < d1,
    a blocchi mensili (limite righe Socrata). Per i mesi piu' vecchi di REALTIME_DAYS
    interroga anche il dataset storico (il realtime tiene solo ~7 mesi); a parita'
    di istante vale il realtime."""
    today = today or date.today()
    out, cur = [], d0
    while cur < d1:
        nxt = min(date(cur.year + (cur.month == 12), cur.month % 12 + 1, 1), d1)
        merged = {}
        n_hist = 0
        if kind in HISTORICAL and (today - cur).days > REALTIME_DAYS - 31:
            try:
                merged = _query(HISTORICAL[kind], sensor, cur, nxt)
                n_hist = len(merged)
            except Exception as e:                  # storico non essenziale
                log(f'    storico {kind} non disponibile ({type(e).__name__})')
            time.sleep(2)
        rt = _query(SOCRATA, sensor, cur, nxt)
        merged.update(rt)
        for ts in sorted(merged):
            out.append((ts[:10], ts[:13], merged[ts]))
        log(f'    sensore {sensor} {cur}..{nxt}: realtime {len(rt)}, storico {n_hist}, totale {len(merged)}')
        cur = nxt
        time.sleep(2)
    return out


def aggregate(pp, tt):
    days = {}
    hours = {}
    for d, h, v in pp:
        if v < 0:
            continue
        x = days.setdefault(d, {'pp': 0.0, 'n': 0, 't': []})
        x['pp'] += v
        x['n'] += 1
        hours[(d, h)] = hours.get((d, h), 0.0) + v
    imax = {}
    for (d, _), v in hours.items():
        imax[d] = max(imax.get(d, 0.0), v)
    for d, _, v in tt:
        days.setdefault(d, {'pp': 0.0, 'n': 0, 't': []})['t'].append(v)
    rows = {}
    for d, x in days.items():
        t = x['t']
        ok = x['n'] >= MIN_OBS
        rows[d] = {
            'data': d,
            'precip_mm': f"{x['pp']:.1f}" if ok else '',
            'intensita_max_mmh': f"{imax.get(d, 0.0):.1f}" if ok else '',
            'tmin': f'{min(t):.1f}' if t else '',
            'tmax': f'{max(t):.1f}' if t else '',
            'tmean': f'{sum(t) / len(t):.1f}' if t else '',
            'n_obs_pp': x['n'],
        }
    return rows


def main():
    today = datetime.now(SOLAR).date()
    n_err = 0
    for name, sens in STATIONS.items():
        out = DATI / f'{name}_arpa_giornaliero.csv'
        old = {}
        if out.exists():
            with out.open(newline='', encoding='utf-8') as fh:
                old = {r['data']: r for r in csv.DictReader(fh, delimiter=';')}
        d0 = START if not old else max(START, today - timedelta(days=REFRESH_DAYS))
        # buchi nel file (es. mesi non ancora nel realtime al primo run): si riparte dal primo
        first_gap = next((START + timedelta(days=i) for i in range((today - START).days)
                          if (START + timedelta(days=i)).isoformat() not in old
                          or not old[(START + timedelta(days=i)).isoformat()].get('precip_mm')), None)
        refill = os.environ.get('ARPA_REFILL') == '1' or today.weekday() == 0   # buchi: 1 volta/settimana
        if old and refill and first_gap and first_gap < d0:
            d0 = date(first_gap.year, first_gap.month, 1)
        d1 = today + timedelta(days=1)
        log(f'{name}: {d0} -> {today}')
        try:
            pp = fetch(sens['PP'], d0, d1, 'PP', today)
            tt = fetch(sens['T'], d0, d1, 'T', today) if sens.get('T') else []
        except Exception as e:
            n_err += 1
            log(f'  {name}: Socrata non risponde ({e}) - file invariato')
            continue
        new = aggregate(pp, tt)
        # un giorno gia' completo non viene sostituito da uno con meno misure
        for d, r in new.items():
            o = old.get(d)
            if o and int(o.get('n_obs_pp') or 0) > int(r['n_obs_pp']):
                continue
            old[d] = r
        with out.open('w', newline='', encoding='utf-8') as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS, delimiter=';')
            w.writeheader()
            for d in sorted(old):
                w.writerow({f: old[d].get(f, '') for f in FIELDS})
        tot = sum(float(r['precip_mm']) for r in new.values() if r['precip_mm'])
        log(f'  {name}: {len(new)} giorni aggiornati ({tot:.1f} mm), {len(old)} giorni nel file')
    return 1 if n_err == len(STATIONS) else 0


if __name__ == '__main__':
    sys.exit(main())
