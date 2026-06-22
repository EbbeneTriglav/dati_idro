#!/usr/bin/env python3
"""
Aggiorna il CSV storico di Monte di Fò (TOS01000916) con i dati recenti del SIR Toscana.

CONTESTO
--------
Il SIR Toscana NON espone un'API REST pubblica con CORS (a differenza di ARPA Lombardia
Socrata), quindi il fetch lato browser non è possibile. Questo script gira LATO SERVER
dentro una GitHub Action (nessun limite CORS) e riscrive/aggiorna il CSV in dati/, che la
pagina montedifo.html carica come storico.

Endpoint per-stazione (dati real-time, non validati):
  https://www.cfr.toscana.it/monitoraggio/dettaglio.php?id=TOS01000916&type=pluvio_men
  (mirror: www.sir.toscana.it/monitoraggio/dettaglio.php?...)

NOTA IMPORTANTE — DA CONFERMARE UNA VOLTA
-----------------------------------------
Il grafico giornaliero è disegnato lato client a partire da una serie dati caricata via
JS/AJAX. L'URL esatto della serie va confermato UNA VOLTA aprendo la pagina del grafico nel
browser → F12 → scheda "Network" → filtra per XHR/Fetch mentre il grafico si carica: vedrai
la richiesta che restituisce i dati (di solito un .php o .json/.csv con i punti data/valore).
Incolla quell'URL in SIR_DATA_URL qui sotto e adatta parse_series() al formato restituito.

Finché non è confermato, lo script prova a estrarre la serie dall'HTML della pagina del
grafico (regex su coppie data/valore): funziona se i dati sono inline; altrimenti esce
senza modificare il CSV (così non rompe nulla).
"""
import csv, os, re, sys, urllib.request, datetime as dt

STATION   = "TOS01000916"
CSV_PATH  = os.path.join(os.path.dirname(__file__), "..", "dati", "MonteDiFo_precip_2004-2026.csv")
# Pagina grafico giornaliero (contenitore). Se hai l'URL diretto della serie, mettilo qui:
SIR_DATA_URL = f"https://www.cfr.toscana.it/monitoraggio/dettaglio.php?id={STATION}&type=pluvio_men"
HEADERS = {"User-Agent": "Mozilla/5.0 (dati_idro updater; +github actions)"}


def fetch(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "replace")


def parse_series(html):
    """Estrae coppie (YYYY-MM-DD, mm) dall'HTML del grafico.
    Cerca pattern tipici dei grafici SIR: array JS [[ms,val],...] o 'dd/mm/yyyy';val.
    ADATTA questa funzione al formato reale una volta confermato l'endpoint."""
    out = {}
    # pattern A: [timestamp_ms, valore]
    for ms, val in re.findall(r"\[\s*(\d{12,13})\s*,\s*([-\d.]+)\s*\]", html):
        d = dt.datetime.utcfromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d")
        try:
            out[d] = round(float(val), 1)
        except ValueError:
            pass
    # pattern B: 'dd/mm/yyyy ...';valore  (export CSV/tabellare SIR)
    for dd, mm, yyyy, val in re.findall(r"(\d{2})/(\d{2})/(\d{4})[^;\n]*[;,]\s*([-\d.]+)", html):
        out[f"{yyyy}-{mm}-{dd}"] = round(float(val), 1)
    return out


def load_csv(path):
    rows = {}
    if os.path.exists(path):
        with open(path, newline="") as f:
            r = csv.reader(f, delimiter=";")
            next(r, None)
            for row in r:
                if len(row) >= 2 and row[0]:
                    rows[row[0]] = row[1]
    return rows


def save_csv(path, rows):
    with open(path, "w", newline="") as f:
        f.write("Data;Precip_mm\n")
        for d in sorted(rows):
            f.write(f"{d};{rows[d]}\n")


def main():
    try:
        html = fetch(SIR_DATA_URL)
    except Exception as e:
        print(f"[WARN] fetch SIR fallito: {e}", file=sys.stderr)
        return 0  # non rompere la Action
    new = parse_series(html)
    if not new:
        print("[WARN] nessun dato estratto: conferma SIR_DATA_URL/parse_series (vedi header).",
              file=sys.stderr)
        return 0
    existing = load_csv(CSV_PATH)
    added = 0
    for d, v in new.items():
        # aggiorna solo giorni nuovi o cambiati (i dati real-time non validati possono variare)
        if existing.get(d) != str(v):
            existing[d] = v
            added += 1
    if added:
        save_csv(CSV_PATH, existing)
        print(f"[OK] {added} giorni aggiornati/aggiunti. Ultimo: {max(new)}")
    else:
        print("[OK] nessuna novità.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
