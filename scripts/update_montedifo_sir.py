#!/usr/bin/env python3
"""
Aggiorna i CSV di Monte di Fo (TOS01000916) - PRECIPITAZIONE e TEMPERATURA - dal SIR Toscana.

PERCHE SERVER-SIDE
------------------
Il SIR NON ha API REST/CORS e il grafico e un PNG generato lato server (nessuna XHR di dati).
Il download avviene dalla pagina  https://www.sir.toscana.it/consistenza-rete :
ricerca stazione -> modale -> icona download -> CSV (serie completa, formato noto).
Questo script gira in una GitHub Action (niente CORS), riscarica il CSV completo e fonde i
giorni nuovi/cambiati nei file in dati/.

>>> UNICA COSA DA FARE UNA VOLTA: incollare i 2 URL di download <<<
Aprire consistenza-rete, cercare "Monte di Fo", aprire il modale, premere F12 -> scheda
"Network", cliccare l'icona di download (una volta col sensore PLUVIOMETRO selezionato, una
col TERMOMETRO): copiare l'URL della richiesta che parte e incollarlo qui sotto.
(In alternativa passarli via variabili d'ambiente SIR_PRECIP_URL / SIR_TEMP_URL.)
"""
import csv, os, re, sys, urllib.request

BASE = os.path.join(os.path.dirname(__file__), "..", "dati")
HEADERS = {"User-Agent": "Mozilla/5.0 (dati_idro updater)"}

# -- URL di download (da catturare dalla Network tab di consistenza-rete) --
SIR_PRECIP_URL = os.environ.get("SIR_PRECIP_URL", "")
SIR_TEMP_URL   = os.environ.get("SIR_TEMP_URL", "")

JOBS = [
    (SIR_PRECIP_URL, "MonteDiFo_precip_2004-2026.csv", "precip"),
    (SIR_TEMP_URL,   "MonteDiFo_temp_1992-2026.csv",   "temp"),
]


def fetch(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.read().decode("utf-8", "replace")


def clean_num(x):
    x = re.sub(r'[NRIVP@\s"]', "", x or "").replace(",", ".")
    if x in ("", "-9999"):
        return None
    try:
        return round(float(x), 1)
    except ValueError:
        return None


def parse_sir_csv(text, kind):
    """precip -> {date: mm}; temp -> {date: (tmin, tmedia, tmax)}."""
    out = {}
    for line in text.splitlines():
        m = re.match(r'^"?(\d{2})/(\d{2})/(\d{4})"?;(.*)$', line.strip())
        if not m:
            continue
        dd, mm, yyyy, rest = m.groups()
        cols = rest.split(";")
        iso = f"{yyyy}-{mm}-{dd}"
        if kind == "precip":
            v = clean_num(cols[0]) if cols else None
            if v is not None:
                out[iso] = v
        else:
            tmax = clean_num(cols[0]) if len(cols) > 0 else None
            tmin = clean_num(cols[1]) if len(cols) > 1 else None
            if tmax is None and tmin is None:
                continue
            tmed = round((tmax + tmin) / 2, 1) if (tmax is not None and tmin is not None) else ""
            out[iso] = (("" if tmin is None else tmin), tmed, ("" if tmax is None else tmax))
    return out


def load_existing(path, kind):
    rows = {}
    if os.path.exists(path):
        with open(path, newline="") as f:
            r = csv.reader(f, delimiter=";")
            next(r, None)
            for row in r:
                if not row or not row[0]:
                    continue
                rows[row[0]] = tuple(row[1:]) if kind == "temp" else row[1]
    return rows


def save(path, rows, kind):
    with open(path, "w", newline="") as f:
        f.write("Data;Tmin;Tmedia;Tmax\n" if kind == "temp" else "Data;Precip_mm\n")
        for d in sorted(rows):
            if kind == "temp":
                tn, tm, tx = rows[d]
                f.write(f"{d};{tn};{tm};{tx}\n")
            else:
                f.write(f"{d};{rows[d]}\n")


def run_job(url, fname, kind):
    path = os.path.join(BASE, fname)
    if not url:
        print(f"[SKIP] {fname}: URL non impostato (vedi header).")
        return
    try:
        text = fetch(url)
    except Exception as e:
        print(f"[WARN] {fname}: fetch fallito ({e}) - file invariato.", file=sys.stderr)
        return
    new = parse_sir_csv(text, kind)
    if not new:
        print(f"[WARN] {fname}: nessun dato estratto (controlla URL/formato).", file=sys.stderr)
        return
    existing = load_existing(path, kind)
    changed = 0
    for d, v in new.items():
        cur = existing.get(d)
        nv = tuple(str(x) for x in v) if kind == "temp" else str(v)
        cv = tuple(str(x) for x in cur) if (kind == "temp" and cur) else cur
        if cv != nv:
            existing[d] = v
            changed += 1
    if changed:
        save(path, existing, kind)
        print(f"[OK] {fname}: {changed} giorni aggiornati. Ultimo: {max(new)}")
    else:
        print(f"[OK] {fname}: nessuna novita.")


def main():
    for url, fname, kind in JOBS:
        run_job(url, fname, kind)
    return 0


if __name__ == "__main__":
    sys.exit(main())
