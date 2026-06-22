#!/usr/bin/env python3
"""
Aggiorna i CSV di Monte di Fo (TOS01000916) - PRECIPITAZIONE e TEMPERATURA - dal SIR Toscana.

FONTE DATI
----------
Pagina per-anno del SIR (tabella HTML a matrice giorni x mesi):
  https://www.sir.toscana.it/archivio/dati.php?A=<anno>&IDS=TOS01000916&IDST=pluvio  (precip)
  https://www.sir.toscana.it/archivio/dati.php?A=<anno>&IDS=TOS01000916&IDST=termo   (temp: Tmax Tmin)

Lo script gira lato server (GitHub Action, niente CORS), legge la matrice dell'ANNO CORRENTE
(il param A viene riscritto in automatico) e fonde i giorni nuovi/cambiati nei CSV in dati/.
Lo storico pre-anno-corrente resta quello gia committato.

URL impostati come Repository variables: SIR_PRECIP_URL / SIR_TEMP_URL
(possono contenere un A= qualsiasi: viene sostituito con l'anno corrente).
"""
import csv, os, re, sys, datetime as dt, urllib.request

BASE = os.path.join(os.path.dirname(__file__), "..", "dati")
HEADERS = {"User-Agent": "Mozilla/5.0 (dati_idro updater)"}
YEAR = dt.date.today().year

SIR_PRECIP_URL = os.environ.get("SIR_PRECIP_URL", "")
SIR_TEMP_URL   = os.environ.get("SIR_TEMP_URL", "")

JOBS = [
    (SIR_PRECIP_URL, "MonteDiFo_precip_1992-2026.csv", "precip"),
    (SIR_TEMP_URL,   "MonteDiFo_temp_1992-2026.csv",   "temp"),
]


def fetch(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.read().decode("utf-8", "replace")


def cells_of(html):
    """Estrae la griglia di celle (lista di righe, ogni riga = lista di testi cella)."""
    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
        cells = []
        for td in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S | re.I):
            txt = re.sub(r"<[^>]+>", " ", td)          # togli tag (br, span, ...)
            txt = txt.replace("&nbsp;", " ").replace("\xa0", " ")
            txt = re.sub(r"\s+", " ", txt).strip()
            cells.append(txt)
        if cells:
            rows.append(cells)
    return rows


def valid_date(y, m, d):
    try:
        dt.date(y, m, d); return True
    except ValueError:
        return False


def parse_sir_matrix(html, kind, year):
    """precip -> {date: mm}; temp -> {date: (tmin, tmedia, tmax)}.
    Matrice: prima cella = giorno (1-31), poi 12 celle = mesi gen..dic.
    precip: '-' = 0.0 (asciutto), vuoto = mancante. temp: cella = 'Tmax Tmin'."""
    out = {}
    for r in cells_of(html):
        if not r:
            continue
        first = r[0].strip()
        if not re.fullmatch(r"\d{1,2}", first):
            continue
        day = int(first)
        if not (1 <= day <= 31):
            continue
        for mi in range(1, 13):                         # colonna mese
            if mi >= len(r):
                break
            cell = r[mi].strip()
            if not valid_date(year, mi, day):
                continue
            iso = f"{year}-{mi:02d}-{day:02d}"
            if kind == "precip":
                if cell == "-":
                    out[iso] = 0.0
                elif cell:
                    nums = re.findall(r"-?\d+(?:[.,]\d+)?", cell)
                    if nums:
                        try:
                            out[iso] = round(float(nums[0].replace(",", ".")), 1)
                        except ValueError:
                            pass
            else:  # temp
                if not cell or cell == "-":
                    continue
                nums = re.findall(r"-?\d+(?:[.,]\d+)?", cell.replace(",", "."))
                if len(nums) >= 2:
                    tmax = float(nums[0]); tmin = float(nums[1])
                    out[iso] = (tmin, round((tmax + tmin) / 2, 1), tmax)
    return out


def load_existing(path, kind):
    rows = {}
    if os.path.exists(path):
        with open(path, newline="") as f:
            rd = csv.reader(f, delimiter=";"); next(rd, None)
            for row in rd:
                if row and row[0]:
                    rows[row[0]] = tuple(row[1:]) if kind == "temp" else row[1]
    return rows


def save(path, rows, kind):
    with open(path, "w", newline="") as f:
        f.write("Data;Tmin;Tmedia;Tmax\n" if kind == "temp" else "Data;Precip_mm\n")
        for d in sorted(rows):
            if kind == "temp":
                tn, tm, tx = rows[d]; f.write(f"{d};{tn};{tm};{tx}\n")
            else:
                f.write(f"{d};{rows[d]}\n")


def run_job(url, fname, kind):
    path = os.path.join(BASE, fname)
    if not url:
        print(f"[SKIP] {fname}: URL non impostato."); return
    url = re.sub(r"A=\d{4}", f"A={YEAR}", url)           # forza anno corrente
    if "A=" not in url:
        url += ("&" if "?" in url else "?") + f"A={YEAR}"
    try:
        html = fetch(url)
    except Exception as e:
        print(f"[WARN] {fname}: fetch fallito ({e}) - file invariato.", file=sys.stderr); return
    new = parse_sir_matrix(html, kind, YEAR)
    if not new:
        print(f"[WARN] {fname}: 0 dati estratti dalla matrice (controlla URL/struttura).", file=sys.stderr); return
    existing = load_existing(path, kind)
    changed = 0
    for d, v in new.items():
        nv = tuple(str(x) for x in v) if kind == "temp" else str(v)
        cur = existing.get(d)
        cv = tuple(str(x) for x in cur) if (kind == "temp" and cur) else cur
        if cv != nv:
            existing[d] = v; changed += 1
    if changed:
        save(path, existing, kind)
        print(f"[OK] {fname}: {changed} giorni aggiornati (anno {YEAR}). Ultimo: {max(new)}")
    else:
        print(f"[OK] {fname}: nessuna novita.")


def main():
    for url, fname, kind in JOBS:
        run_job(url, fname, kind)
    return 0


if __name__ == "__main__":
    sys.exit(main())
