# PLACSP daily tenders by CPV (sindicacion_643) — módulo con salida HTML
# ----------------------------------------------------------------------
import csv
import datetime as dt
import html
import io
import os
import time
import zipfile
from typing import Iterable, List, Set, Dict, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests
from lxml import etree
import pandas as pd

DEFAULT_CPV = ["09330000", "45261215", "45315300"]
PLACSP_TZ = "Europe/Madrid"
ZIP_URL_TEMPLATE = (
    "https://contrataciondelestado.es/sindicacion/sindicacion_643/"
    "licitacionesPerfilesContratanteCompleto3_{yyyymm}.zip"
)

# --- red con reintentos ---
def http_get_bytes(url: str, timeout: int = 120, retries: int = 3, backoff: float = 1.5) -> bytes:
    last_exc = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.content
        except Exception as exc:
            last_exc = exc
            if i < retries - 1:
                time.sleep(backoff ** i)
    raise RuntimeError(f"Error descargando {url}: {last_exc}") from last_exc

# --- XML tolerante ---
_PARSER = etree.XMLParser(recover=True, resolve_entities=False, huge_tree=True)

def iter_entries(atom_bytes: bytes):
    root = etree.fromstring(atom_bytes, parser=_PARSER)
    return root.xpath("//*[local-name()='entry']")

def text1(node: etree._Element, xpath_expr: str) -> str:
    res = node.xpath(xpath_expr)
    if not res:
        return ""
    val = res[0]
    return val if isinstance(val, str) else (val.text or "")

def texts(node: etree._Element, xpath_expr: str) -> List[str]:
    res = node.xpath(xpath_expr)
    out = []
    for v in res:
        out.append(v if isinstance(v, str) else (v.text or ""))
    return [x for x in out if x is not None]

def entry_is_for_date(entry: etree._Element, iso_date: str) -> bool:
    upd = text1(entry, "string(./*[local-name()='updated'])")
    return bool(upd) and upd.startswith(iso_date)

def entry_cpv_codes(entry: etree._Element) -> List[str]:
    return texts(entry, ".//*[local-name()='ItemClassificationCode']/text()")

def extract_fields(entry: etree._Element) -> Dict[str, str]:
    title = text1(entry, "string(./*[local-name()='title'])")
    updated = text1(entry, "string(./*[local-name()='updated'])")
    link = text1(entry, "string(./*[local-name()='link']/@href)")
    expediente = text1(entry, "string(.//*[local-name()='ContractFolderID'])")
    organo = text1(entry, "string(.//*[local-name()='ContractingPartyName'])")
    importe = text1(entry, "string(.//*[local-name()='TotalAmount'])")
    estado = text1(entry, "string(.//*[local-name()='ContractFolderStatus'])")
    cpvs = entry_cpv_codes(entry)
    cpv_concat = ";".join(sorted(set(cpvs)))
    return {
        "expediente": expediente,
        "objeto": title,
        "organo": organo,
        "estado": estado,
        "importe": importe,
        "cpv": cpv_concat,
        "fecha_updated": updated,
        "enlace": link,
    }

def month_zip_url(target_date: dt.date) -> str:
    return ZIP_URL_TEMPLATE.replace("{yyyymm}", target_date.strftime("%Y%m"))

def process_zip_bytes(zip_bytes: bytes, iso_date: str, cpv_targets: Set[str]) -> List[Dict[str, str]]:
    found: List[Dict[str, str]] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".atom"):
                continue
            try:
                atom = zf.read(name)
            except KeyError:
                continue
            for e in iter_entries(atom):
                if not entry_is_for_date(e, iso_date):
                    continue
                cpvs = set(entry_cpv_codes(e))
                if not (cpvs & cpv_targets):
                    continue
                found.append(extract_fields(e))
    return found

def write_csv(rows: List[Dict[str, str]], path: str) -> None:
    cols = ["expediente","objeto","organo","estado","importe","cpv","fecha_updated","enlace"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})

def resolve_target_date(iso_date_or_none: Optional[str] = None) -> dt.date:
    if iso_date_or_none:
        return dt.date.fromisoformat(iso_date_or_none)
    if ZoneInfo is None:
        return dt.datetime.utcnow().date()
    return dt.datetime.now(ZoneInfo(PLACSP_TZ)).date()

def run_placsp(
    date_iso: Optional[str] = None,
    cpv: Optional[List[str]] = None,
    out_path: Optional[str] = None,
    verbose: bool = True,
):
    """
    Ejecuta la descarga/filtrado para una fecha y CPV dados.
    Devuelve (df, out_path_generado, num_filas)
    """
    target = resolve_target_date(date_iso)
    iso_date = target.isoformat()
    cpv_targets: Set[str] = {c.strip() for c in (cpv or DEFAULT_CPV) if c and c.strip().isdigit()}
    cpv_targets = {c.zfill(8) for c in cpv_targets}
    if not cpv_targets:
        raise ValueError("Sin CPV válidos (8 dígitos).")

    out_path = out_path or os.path.abspath(f"placsp_{iso_date}_cpv.csv")
    url = month_zip_url(target)

    if verbose:
        print(f"[i] Fecha objetivo: {iso_date} ({PLACSP_TZ})")
        print(f"[i] CPV objetivo  : {', '.join(sorted(cpv_targets))}")
        print(f"[i] Descargando ZIP: {url}")

    zip_bytes = http_get_bytes(url)

    if verbose:
        print("[i] Procesando .atom del ZIP…")

    rows = process_zip_bytes(zip_bytes, iso_date, cpv_targets)
    df = pd.DataFrame(rows, columns=["expediente","objeto","organo","estado","importe","cpv","fecha_updated","enlace"])
    if not df.empty:
        df = df.sort_values(["organo","expediente"], na_position="last")

    write_csv(rows, out_path)  # por si quieres conservar histórico en artefactos

    if verbose:
        print(f"[ok] {len(rows)} licitaciones encontradas para {iso_date}.")
        print(f"[ok] CSV (opcional): {out_path}")

    return df, out_path, len(rows)

# ---------- NUEVO: HTML ----------
def render_html_report(
    df: pd.DataFrame,
    date_iso: str,
    cpv_targets: List[str],
    title_prefix: str = "Licitaciones PLACSP por CPV",
) -> str:
    """
    Devuelve HTML listo para enviar por correo (inlines CSS).
    """
    total = 0 if df is None or df.empty else len(df)
    cpv_txt = ", ".join(sorted({c.zfill(8) for c in cpv_targets})) if cpv_targets else "—"

    def esc(x: Optional[str]) -> str:
        return html.escape("" if pd.isna(x) else str(x))

    # filas
    rows_html = ""
    if total:
        for _, r in df.iterrows():
            rows_html += f"""
            <tr>
              <td>{esc(r['organo'])}</td>
              <td>{esc(r['expediente'])}</td>
              <td>{esc(r['estado'])}</td>
              <td style="text-align:right">{esc(r['importe'])}</td>
              <td>{esc(r['cpv'])}</td>
              <td>{esc(r['fecha_updated'])}</td>
              <td><a href="{esc(r['enlace'])}" target="_blank" rel="noopener noreferrer">Ver</a></td>
              <td>{esc(r['objeto'])}</td>
            </tr>"""
    else:
        rows_html = """<tr><td colspan="8" style="text-align:center;padding:16px;color:#666;">
        No se han encontrado licitaciones para los CPV objetivo en la fecha indicada.</td></tr>"""

    # HTML final
    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>{html.escape(title_prefix)} — {html.escape(date_iso)}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,"Noto Sans",Arial,sans-serif;line-height:1.35;margin:0;background:#f7f7f8;color:#111;}}
  .wrap{{max-width:960px;margin:0 auto;padding:24px;}}
  h1{{margin:0 0 8px 0;font-size:20px}}
  .meta{{color:#555;margin:0 0 16px 0;font-size:14px}}
  table{{border-collapse:collapse;width:100%;background:#fff;border:1px solid #e5e7eb}}
  th,td{{border-bottom:1px solid #eee;padding:8px;vertical-align:top;font-size:14px}}
  th{{text-align:left;background:#fafafa}}
  .footer{{margin-top:16px;color:#666;font-size:12px}}
  .badge{{display:inline-block;background:#111;color:#fff;border-radius:999px;padding:2px 8px;font-size:12px}}
</style>
</head>
<body>
  <div class="wrap">
    <h1>{html.escape(title_prefix)} <span class="badge">{total}</span></h1>
    <p class="meta"><strong>Fecha:</strong> {html.escape(date_iso)} (Europe/Madrid) ·
    <strong>CPV:</strong> {html.escape(cpv_txt)}</p>
    <table>
      <thead>
        <tr>
          <th>Órgano</th>
          <th>Expediente</th>
          <th>Estado</th>
          <th>Importe</th>
          <th>CPV</th>
          <th>Updated</th>
          <th>Enlace</th>
          <th>Objeto</th>
        </tr>
      </thead>
      <tbody>{rows_html}
      </tbody>
    </table>
    <p class="footer">Fuente: Sindicación 643 (PLACSP). Este mensaje se generó automáticamente.</p>
  </div>
</body>
</html>"""
