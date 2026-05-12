#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Filename: dev/tools/catalogs/build_deepsky_catalogs.py
Objective: Build normalized deep-sky catalogs from master NGC/IC/Messier data.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import ssl
import tempfile
import xml.etree.ElementTree as ET
from zipfile import ZipFile
from pathlib import Path
from urllib.request import Request, urlopen

from lxml import html

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CATALOG_DIR = PROJECT_ROOT / "catalogs"
DEFAULT_MASTER = CATALOG_DIR / "ngc-ic-messier-catalog.json"

CALDWELL_URL = "https://astropixels.com/caldwell/caldwellcat.html"
HERSCHEL_URL = "https://en.wikipedia.org/wiki/Herschel_400_Catalogue"
SHARPLESS_URL = "https://www.reinervogel.net/Sharpless/Sharpless_data_e.html"
OPENGC_URL = "https://raw.githubusercontent.com/mattiaverga/OpenNGC/master/database_files/NGC.csv"
BENNETT_URL = "https://assa.saao.ac.za/sections/deep-sky/jack-bennett-catalogue/"
DUNLOP_URL = "https://assa.saao.ac.za/?p=2993"
BAMBURY_URL = "https://www.astroleague.org/wp-content/uploads/2024/02/BAM600-John-Bambury-Southern-Skies-Observing-List-V20240102.xlsx"


def _fetch_doc(url: str) -> html.HtmlElement:
    req = Request(url, headers={"User-Agent": "space-cats-catalog-builder/1.0"})
    data = urlopen(req, timeout=30, context=ssl._create_unverified_context()).read()
    return html.fromstring(data)


def _fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "space-cats-catalog-builder/1.0"})
    return urlopen(req, timeout=60, context=ssl._create_unverified_context()).read().decode("utf-8")


def _hms_to_deg(value: str) -> float:
    parts = [float(part) for part in re.split(r"[:\s]+", value.strip()) if part]
    while len(parts) < 3:
        parts.append(0.0)
    return round((parts[0] + parts[1] / 60.0 + parts[2] / 3600.0) * 15.0, 6)


def _dms_to_deg(value: str) -> float:
    text = value.strip().replace("+", "")
    sign = -1.0 if text.startswith("-") else 1.0
    text = text.lstrip("-")
    parts = [float(part) for part in re.split(r"[:\s]+", text) if part]
    while len(parts) < 3:
        parts.append(0.0)
    return round(sign * (parts[0] + parts[1] / 60.0 + parts[2] / 3600.0), 6)


def _clean_name(value: str | None) -> str:
    return " ".join(str(value or "").replace("_", " ").split())


def _ngc_key(value: str | int | None) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).upper().replace(" ", "")
    match = re.search(r"(NGC|IC)(\d+[A-Z]?)", text)
    if not match and re.fullmatch(r"\d+[A-Z]?", text):
        return f"NGC{text}"
    if not match:
        return None
    return f"{match.group(1)}{match.group(2)}"


def _m_key(value: str | None) -> str | None:
    match = re.search(r"M\s*(\d+)", str(value or ""), re.IGNORECASE)
    return f"M{int(match.group(1))}" if match else None


def _float_or_none(value) -> float | None:
    try:
        if value in (None, "", "-", "–"):
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _opengc_key(value: str | int | None) -> str | None:
    key = _ngc_key(value)
    if not key:
        return None
    prefix = key[:3]
    number = key[3:]
    if prefix in {"NGC", "IC"} and number.isdigit():
        return f"{prefix}{int(number):04d}"
    return key


def _load_opengc_index() -> dict[str, dict]:
    rows = csv.DictReader(_fetch_text(OPENGC_URL).splitlines(), delimiter=";")
    return {row["Name"]: row for row in rows if row.get("Name")}


def _aliases_from_text(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in re.split(r"[,;/]", str(value)) if item.strip()]


def _target_from_opengc(
    row: dict,
    *,
    name: str,
    catalog: str,
    source_id: str,
    aliases: list[str] | None = None,
    priority: int = -8,
    duration: int = 1200,
) -> dict:
    return {
        "name": name,
        "common_name": _clean_name(row.get("Common names")),
        "source_id": source_id,
        "aliases": sorted(set(aliases or [])),
        "ra": _hms_to_deg(row["RA"]),
        "dec": _dms_to_deg(row["Dec"]),
        "type": row.get("Type") or None,
        "object_definition": row.get("Hubble") or row.get("Type") or None,
        "catalog": catalog,
        "science_mode": "imaging",
        "max_mag": _float_or_none(row.get("V-Mag") or row.get("B-Mag")),
        "b_mag": _float_or_none(row.get("B-Mag")),
        "v_mag": _float_or_none(row.get("V-Mag")),
        "constellation": row.get("Const") or None,
        "major_axis_arcmin": _float_or_none(row.get("MajAx")),
        "minor_axis_arcmin": _float_or_none(row.get("MinAx")),
        "priority": priority,
        "duration": duration,
    }


def _master_indexes(rows: list[dict]) -> tuple[dict[str, dict], dict[str, dict]]:
    by_name: dict[str, dict] = {}
    by_messier: dict[str, dict] = {}
    for row in rows:
        key = _ngc_key(row.get("name"))
        if key:
            by_name[key] = row
        for messier in row.get("m") or []:
            mkey = _m_key(messier)
            if mkey:
                by_messier[mkey] = row
    return by_name, by_messier


def _target_from_master(
    row: dict,
    *,
    name: str,
    catalog: str,
    common_name: str = "",
    source_id: str = "",
    priority: int = -5,
    duration: int = 900,
) -> dict:
    aliases = []
    for item in row.get("m") or []:
        if item:
            aliases.append(str(item))
    if row.get("name") and row.get("name") != name:
        aliases.append(str(row["name"]))

    return {
        "name": name,
        "common_name": common_name or _clean_name(row.get("common_names")),
        "source_id": source_id or name,
        "aliases": sorted(set(aliases)),
        "ra": _hms_to_deg(str(row["ra"])),
        "dec": _dms_to_deg(str(row["dec"])),
        "type": str(row.get("type") or catalog.upper()),
        "object_definition": _clean_name(row.get("object_definition")),
        "catalog": catalog,
        "science_mode": "imaging",
        "max_mag": _float_or_none(row.get("v_mag") or row.get("b_mag")),
        "b_mag": _float_or_none(row.get("b_mag")),
        "v_mag": _float_or_none(row.get("v_mag")),
        "constellation": row.get("const"),
        "major_axis_arcmin": _float_or_none(row.get("majax")),
        "minor_axis_arcmin": _float_or_none(row.get("minax")),
        "priority": priority,
        "duration": duration,
    }


def build_messier(master_rows: list[dict]) -> list[dict]:
    _, by_messier = _master_indexes(master_rows)
    targets = []
    for idx in range(1, 111):
        key = f"M{idx}"
        row = by_messier.get(key)
        if row:
            targets.append(_target_from_master(row, name=key, catalog="messier", source_id=key))
    return targets


def build_caldwell(master_rows: list[dict]) -> list[dict]:
    by_name, _ = _master_indexes(master_rows)
    doc = _fetch_doc(CALDWELL_URL)
    tables = sorted(doc.xpath("//table"), key=lambda table: len(table.xpath(".//tr")), reverse=True)
    rows = tables[0].xpath(".//tr")[3:] if tables else []
    targets = []
    for tr in rows:
        cells = [" ".join(cell.text_content().split()) for cell in tr.xpath("./td")]
        if len(cells) < 8 or not cells[0].isdigit():
            continue
        caldwell = f"C{int(cells[0])}"
        ngc = _ngc_key(cells[1])
        row = by_name.get(ngc or "")
        if row:
            targets.append(
                _target_from_master(
                    row,
                    name=caldwell,
                    catalog="caldwell",
                    common_name=cells[10] if len(cells) > 10 else "",
                    source_id=ngc or caldwell,
                )
            )
    return targets


def build_herschel400(master_rows: list[dict]) -> list[dict]:
    by_name, _ = _master_indexes(master_rows)
    doc = _fetch_doc(HERSCHEL_URL)
    targets = []
    seen = set()
    for table in doc.xpath("//table")[9:13]:
        for tr in table.xpath(".//tr")[1:]:
            cells = [" ".join(cell.text_content().split()) for cell in tr.xpath("./td")]
            if len(cells) < 2:
                continue
            key = _ngc_key(cells[1])
            if not key or key in seen or key not in by_name:
                continue
            seen.add(key)
            targets.append(
                _target_from_master(
                    by_name[key],
                    name=key,
                    catalog="herschel400",
                    common_name=cells[2] if len(cells) > 2 else "",
                    source_id=key,
                    priority=-6,
                    duration=900,
                )
            )
    return targets


def build_sharpless() -> list[dict]:
    doc = _fetch_doc(SHARPLESS_URL)
    targets = []
    tables = sorted(doc.xpath("//table"), key=lambda table: len(table.xpath(".//tr")), reverse=True)
    rows = tables[0].xpath(".//tr")[1:] if tables else []
    for tr in rows:
        cells = [" ".join(cell.text_content().split()) for cell in tr.xpath("./td")]
        if len(cells) < 4 or not cells[0].startswith("Sh"):
            continue
        targets.append(
            {
                "name": cells[0].replace("Sh 2-", "Sh2-"),
                "common_name": cells[4] if len(cells) > 4 else "",
                "source_id": cells[0],
                "aliases": [],
                "ra": _hms_to_deg(cells[1]),
                "dec": _dms_to_deg(cells[2]),
                "type": "SHARPLESS",
                "object_definition": "Emission nebula",
                "catalog": "sharpless",
                "science_mode": "imaging",
                "max_mag": None,
                "constellation": None,
                "major_axis_arcmin": _float_or_none(cells[3]),
                "minor_axis_arcmin": None,
                "priority": -7,
                "duration": 1200,
            }
        )
    return targets


def build_bennett() -> list[dict]:
    doc = _fetch_doc(BENNETT_URL)
    table = sorted(doc.xpath("//table"), key=lambda table: len(table.xpath(".//tr")), reverse=True)[0]
    opengc = _load_opengc_index()
    targets = []
    for tr in table.xpath(".//tr")[1:]:
        cells = [" ".join(cell.text_content().split()).replace("–", "-") for cell in tr.xpath("./td")]
        if len(cells) < 8 or not cells[0].lower().startswith("ben"):
            continue
        name = cells[0].replace("Ben ", "Bennett ")
        designation = cells[1]
        source_id = cells[0]
        ra = _hms_to_deg(f"{cells[2]} {cells[3]} {cells[4]}")
        dec = _dms_to_deg(f"{cells[5]} {cells[6]} 0")
        ngc_row = opengc.get(_opengc_key(designation) or "")
        if ngc_row:
            target = _target_from_opengc(
                ngc_row,
                name=name,
                catalog="bennett",
                source_id=source_id,
                aliases=[designation],
            )
            target["ra"] = ra
            target["dec"] = dec
        else:
            target = {
                "name": name,
                "common_name": "",
                "source_id": source_id,
                "aliases": [designation],
                "ra": ra,
                "dec": dec,
                "type": None,
                "object_definition": None,
                "catalog": "bennett",
                "science_mode": "imaging",
                "max_mag": None,
                "constellation": cells[7],
                "major_axis_arcmin": None,
                "minor_axis_arcmin": None,
                "priority": -8,
                "duration": 1200,
            }
        targets.append(target)
    return targets


def build_dunlop() -> list[dict]:
    doc = _fetch_doc(DUNLOP_URL)
    table = sorted(doc.xpath("//table"), key=lambda table: len(table.xpath(".//tr")), reverse=True)[0]
    opengc = _load_opengc_index()
    targets = []
    seen = set()
    for tr in table.xpath(".//tr")[1:]:
        cells = [" ".join(cell.text_content().split()) for cell in tr.xpath("./td")]
        for idx in range(0, len(cells) - 1, 2):
            dunlop_no = cells[idx]
            ngc_no = cells[idx + 1]
            if not dunlop_no.isdigit() or not ngc_no.isdigit():
                continue
            source_id = f"D {int(dunlop_no)}"
            if source_id in seen:
                continue
            row = opengc.get(_opengc_key(f"NGC {ngc_no}") or "")
            if not row:
                continue
            seen.add(source_id)
            targets.append(
                _target_from_opengc(
                    row,
                    name=f"Dunlop {int(dunlop_no)}",
                    catalog="dunlop",
                    source_id=source_id,
                    aliases=[f"NGC {int(ngc_no)}"],
                    priority=-8,
                    duration=1200,
                )
            )
    return targets


def _column_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    idx = 0
    for ch in letters:
        idx = idx * 26 + ord(ch.upper()) - ord("A") + 1
    return idx - 1


def _xlsx_rows(url: str) -> list[list[str]]:
    req = Request(url, headers={"User-Agent": "space-cats-catalog-builder/1.0"})
    data = urlopen(req, timeout=60, context=ssl._create_unverified_context()).read()
    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        tmp.write(data)
        tmp.flush()
        with ZipFile(tmp.name) as zf:
            ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
            strings = []
            if "xl/sharedStrings.xml" in zf.namelist():
                root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
                for si in root.findall("a:si", ns):
                    strings.append("".join(t.text or "" for t in si.findall(".//a:t", ns)))
            sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
            rows = []
            for row in sheet.findall(".//a:row", ns):
                values: dict[int, str] = {}
                for cell in row.findall("a:c", ns):
                    value = cell.find("a:v", ns)
                    if value is None:
                        continue
                    text = value.text or ""
                    if cell.attrib.get("t") == "s" and text:
                        text = strings[int(text)]
                    values[_column_index(cell.attrib["r"])] = text
                if values:
                    rows.append([values.get(idx, "") for idx in range(max(values) + 1)])
            return rows


def build_bambury() -> list[dict]:
    targets = []
    for row in _xlsx_rows(BAMBURY_URL)[3:]:
        if len(row) < 24 or not str(row[0]).strip().isdigit():
            continue
        try:
            name = _clean_name(row[1])
            ra = _hms_to_deg(f"{row[17]} {row[18]} {row[19]}")
            dec = _dms_to_deg(f"{row[20]}{row[21]} {row[22]} {row[23]}")
        except (ValueError, IndexError):
            continue
        aliases = _aliases_from_text(row[2])
        common_name = _clean_name(row[3])
        target = {
            "name": name,
            "common_name": common_name,
            "source_id": f"BAM {int(float(row[0]))}",
            "aliases": aliases,
            "ra": ra,
            "dec": dec,
            "type": _clean_name(row[6]) or None,
            "object_definition": _clean_name(row[13] or row[16]) or None,
            "catalog": "bambury",
            "science_mode": "imaging",
            "max_mag": _float_or_none(row[8]),
            "b_mag": None,
            "v_mag": _float_or_none(row[8]),
            "constellation": _clean_name(row[7]) or None,
            "major_axis_arcmin": _float_or_none(row[10]),
            "minor_axis_arcmin": _float_or_none(row[11]),
            "priority": -8,
            "duration": 1200,
            "best_month": _clean_name(row[24]) if len(row) > 24 else None,
        }
        targets.append(target)
    return targets


def write_catalog(name: str, targets: list[dict], source: str) -> Path:
    payload = {
        "#objective": f"Normalized {name} deep-sky catalog. Coordinates are J2000 decimal degrees.",
        "schema": "space-cats-v1",
        "source": source,
        "target_count": len(targets),
        "targets": targets,
    }
    path = CATALOG_DIR / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build normalized deep-sky secondary catalogs.")
    parser.add_argument("--master", type=Path, default=DEFAULT_MASTER)
    parser.add_argument(
        "--only",
        nargs="*",
        choices=["messier", "caldwell", "herschel400", "sharpless", "bennett", "dunlop", "bambury"],
    )
    args = parser.parse_args()

    wanted = args.only or ["messier", "caldwell", "herschel400", "sharpless", "bennett", "dunlop", "bambury"]
    needs_master = bool({"messier", "caldwell", "herschel400"} & set(wanted))
    master_rows = json.loads(args.master.read_text(encoding="utf-8")) if needs_master else []
    builders = {
        "messier": lambda: build_messier(master_rows),
        "caldwell": lambda: build_caldwell(master_rows),
        "herschel400": lambda: build_herschel400(master_rows),
        "sharpless": build_sharpless,
        "bennett": build_bennett,
        "dunlop": build_dunlop,
        "bambury": build_bambury,
    }
    sources = {
        "sharpless": SHARPLESS_URL,
        "bennett": BENNETT_URL,
        "dunlop": DUNLOP_URL,
        "bambury": BAMBURY_URL,
    }
    for name in wanted:
        targets = builders[name]()
        path = write_catalog(name, targets, str(sources.get(name, args.master)))
        print(f"{name}: {len(targets)} -> {path}")


if __name__ == "__main__":
    main()
