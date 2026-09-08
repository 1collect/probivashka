#!/usr/bin/env python3
import argparse
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
from io import BytesIO


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8890

KZ_CLOSING_INTRO_PATTERN = (
    r"(?:Жоғарыда\s+(?:аталғанның|айтылғанның|баяндалғанның)\s+негізінде"
    r"|Жоғарыдағылардың\s+негізінде"
    r"|Жоғарыдағылдардың\s+негізінде)"
)
PDF_DASH_PATTERN = r"[-‐‑‒–—]"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PDF parse API for Probivashka.")
    parser.add_argument("--serve", action="store_true", help="Start HTTP API server.")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"HTTP host. Default: {DEFAULT_HOST}")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"HTTP port. Default: {DEFAULT_PORT}")
    return parser.parse_args()


def normalize_pdf_text(value: str) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", re.sub(r"\n+", "\n", value or "")).strip()


def collapse_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def extract_decree_date(text: str) -> str:
    match = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", text or "")
    return match.group(0) if match else ""


def extract_structured_legal_basis(text: str) -> str:
    normalized = normalize_pdf_text(text)
    search_areas: list[str] = []

    kz_guided = re.search(
        KZ_CLOSING_INTRO_PATTERN + r"[\s\S]{0,1500}?басшылыққа\s+ала\s+отырып",
        normalized,
        flags=re.IGNORECASE,
    )
    if kz_guided:
        search_areas.append(kz_guided.group(0))

    ru_guided = re.search(
        r"руководствуясь[\s\S]{0,1200}?(?=\s*ПОСТАНОВИЛ\s*\(?А?\)?\s*:)",
        normalized,
        flags=re.IGNORECASE,
    )
    if ru_guided:
        search_areas.append(ru_guided.group(0))

    search_areas.append(normalized)

    kz_subpoint_pattern = re.compile(
        rf"(?P<article>\d+(?:[-.]\d+)?)\s*{PDF_DASH_PATTERN}\s*бабы(?:ның)?\s+"
        rf"(?P<point>\d+(?:[-.]\d+)?)\s*{PDF_DASH_PATTERN}\s*тармағының\s+"
        rf"(?P<subpoint>\d+(?:[-.]\d+)?)\s*(?:\)|{PDF_DASH_PATTERN})\s*тармақшасын",
        flags=re.IGNORECASE,
    )
    ru_subpoint_pattern = re.compile(
        r"(?:подпункт(?:ом|а|у)?|пп\.?)\s*"
        r"(?P<subpoint>\d+(?:[-.]\d+)?)\s*\)?\s*,?\s*"
        r"(?:пункт(?:а|ом|у)?|п\.?)\s*"
        r"(?P<point>\d+(?:[-.]\d+)?)\s*\)?\s*,?\s*"
        r"(?:стать(?:и|ей|ю)|ст\.?)\s*"
        r"(?P<article>\d+(?:[-.]\d+)?)",
        flags=re.IGNORECASE,
    )
    ru_point_pattern = re.compile(
        r"(?:пункт(?:а|ом|у)?|п\.?)\s*"
        r"(?P<point>\d+(?:[-.]\d+)?)\s*\)?\s*,?\s*"
        r"(?:стать(?:и|ей|ю)|ст\.?)\s*"
        r"(?P<article>\d+(?:[-.]\d+)?)",
        flags=re.IGNORECASE,
    )

    for area in search_areas:
        match = kz_subpoint_pattern.search(area)
        if match:
            return (
                f"{match.group('article')}-бабы "
                f"{match.group('point')}-тармағының "
                f"{match.group('subpoint')}-тармақшасын"
            )

    for area in search_areas:
        match = ru_subpoint_pattern.search(area)
        if match:
            return (
                f"подпунктом {match.group('subpoint')} "
                f"пункта {match.group('point')} "
                f"статьи {match.group('article')}"
            )

    for area in search_areas:
        match = ru_point_pattern.search(area)
        if match:
            return f"пунктом {match.group('point')} статьи {match.group('article')}"

    return ""


def extract_legal_basis(text: str) -> str:
    normalized = normalize_pdf_text(text)
    structured = extract_structured_legal_basis(normalized)
    if structured:
        return structured

    ru_fallback = re.search(
        r"На\s+основании\s+изложенного,\s+руководствуясь[\s\S]{0,1500}?(?=\s*ПОСТАНОВИЛ\s*\(?А?\)?\s*:)",
        normalized,
        flags=re.IGNORECASE,
    )
    if ru_fallback:
        return collapse_spaces(ru_fallback.group(0))

    kz_fallback = re.search(
        KZ_CLOSING_INTRO_PATTERN + r"[\s\S]{0,1500}?басшылыққа\s+ала\s+отырып",
        normalized,
        flags=re.IGNORECASE,
    )
    if kz_fallback:
        return collapse_spaces(kz_fallback.group(0))

    loose_fallback = re.search(
        rf"(?:На\s+основании\s+изложенного|{KZ_CLOSING_INTRO_PATTERN})[\s\S]{{0,700}}",
        normalized,
        flags=re.IGNORECASE,
    )
    return collapse_spaces(loose_fallback.group(0)) if loose_fallback else ""


def extract_pdf_text(data: bytes) -> tuple[str, int]:
    try:
        from pypdf import PdfReader
    except ModuleNotFoundError as exc:
        raise RuntimeError("Для парсинга PDF нужна библиотека pypdf.") from exc

    reader = PdfReader(BytesIO(data))
    parts: list[str] = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(parts), len(reader.pages)


def parse_pdf(data: bytes) -> dict[str, str | int]:
    text, page_count = extract_pdf_text(data)
    normalized_text = normalize_pdf_text(text)
    return {
        "date": extract_decree_date(text),
        "basis": extract_legal_basis(text),
        "fileSha1": hashlib.sha1(data).hexdigest(),
        "textSha1": hashlib.sha1(normalized_text.encode("utf-8")).hexdigest(),
        "pageCount": page_count,
        "textPreview": collapse_spaces(normalized_text[:700]),
    }


class PDFAPIHandler(BaseHTTPRequestHandler):
    server_version = "ProbivashkaPDFAPI/1.0"

    def do_GET(self) -> None:
        if self.path in ("/", "/health"):
            self.write_json(200, {"ok": True})
            return
        self.send_error(404)

    def do_POST(self) -> None:
        if self.path != "/api/parse-pdf":
            self.send_error(404)
            return

        try:
            payload = self.read_json()
            file_base64 = str(payload.get("fileBase64", "")).strip()
            if not file_base64:
                raise ValueError("fileBase64 is required")

            file_bytes = base64.b64decode(file_base64, validate=True)
            self.write_json(200, parse_pdf(file_bytes))
        except Exception as exc:
            self.write_json(400, {"error": str(exc)})

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_common_headers()
        self.end_headers()

    def read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("JSON object expected")
        return parsed

    def write_json(self, status: int, data: dict) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_common_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_common_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def log_message(self, format: str, *args) -> None:
        return


def serve(host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), PDFAPIHandler)
    print(f"PDF API listening on http://{host}:{port}", flush=True)
    server.serve_forever()


def main() -> None:
    args = parse_args()
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
