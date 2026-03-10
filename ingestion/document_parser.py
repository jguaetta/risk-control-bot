import io
import re
import requests
import pdfplumber
from docx import Document as DocxDocument
from config import GOOGLE_CREDENTIALS_FILE, GOOGLE_TOKEN_FILE
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/documents.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def parse_pdf(file_bytes: bytes) -> dict:
    """Extract text and page structure from a PDF."""
    pages = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            pages.append({"page": i, "text": text})
    full_text = "\n".join(p["text"] for p in pages)
    return {"pages": pages, "full_text": full_text}


def parse_docx(file_bytes: bytes) -> dict:
    """Extract text and heading structure from a DOCX file."""
    doc = DocxDocument(io.BytesIO(file_bytes))
    pages = []
    current_section = "Document Start"
    current_page = 1
    current_lines = []

    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue

        style = para.style.name if para.style else ""
        if "Heading" in style:
            if current_lines:
                pages.append({
                    "page": current_page,
                    "section": current_section,
                    "text": "\n".join(current_lines),
                })
                current_page += 1
                current_lines = []
            current_section = text

        current_lines.append(text)

    if current_lines:
        pages.append({
            "page": current_page,
            "section": current_section,
            "text": "\n".join(current_lines),
        })

    full_text = "\n".join(p["text"] for p in pages)
    return {"pages": pages, "full_text": full_text}


def parse_plain_text(file_bytes: bytes) -> dict:
    """Parse plain text into pseudo-pages of ~500 words each."""
    text = file_bytes.decode("utf-8", errors="replace")
    words = text.split()
    chunk_size = 500
    pages = []
    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i + chunk_size])
        pages.append({"page": (i // chunk_size) + 1, "text": chunk})
    return {"pages": pages, "full_text": text}


def _get_google_creds():
    creds = None
    try:
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_FILE, GOOGLE_SCOPES)
    except Exception:
        pass

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GOOGLE_CREDENTIALS_FILE, GOOGLE_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(GOOGLE_TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
    return creds


def parse_google_doc(doc_id: str) -> dict:
    """Fetch and parse a Google Doc by document ID."""
    creds = _get_google_creds()
    service = build("docs", "v1", credentials=creds)
    doc = service.documents().get(documentId=doc_id).execute()

    pages = []
    current_page = 1
    current_section = "Document Start"
    current_lines = []

    for element in doc.get("body", {}).get("content", []):
        paragraph = element.get("paragraph")
        if not paragraph:
            continue

        style = paragraph.get("paragraphStyle", {}).get("namedStyleType", "")
        text = "".join(
            run.get("textRun", {}).get("content", "")
            for run in paragraph.get("elements", [])
        ).strip()

        if not text:
            continue

        if "HEADING" in style:
            if current_lines:
                pages.append({
                    "page": current_page,
                    "section": current_section,
                    "text": "\n".join(current_lines),
                })
                current_page += 1
                current_lines = []
            current_section = text

        current_lines.append(text)

    if current_lines:
        pages.append({
            "page": current_page,
            "section": current_section,
            "text": "\n".join(current_lines),
        })

    full_text = "\n".join(p["text"] for p in pages)
    return {"pages": pages, "full_text": full_text}


def parse_document(file_bytes: bytes = None, filename: str = "", google_doc_id: str = None) -> dict:
    """
    Universal document parser. Routes to the correct parser based on file type.
    Returns: { pages: [...], full_text: str }
    """
    if google_doc_id:
        return parse_google_doc(google_doc_id)

    name = filename.lower()
    if name.endswith(".pdf"):
        return parse_pdf(file_bytes)
    elif name.endswith(".docx"):
        return parse_docx(file_bytes)
    elif name.endswith(".txt"):
        return parse_plain_text(file_bytes)
    else:
        # fallback: try plain text
        return parse_plain_text(file_bytes)
