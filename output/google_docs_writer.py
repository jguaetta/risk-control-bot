from googleapiclient.discovery import build
from ingestion.document_parser import _get_google_creds

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
]


def _get_docs_service():
    creds = _get_google_creds()
    return build("docs", "v1", credentials=creds)


def _get_drive_service():
    creds = _get_google_creds()
    return build("drive", "v3", credentials=creds)


def create_controls_doc(document_name: str, controls: list[dict]) -> tuple[str, str]:
    """
    Create a new Google Doc and populate it with extracted controls.

    Args:
        document_name: Name of the source document (used as the doc title)
        controls: List of control dicts from control_extractor

    Returns:
        Tuple of (doc_id, doc_url)
    """
    docs_service = _get_docs_service()
    drive_service = _get_drive_service()

    title = f"Controls - {document_name}"
    doc = docs_service.documents().create(body={"title": title}).execute()
    doc_id = doc["documentId"]
    doc_url = f"https://docs.google.com/document/d/{doc_id}/edit"

    requests = _build_doc_requests(document_name, controls)
    if requests:
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={"requests": requests},
        ).execute()

    return doc_id, doc_url


def _build_doc_requests(document_name: str, controls: list[dict]) -> list[dict]:
    """Build the batchUpdate requests to populate the Google Doc."""
    requests = []
    index = 1  # Google Docs text insertion index

    def insert_text(text, idx):
        return {"insertText": {"location": {"index": idx}, "text": text}}

    def apply_style(style, start, end):
        return {
            "updateParagraphStyle": {
                "range": {"startIndex": start, "endIndex": end},
                "paragraphStyle": {"namedStyleType": style},
                "fields": "namedStyleType",
            }
        }

    def bold_text(start, end):
        return {
            "updateTextStyle": {
                "range": {"startIndex": start, "endIndex": end},
                "textStyle": {"bold": True},
                "fields": "bold",
            }
        }

    # Title
    title_text = f"Controls Extracted From: {document_name}\n"
    requests.append(insert_text(title_text, index))
    requests.append(apply_style("HEADING_1", index, index + len(title_text)))
    index += len(title_text)

    for ctrl in controls:
        # Control heading
        heading_text = f"{ctrl.get('control_id', 'CTRL')} - {ctrl.get('title', 'Untitled Control')}\n"
        requests.append(insert_text(heading_text, index))
        requests.append(apply_style("HEADING_2", index, index + len(heading_text)))
        index += len(heading_text)

        # Control fields
        fields = [
            ("Type", ctrl.get("control_type", "")),
            ("Description", ctrl.get("description", "")),
            ("Source Reference", f"Page {ctrl.get('page_number', '?')} | Section: {ctrl.get('section_heading', 'N/A')}"),
            ("Source Excerpt", ctrl.get("source_excerpt", "")),
            ("Expected Evidence", ctrl.get("expected_evidence", "")),
        ]

        for label, value in fields:
            label_text = f"{label}: "
            value_text = f"{value}\n"
            line = label_text + value_text

            requests.append(insert_text(line, index))
            requests.append(bold_text(index, index + len(label_text)))
            index += len(line)

        # Spacer
        requests.append(insert_text("\n", index))
        index += 1

    return requests


def append_evidence_to_doc(doc_id: str, control_id: str, evidence_summary: str):
    """Append an evidence log entry to an existing controls Google Doc."""
    docs_service = _get_docs_service()

    doc = docs_service.documents().get(documentId=doc_id).execute()
    content = doc.get("body", {}).get("content", [])
    end_index = content[-1].get("endIndex", 1) - 1

    evidence_text = f"\n[EVIDENCE LOG] {control_id}: {evidence_summary}\n"
    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={
            "requests": [
                {"insertText": {"location": {"index": end_index}, "text": evidence_text}}
            ]
        },
    ).execute()
