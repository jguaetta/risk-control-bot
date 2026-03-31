from googleapiclient.discovery import build
from ingestion.document_parser import _get_google_creds


def _get_sheets_service():
    creds = _get_google_creds()
    return build("sheets", "v4", credentials=creds)


def _get_drive_service():
    creds = _get_google_creds()
    return build("drive", "v3", credentials=creds)


def create_controls_spreadsheet(document_name: str, controls: list[dict], gap_findings: list[dict], recommendations: list[dict]) -> tuple[str, str]:
    """
    Create a Google Sheet with three tabs:
    - Draft Controls
    - Recommendations
    - Gap Analysis

    Returns:
        Tuple of (spreadsheet_id, spreadsheet_url)
    """
    sheets_service = _get_sheets_service()

    title = f"Controls - {document_name}"

    spreadsheet = sheets_service.spreadsheets().create(body={
        "properties": {"title": title},
        "sheets": [
            {"properties": {"title": "Draft Controls", "index": 0}},
            {"properties": {"title": "Recommendations", "index": 1}},
            {"properties": {"title": "Gap Analysis", "index": 2}},
        ]
    }).execute()

    spreadsheet_id = spreadsheet["spreadsheetId"]
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    sheet_ids = {s["properties"]["title"]: s["properties"]["sheetId"] for s in spreadsheet["sheets"]}

    requests = []

    # --- Draft Controls Tab ---
    draft_headers = [
        "Control ID", "Type", "Title", "Description", "Risk Mitigated",
        "Risk Event", "Control Owner", "Frequency", "Page", "Section",
        "Source Excerpt", "Expected Evidence", "Status"
    ]
    draft_rows = [draft_headers]
    for ctrl in controls:
        draft_rows.append([
            ctrl.get("control_id", ""),
            ctrl.get("control_type", ""),
            ctrl.get("title", ""),
            ctrl.get("description", ""),
            ctrl.get("risk_mitigated", ""),
            ctrl.get("risk_event", ""),
            ctrl.get("control_owner", ""),
            ctrl.get("frequency", ""),
            str(ctrl.get("page_number", "")),
            ctrl.get("section_heading", ""),
            ctrl.get("source_excerpt", ""),
            ctrl.get("expected_evidence", ""),
            "Open",
        ])

    # --- Recommendations Tab ---
    rec_headers = [
        "Recommendation ID", "Gap Tier", "Risk Event", "Risk Statement",
        "Recommended Type", "Recommended Title", "Recommended Description",
        "Recommended Owner", "Recommended Frequency", "Recommended Evidence",
        "Validation Status", "SME Notes"
    ]
    rec_rows = [rec_headers]
    for rec in recommendations:
        rec_rows.append([
            rec.get("recommendation_id", ""),
            f"Tier {rec.get('gap_tier', '')}",
            rec.get("risk_event", ""),
            rec.get("risk_statement", ""),
            rec.get("control_type", ""),
            rec.get("title", ""),
            rec.get("description", ""),
            rec.get("control_owner", "TBD"),
            rec.get("frequency", "TBD"),
            rec.get("expected_evidence", ""),
            "Draft — Requires SME Validation",
            "",
        ])

    # --- Gap Analysis Tab ---
    gap_headers = [
        "Risk Statement", "Coverage Tier", "Controls Addressing Risk", "Gap Description"
    ]
    gap_rows = [gap_headers]
    tier_labels = {1: "Tier 1 — No Coverage", 2: "Tier 2 — Inadequate Coverage", 3: "Tier 3 — Adequate Coverage"}
    for finding in gap_findings:
        gap_rows.append([
            finding.get("risk_statement", ""),
            tier_labels.get(finding.get("coverage_tier"), ""),
            finding.get("controls_addressing", "None"),
            finding.get("gap_description", ""),
        ])

    # Build batch update requests
    def rows_to_values(rows):
        return [{"values": [{"userEnteredValue": {"stringValue": str(cell)}} for cell in row]} for row in rows]

    def append_data(sheet_name, rows):
        return {
            "updateCells": {
                "range": {
                    "sheetId": sheet_ids[sheet_name],
                    "startRowIndex": 0,
                    "startColumnIndex": 0,
                },
                "rows": rows_to_values(rows),
                "fields": "userEnteredValue",
            }
        }

    def bold_header(sheet_name, num_cols):
        return {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_ids[sheet_name],
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_cols,
                },
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        }

    requests.extend([
        append_data("Draft Controls", draft_rows),
        bold_header("Draft Controls", len(draft_headers)),
        append_data("Recommendations", rec_rows),
        bold_header("Recommendations", len(rec_headers)),
        append_data("Gap Analysis", gap_rows),
        bold_header("Gap Analysis", len(gap_headers)),
    ])

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()

    return spreadsheet_id, spreadsheet_url


def append_evidence_to_sheet(spreadsheet_id: str, control_id: str, evidence_summary: str):
    """Append an evidence log entry to the Draft Controls sheet."""
    sheets_service = _get_sheets_service()

    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range="Draft Controls!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[f"[EVIDENCE] {control_id}", "", "", evidence_summary, "", "", "", "", "", "", "", "Evidence Received"]]}
    ).execute()
