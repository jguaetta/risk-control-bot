from datetime import datetime
from database.db import get_session, Control, EvidenceLog


def log_evidence(
    control_db_id: int,
    submitted_by: str,
    evidence_type: str,
    evidence_content: str,
    slack_file_id: str = None,
    slack_message_ts: str = None,
    notes: str = None,
    google_doc_id: str = None,
) -> EvidenceLog:
    """
    Log evidence of control execution against a specific control.

    Args:
        control_db_id: Primary key of the Control record
        submitted_by: Slack user ID or name
        evidence_type: 'file', 'message', 'link', 'screenshot', etc.
        evidence_content: Description or content of the evidence
        slack_file_id: Optional Slack file ID if a file was uploaded
        slack_message_ts: Slack message timestamp for traceability
        notes: Any additional notes
        google_doc_id: If set, appends evidence summary to the Google Doc

    Returns:
        The saved EvidenceLog record
    """
    session = get_session()
    try:
        control = session.query(Control).filter(Control.id == control_db_id).first()
        if not control:
            raise ValueError(f"Control with id {control_db_id} not found")

        log = EvidenceLog(
            control_id=control_db_id,
            submitted_by=submitted_by,
            evidence_type=evidence_type,
            evidence_content=evidence_content,
            slack_file_id=slack_file_id,
            slack_message_ts=slack_message_ts,
            notes=notes,
            logged_at=datetime.utcnow(),
        )
        session.add(log)

        # Update control status to indicate evidence has been received
        control.status = "evidence_received"

        session.commit()
        session.refresh(log)

        return log
    finally:
        session.close()


def get_evidence_for_control(control_db_id: int) -> list[EvidenceLog]:
    """Return all evidence logs for a given control."""
    session = get_session()
    try:
        return (
            session.query(EvidenceLog)
            .filter(EvidenceLog.control_id == control_db_id)
            .order_by(EvidenceLog.logged_at)
            .all()
        )
    finally:
        session.close()


def get_controls_for_document(document_id: int) -> list[Control]:
    """Return all controls extracted from a given document."""
    session = get_session()
    try:
        return (
            session.query(Control)
            .filter(Control.document_id == document_id)
            .order_by(Control.control_id)
            .all()
        )
    finally:
        session.close()


def format_control_summary(control: Control) -> str:
    """Return a human-readable summary of a control for Slack messages."""
    return (
        f"*{control.control_id} - {control.title}*\n"
        f">Type: {control.control_type}\n"
        f">Owner: {control.control_owner}\n"
        f">Frequency: {control.frequency}\n"
        f">Risk Mitigated: {control.risk_mitigated}\n"
        f">Source: Page {control.page_number} | {control.section_heading}\n"
        f">Status: {control.status}\n"
        f">Description: {control.description}\n"
        f">Expected Evidence: {control.expected_evidence}"
    )
