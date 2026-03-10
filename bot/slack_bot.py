import re
import requests
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from config import SLACK_BOT_TOKEN, SLACK_SIGNING_SECRET, SLACK_APP_TOKEN
from recipe.recipe_parser import save_recipe, get_latest_recipe
from ingestion.document_parser import parse_document
from ai.control_extractor import extract_controls
from evidence.evidence_tracker import (
    log_evidence,
    get_controls_for_document,
    get_evidence_for_control,
    format_control_summary,
)
from database.db import get_session, Document, Control, init_db

app = App(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _download_slack_file(url: str) -> bytes:
    response = requests.get(url, headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"})
    response.raise_for_status()
    return response.content


def _post(say, text: str):
    say(text)


# ---------------------------------------------------------------------------
# /load-recipe  — upload a recipe DOCX
# ---------------------------------------------------------------------------

@app.command("/load-recipe")
def handle_load_recipe(ack, body, say):
    ack()
    say(
        "Please upload your methodology recipe DOCX file in this channel "
        "and include the text `recipe` in the message so I can detect it."
    )


@app.event("message")
def handle_message_events(body, say, logger):
    event = body.get("event", {})
    text = event.get("text", "") or ""
    files = event.get("files", [])
    user = event.get("user", "unknown")

    if not files:
        return

    for f in files:
        filename = f.get("name", "").lower()
        url = f.get("url_private_download") or f.get("url_private")
        file_id = f.get("id")

        # Recipe upload
        if "recipe" in text.lower() and filename.endswith(".docx"):
            try:
                file_bytes = _download_slack_file(url)
                recipe = save_recipe(name=filename, file_bytes=file_bytes, uploaded_by=user)
                say(f"Recipe *{filename}* loaded successfully (ID: {recipe.id}). "
                    f"You can now ingest documents using `/ingest-document`.")
            except Exception as e:
                logger.error(e)
                say(f"Failed to load recipe: {e}")
            return

        # Document ingestion
        if "ingest" in text.lower():
            _ingest_document(f, file_bytes=None, url=url, filename=filename, user=user, say=say, logger=logger)
            return

        # Evidence submission — expect "evidence CTRL-XXX" in the message
        match = re.search(r"evidence\s+(CTRL-\d+)", text, re.IGNORECASE)
        if match:
            control_id_str = match.group(1).upper()
            _log_evidence_from_file(
                control_id_str=control_id_str,
                file_id=file_id,
                filename=filename,
                url=url,
                user=user,
                message_ts=event.get("ts"),
                say=say,
                logger=logger,
            )
            return


def _ingest_document(file_obj, file_bytes, url, filename, user, say, logger):
    recipe = get_latest_recipe()
    if not recipe:
        say("No recipe loaded. Please upload a recipe DOCX first with the word `recipe` in your message.")
        return

    say(f"Ingesting *{filename}*... This may take a moment.")
    try:
        if file_bytes is None:
            file_bytes = _download_slack_file(url)

        parsed = parse_document(file_bytes=file_bytes, filename=filename)
        controls_data = extract_controls(recipe.content, parsed)

        session = get_session()
        try:
            doc = Document(
                name=filename,
                content=parsed["full_text"][:50000],
                uploaded_by=user,
                recipe_id=recipe.id,
            )
            session.add(doc)
            session.flush()

            for ctrl in controls_data:
                control = Control(
                    document_id=doc.id,
                    control_id=ctrl.get("control_id"),
                    control_type=ctrl.get("control_type"),
                    title=ctrl.get("title"),
                    description=ctrl.get("description"),
                    risk_mitigated=ctrl.get("risk_mitigated"),
                    control_owner=ctrl.get("control_owner"),
                    frequency=ctrl.get("frequency"),
                    page_number=ctrl.get("page_number"),
                    section_heading=ctrl.get("section_heading"),
                    source_excerpt=ctrl.get("source_excerpt"),
                    expected_evidence=ctrl.get("expected_evidence"),
                )
                session.add(control)

            doc_id_db = doc.id
            session.commit()
        finally:
            session.close()

        say(
            f"*{len(controls_data)} controls* extracted from *{filename}*.\n"
            f"Use `/list-controls` to see all controls.\n"
            f"To log evidence for a control, upload a file and include `evidence CTRL-XXX` in your message."
        )

    except Exception as e:
        logger.error(e)
        say(f"Failed to ingest document: {e}")


def _log_evidence_from_file(control_id_str, file_id, filename, url, user, message_ts, say, logger):
    session = get_session()
    try:
        control = session.query(Control).filter(Control.control_id == control_id_str).first()
        if not control:
            say(f"Control *{control_id_str}* not found. Use `/list-controls` to see available controls.")
            return

        control_db_id = control.id
    finally:
        session.close()

    try:
        log_evidence(
            control_db_id=control_db_id,
            submitted_by=user,
            evidence_type="file",
            evidence_content=f"File uploaded: {filename}",
            slack_file_id=file_id,
            slack_message_ts=message_ts,
        )
        say(f"Evidence logged for *{control_id_str}*. File: `{filename}`.")
    except Exception as e:
        logger.error(e)
        say(f"Failed to log evidence: {e}")


# ---------------------------------------------------------------------------
# /list-controls  — list all controls for the most recently ingested document
# ---------------------------------------------------------------------------

@app.command("/list-controls")
def handle_list_controls(ack, body, say):
    ack()
    session = get_session()
    try:
        doc = session.query(Document).order_by(Document.uploaded_at.desc()).first()
        if not doc:
            say("No documents have been ingested yet.")
            return

        controls = session.query(Control).filter(Control.document_id == doc.id).all()
        if not controls:
            say(f"No controls found for *{doc.name}*.")
            return

        lines = [f"*Controls extracted from: {doc.name}*\n"]
        for ctrl in controls:
            lines.append(
                f"• *{ctrl.control_id}* — {ctrl.title} "
                f"_(Page {ctrl.page_number} | {ctrl.section_heading})_ "
                f"[{ctrl.status}]"
            )
        say("\n".join(lines))
    finally:
        session.close()


# ---------------------------------------------------------------------------
# /control-detail  — show full detail for a specific control
# ---------------------------------------------------------------------------

@app.command("/control-detail")
def handle_control_detail(ack, body, say):
    ack()
    control_id_str = body.get("text", "").strip().upper()
    if not control_id_str:
        say("Usage: `/control-detail CTRL-001`")
        return

    session = get_session()
    try:
        control = session.query(Control).filter(Control.control_id == control_id_str).first()
        if not control:
            say(f"Control *{control_id_str}* not found.")
            return
        say(format_control_summary(control))
    finally:
        session.close()


# ---------------------------------------------------------------------------
# /list-evidence  — list all evidence logged for a control
# ---------------------------------------------------------------------------

@app.command("/list-evidence")
def handle_list_evidence(ack, body, say):
    ack()
    control_id_str = body.get("text", "").strip().upper()
    if not control_id_str:
        say("Usage: `/list-evidence CTRL-001`")
        return

    session = get_session()
    try:
        control = session.query(Control).filter(Control.control_id == control_id_str).first()
        if not control:
            say(f"Control *{control_id_str}* not found.")
            return

        logs = get_evidence_for_control(control.id)
        if not logs:
            say(f"No evidence logged for *{control_id_str}* yet.")
            return

        lines = [f"*Evidence log for {control_id_str} — {control.title}*\n"]
        for log in logs:
            lines.append(
                f"• [{log.logged_at.strftime('%Y-%m-%d %H:%M')}] "
                f"*{log.evidence_type}* by {log.submitted_by}: {log.evidence_content}"
            )
            if log.notes:
                lines.append(f"  _Notes: {log.notes}_")
        say("\n".join(lines))
    finally:
        session.close()


# ---------------------------------------------------------------------------
# /log-evidence  — log evidence via text (no file upload required)
# ---------------------------------------------------------------------------

@app.command("/log-evidence")
def handle_log_evidence(ack, body, say):
    ack()
    text = body.get("text", "").strip()
    user = body.get("user_id", "unknown")

    # Expected format: CTRL-001 <evidence type> <description>
    parts = text.split(" ", 2)
    if len(parts) < 3:
        say("Usage: `/log-evidence CTRL-001 <type> <description>`\nExample: `/log-evidence CTRL-001 message Transaction blocked at 2024-01-15 for failing model threshold`")
        return

    control_id_str = parts[0].upper()
    evidence_type = parts[1]
    evidence_content = parts[2]

    session = get_session()
    try:
        control = session.query(Control).filter(Control.control_id == control_id_str).first()
        if not control:
            say(f"Control *{control_id_str}* not found.")
            return

        control_db_id = control.id
    finally:
        session.close()

    log_evidence(
        control_db_id=control_db_id,
        submitted_by=user,
        evidence_type=evidence_type,
        evidence_content=evidence_content,
        slack_message_ts=body.get("trigger_id"),
    )
    say(f"Evidence logged for *{control_id_str}*.")


# ---------------------------------------------------------------------------
# App mention — help text
# ---------------------------------------------------------------------------

@app.event("app_mention")
def handle_mention(body, say):
    say(
        "*Risk Control Bot — Available Commands*\n\n"
        "• Upload a `.docx` file with `recipe` in your message — load methodology recipe\n"
        "• Upload any document with `ingest` in your message — extract controls\n"
        "• Upload a file with `evidence CTRL-XXX` in your message — log file evidence\n"
        "• `/list-controls` — list all controls from the most recent document\n"
        "• `/control-detail CTRL-001` — view full detail for a control\n"
        "• `/list-evidence CTRL-001` — view all evidence logged for a control\n"
        "• `/log-evidence CTRL-001 <type> <description>` — log text-based evidence"
    )


def start():
    init_db()
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
