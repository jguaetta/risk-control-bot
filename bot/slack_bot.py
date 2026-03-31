import os
import re
import threading
import requests
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from config import SLACK_BOT_TOKEN, SLACK_SIGNING_SECRET, SLACK_APP_TOKEN, SLACK_AUDIT_CHANNEL
from recipe.recipe_parser import save_recipe, get_latest_recipe
from ingestion.document_parser import parse_document
from ingestion.repository_parser import load_repository, get_repository_count
from ai.control_extractor import extract_controls, analyze_gaps, match_controls_to_repository
from evidence.evidence_tracker import (
    log_evidence,
    get_controls_for_document,
    get_evidence_for_control,
    format_control_summary,
)
from database.db import get_session, Document, Control, GapFinding, RecommendedControl, RepositoryMatch, RepositoryControl, init_db
from output.excel_writer import create_controls_excel

app = App(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)


@app.middleware
def log_all_events(body, next):
    event = body.get("event", {})
    print(f"[EVENT] type={event.get('type')} subtype={event.get('subtype')} files={bool(event.get('files'))}", flush=True)
    next()


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


@app.command("/load-repository")
def handle_load_repository(ack, body, say):
    ack()
    count = get_repository_count()
    if count:
        say(
            f"A control repository is already loaded ({count} controls). "
            "To replace it, upload a new CSV file with the word `repository` in your message."
        )
    else:
        say("Please upload your control repository CSV file in this channel and include the text `repository` in your message.")


def _process_message(body, say, logger):
    event = body.get("event", {})
    text = event.get("text", "") or ""
    files = event.get("files", [])
    user = event.get("user", "unknown")

    logger.info(f"[_process_message] text='{text[:80]}' files={len(files)}")

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

        # Repository upload
        if "repository" in text.lower() and filename.endswith(".csv"):
            try:
                file_bytes = _download_slack_file(url)
                count = load_repository(file_bytes=file_bytes, filename=filename)
                say(f"Control repository loaded: *{count} controls* from `{filename}`.")
            except Exception as e:
                logger.error(e)
                say(f"Failed to load repository: {e}")
            return

        # Document ingestion
        if "ingest" in text.lower():
            _ingest_document(f, file_bytes=None, url=url, filename=filename, user=user, say=say, logger=logger, channel_id=event.get("channel"))
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


@app.event("message")
def handle_message_events(body, say, logger):
    event = body.get("event", {})
    logger.info(f"[message] subtype={event.get('subtype')} files={bool(event.get('files'))} text={event.get('text', '')[:50]}")
    _process_message(body, say, logger)


@app.event({"type": "message", "subtype": "file_share"})
def handle_file_share_events(body, say, logger):
    event = body.get("event", {})
    logger.info(f"[file_share] files={bool(event.get('files'))} text={event.get('text', '')[:50]}")
    _process_message(body, say, logger)


def _run_gap_analysis_background(recipe_content, parsed, controls_data, doc_id_db, channel_id, filename):
    """Run gap analysis in a background thread and post results to Slack when done."""
    try:
        # Step 1: Match each extracted control against the repository
        print("  Matching extracted controls to repository...")
        control_matches = match_controls_to_repository(controls_data)

        # Save match results to DB and update controls_data in memory
        if control_matches:
            match_by_id = {m["control_id"]: m for m in control_matches}
            for ctrl in controls_data:
                match = match_by_id.get(ctrl.get("control_id"))
                if match and match["match_type"] != "none":
                    ctrl["repository_match_type"] = match["match_type"]
                    ctrl["repository_protecht_id"] = match.get("protecht_id")

            session = get_session()
            try:
                for match in control_matches:
                    ctrl = session.query(Control).filter(
                        Control.document_id == doc_id_db,
                        Control.control_id == match["control_id"]
                    ).first()
                    if ctrl:
                        ctrl.repository_match_type = match["match_type"] if match["match_type"] != "none" else None
                        ctrl.repository_protecht_id = match.get("protecht_id")
                session.commit()
            finally:
                session.close()

        # Step 2: Run gap analysis
        gap_results = analyze_gaps(recipe_content, parsed, controls_data)
        gap_findings = gap_results.get("gap_findings", [])
        recommendations = gap_results.get("recommendations", [])
        repo_matches = gap_results.get("repository_matches", [])

        session = get_session()
        try:
            for finding in gap_findings:
                session.add(GapFinding(
                    document_id=doc_id_db,
                    risk_statement=finding.get("risk_statement"),
                    coverage_tier=finding.get("coverage_tier"),
                    controls_addressing=finding.get("controls_addressing"),
                    gap_description=finding.get("gap_description"),
                ))
            for rec in recommendations:
                session.add(RecommendedControl(
                    document_id=doc_id_db,
                    recommendation_id=rec.get("recommendation_id"),
                    gap_tier=rec.get("gap_tier"),
                    risk_statement=rec.get("risk_statement"),
                    risk_event=rec.get("risk_event"),
                    control_type=rec.get("control_type"),
                    title=rec.get("title"),
                    description=rec.get("description"),
                    control_owner=rec.get("control_owner"),
                    control_performer=rec.get("control_performer"),
                    frequency=rec.get("frequency"),
                    expected_evidence=rec.get("expected_evidence"),
                ))
            for match in repo_matches:
                session.add(RepositoryMatch(
                    document_id=doc_id_db,
                    risk_statement=match.get("risk_statement"),
                    gap_tier=match.get("gap_tier"),
                    gap_description=match.get("gap_description"),
                    repository_control_id=match.get("repository_control_id"),
                    match_type=match.get("match_type"),
                ))
            session.commit()
        finally:
            session.close()

        tier1 = [f for f in gap_findings if f.get("coverage_tier") == 1]
        tier2 = [f for f in gap_findings if f.get("coverage_tier") == 2]
        tier3 = [f for f in gap_findings if f.get("coverage_tier") == 3]
        exact_count = sum(1 for m in repo_matches if m.get("match_type") == "exact")
        partial_count = sum(1 for m in repo_matches if m.get("match_type") == "partial")

        repo_line = ""
        if exact_count or partial_count:
            repo_line = (
                f">:white_check_mark: *Existing Controls (Exact Match):* {exact_count}\n"
                f">:large_orange_circle: *Partial Repository Matches:* {partial_count}\n"
            )

        gap_summary = (
            f"*Analysis Complete — {filename}*\n"
            f">:page_facing_up: *Controls Extracted:* {len(controls_data)}\n"
            f">:red_circle: *Tier 1 — No Coverage:* {len(tier1)} risk(s)\n"
            f">:yellow_circle: *Tier 2 — Partially Adequate Coverage:* {len(tier2)} risk(s)\n"
            f">:large_green_circle: *Tier 3 — Adequate Coverage:* {len(tier3)} risk(s)\n"
            f"{repo_line}"
            f">:bulb: *Net-New Recommendations:* {len(recommendations)}\n\n"
            f"Use `/gap-analysis` to view full details."
        )

        app.client.chat_postMessage(channel=channel_id, text=gap_summary)

        # Fetch repository control details for Excel export
        repo_controls_for_excel = []
        if repo_matches:
            repo_ctrl_db_ids = list({m.get("repository_control_id") for m in repo_matches if m.get("repository_control_id")})
            if repo_ctrl_db_ids:
                session = get_session()
                try:
                    rc_rows = session.query(RepositoryControl).filter(
                        RepositoryControl.id.in_(repo_ctrl_db_ids)
                    ).all()
                    repo_controls_for_excel = [
                        {
                            "protecht_id": rc.protecht_id,
                            "control_name": rc.control_name,
                            "control_description": rc.control_description,
                            "control_type": rc.control_type,
                        }
                        for rc in rc_rows
                    ]
                finally:
                    session.close()

        # Export to Excel and upload to Slack
        try:
            import tempfile
            excel_path = create_controls_excel(
                document_name=filename,
                controls=controls_data,
                gap_findings=gap_findings,
                recommendations=recommendations,
                output_dir=tempfile.gettempdir(),
                repo_controls=repo_controls_for_excel,
            )
            with open(excel_path, "rb") as f:
                app.client.files_upload_v2(
                    channel=channel_id,
                    file=f.read(),
                    filename=os.path.basename(excel_path),
                    title=f"Controls Analysis — {filename}",
                )
            os.remove(excel_path)
        except Exception as excel_err:
            print(f"  Excel export failed: {excel_err}", flush=True)
            app.client.chat_postMessage(channel=channel_id, text=f"_Note: Excel export failed — {excel_err}_")

    except Exception as e:
        app.client.chat_postMessage(channel=channel_id, text=f"Gap analysis failed: {e}")


def _ingest_document(file_obj, file_bytes, url, filename, user, say, logger, channel_id=None):
    recipe = get_latest_recipe()
    if not recipe:
        say("No recipe loaded. Please upload a recipe DOCX first with the word `recipe` in your message.")
        return

    say(f":hourglass_flowing_sand: Got it — analyzing *{filename}*. I'll post the full results when it's ready.")
    try:
        if file_bytes is None:
            file_bytes = _download_slack_file(url)

        parsed = parse_document(file_bytes=file_bytes, filename=filename)
        controls_data, skipped_sections = extract_controls(recipe.content, parsed)

        # Post skip warnings in real time so teams know immediately
        if skipped_sections:
            say(
                f":warning: *{len(skipped_sections)} section(s) could not be parsed and were skipped:*\n"
                + "\n".join(f">• {s}" for s in skipped_sections)
            )

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
                    risk_event=ctrl.get("risk_event"),
                    control_owner=ctrl.get("control_owner"),
                    control_performer=ctrl.get("control_performer"),
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

        # Run gap analysis in background thread
        thread = threading.Thread(
            target=_run_gap_analysis_background,
            args=(recipe.content, parsed, controls_data, doc_id_db, channel_id, filename),
            daemon=True,
        )
        thread.start()

    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
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
# /gap-analysis  — show full gap analysis for the most recent document
# ---------------------------------------------------------------------------

@app.command("/gap-analysis")
def handle_gap_analysis(ack, body, say):
    ack()
    session = get_session()
    try:
        doc = session.query(Document).order_by(Document.uploaded_at.desc()).first()
        if not doc:
            say("No documents have been ingested yet.")
            return
        _post_gap_analysis(doc, say)
    finally:
        session.close()


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
            if ctrl.repository_match_type == "exact":
                status_icon = f":white_check_mark: Existing `{ctrl.repository_protecht_id}`"
            elif ctrl.repository_match_type == "partial":
                status_icon = f":large_orange_circle: Partial `{ctrl.repository_protecht_id}`"
            else:
                status_icon = ":new: New"
            lines.append(
                f"• *{ctrl.control_id}* — {ctrl.title} "
                f"_(Page {ctrl.page_number} | {ctrl.section_heading})_ "
                f"[{status_icon}]"
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

def _build_audit_log() -> str:
    from database.db import Recipe, EvidenceLog, RepositoryControl
    session = get_session()
    try:
        lines = ["*RiskControlBot — Audit Log*\n"]

        # Repository
        repo_count = session.query(RepositoryControl).count()
        if repo_count:
            lines.append(f":file_cabinet: *Control Repository:* {repo_count} controls loaded")
        else:
            lines.append(":file_cabinet: *Control Repository:* not loaded")

        # Recipe
        recipe = session.query(Recipe).order_by(Recipe.uploaded_at.desc()).first()
        if recipe:
            lines.append(f":book: *Active Recipe:* `{recipe.name}` — loaded {recipe.uploaded_at.strftime('%Y-%m-%d %H:%M')}")
        else:
            lines.append(":book: *Active Recipe:* none")

        lines.append("")

        # Documents
        docs = session.query(Document).order_by(Document.uploaded_at.desc()).limit(10).all()
        if not docs:
            lines.append("_No documents have been ingested yet._")
        else:
            lines.append(f":page_facing_up: *Recent Ingestions (last {len(docs)}):*")
            for doc in docs:
                ctrl_count = session.query(Control).filter(Control.document_id == doc.id).count()
                gap_count = session.query(GapFinding).filter(GapFinding.document_id == doc.id).count()
                rec_count = session.query(RecommendedControl).filter(RecommendedControl.document_id == doc.id).count()
                existing = session.query(Control).filter(
                    Control.document_id == doc.id,
                    Control.repository_match_type == "exact"
                ).count()
                partial = session.query(Control).filter(
                    Control.document_id == doc.id,
                    Control.repository_match_type == "partial"
                ).count()
                lines.append(
                    f">*{doc.name}*\n"
                    f">Uploaded by <@{doc.uploaded_by}> on {doc.uploaded_at.strftime('%Y-%m-%d %H:%M')}\n"
                    f">Controls: {ctrl_count} extracted "
                    f"(:white_check_mark: {existing} existing, :large_orange_circle: {partial} partial, :new: {ctrl_count - existing - partial} new)\n"
                    f">Gap findings: {gap_count} | Recommendations: {rec_count}"
                )

        lines.append("")
        evidence_count = session.query(EvidenceLog).count()
        lines.append(f":memo: *Total Evidence Logged:* {evidence_count} entries")

        return "\n".join(lines)
    finally:
        session.close()


@app.command("/audit-log")
def handle_audit_log(ack, body, say):
    ack()
    say(_build_audit_log())


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
# App mention — help text and gap analysis trigger
# ---------------------------------------------------------------------------

def _post_gap_analysis(doc, say):
    session = get_session()
    try:
        findings = session.query(GapFinding).filter(GapFinding.document_id == doc.id).all()
        recommendations = session.query(RecommendedControl).filter(RecommendedControl.document_id == doc.id).all()
        repo_matches = session.query(RepositoryMatch).filter(RepositoryMatch.document_id == doc.id).all()

        if not findings:
            say(f"No gap analysis found for *{doc.name}*.")
            return

        # Build lookups: risk_statement -> list of RepositoryControl
        exact_by_risk = {}
        partial_by_risk = {}
        for m in repo_matches:
            rc = session.query(RepositoryControl).filter(RepositoryControl.id == m.repository_control_id).first()
            if not rc:
                continue
            if m.match_type == "exact":
                exact_by_risk.setdefault(m.risk_statement, []).append(rc)
            else:
                partial_by_risk.setdefault(m.risk_statement, []).append(rc)

        # Recs lookup by risk_statement
        recs_by_risk = {}
        for rec in recommendations:
            recs_by_risk.setdefault(rec.risk_statement, []).append(rec)

        tier1 = [f for f in findings if f.coverage_tier == 1]
        tier2 = [f for f in findings if f.coverage_tier == 2]
        tier3 = [f for f in findings if f.coverage_tier == 3]

        lines = [f"*Gap Analysis — {doc.name}*\n"]

        if tier1:
            lines.append(":red_circle: *Tier 1 — No Coverage*")
            for f in tier1:
                exact = exact_by_risk.get(f.risk_statement, [])
                if exact:
                    refs = ", ".join(f"`{rc.protecht_id}` {rc.control_name}" for rc in exact)
                    lines.append(f">• {f.risk_statement}\n>:white_check_mark: Existing: {refs}")
                else:
                    recs = recs_by_risk.get(f.risk_statement, [])
                    rec_ids = ", ".join(r.recommendation_id for r in recs) if recs else "—"
                    lines.append(f">• {f.risk_statement}\n>_{f.gap_description}_\n>:bulb: See: {rec_ids}")

        if tier2:
            lines.append("\n:yellow_circle: *Tier 2 — Partially Adequate Coverage*")
            for f in tier2:
                exact = exact_by_risk.get(f.risk_statement, [])
                if exact:
                    refs = ", ".join(f"`{rc.protecht_id}` {rc.control_name}" for rc in exact)
                    lines.append(f">• {f.risk_statement}\n>Controls: {f.controls_addressing}\n>:white_check_mark: Existing: {refs}")
                else:
                    recs = recs_by_risk.get(f.risk_statement, [])
                    rec_ids = ", ".join(r.recommendation_id for r in recs) if recs else "—"
                    lines.append(f">• {f.risk_statement}\n>Controls: {f.controls_addressing}\n>_{f.gap_description}_\n>:bulb: See: {rec_ids}")

        if partial_by_risk:
            lines.append("\n:large_orange_circle: *Partial Repository Matches — SME Review Required*")
            for risk_stmt, rcs in partial_by_risk.items():
                for rc in rcs:
                    lines.append(
                        f">• `{rc.protecht_id}` — {rc.control_name} ({rc.control_type})\n"
                        f">Risk: {risk_stmt[:150]}..."
                    )

        if tier3:
            lines.append("\n:large_green_circle: *Tier 3 — Adequate Coverage*")
            for f in tier3:
                lines.append(f">• {f.risk_statement} — {f.controls_addressing}")

        if recommendations:
            lines.append(f"\n:bulb: *{len(recommendations)} Net-New Draft Recommendation(s) — Require SME Validation*")
            for rec in recommendations:
                lines.append(
                    f">• *{rec.recommendation_id}* [{rec.control_type}] — {rec.title}\n"
                    f">Risk: {rec.risk_statement[:150]}\n"
                    f">_{rec.description[:200]}..._"
                )

        say("\n".join(lines))
    finally:
        session.close()


@app.event("app_mention")
def handle_mention(body, say, logger):
    event = body.get("event", {})
    text = event.get("text", "").lower()
    files = event.get("files", [])
    user = event.get("user", "unknown")

    print(f"[app_mention] user={user} text='{text[:80]}' files={len(files)}", flush=True)

    # File upload with mention — route to appropriate handler
    if files:
        for f in files:
            filename = f.get("name", "").lower()
            url = f.get("url_private_download") or f.get("url_private")
            file_id = f.get("id")

            print(f"[app_mention] file='{filename}' url={'yes' if url else 'NO URL'}", flush=True)

            if "recipe" in text and filename.endswith(".docx"):
                try:
                    file_bytes = _download_slack_file(url)
                    recipe = save_recipe(name=filename, file_bytes=file_bytes, uploaded_by=user)
                    say(f"Recipe *{filename}* loaded successfully (ID: {recipe.id}).")
                except Exception as e:
                    logger.error(e)
                    say(f"Failed to load recipe: {e}")
                return

            if "repository" in text and filename.endswith(".csv"):
                try:
                    file_bytes = _download_slack_file(url)
                    count = load_repository(file_bytes=file_bytes, filename=filename)
                    say(f"Control repository loaded: *{count} controls* from `{filename}`.")
                except Exception as e:
                    logger.error(e)
                    say(f"Failed to load repository: {e}")
                return

            if "ingest" in text:
                _ingest_document(f, file_bytes=None, url=url, filename=filename, user=user, say=say, logger=logger, channel_id=event.get("channel"))
                return

            import re
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

    if "gap analysis" in text:
        session = get_session()
        try:
            doc = session.query(Document).order_by(Document.uploaded_at.desc()).first()
            if not doc:
                say("No documents have been ingested yet.")
                return
            _post_gap_analysis(doc, say)
        finally:
            session.close()
        return

    say(
        "*RiskControlBot* is a GRC automation tool that analyzes source documents, extracts controls, "
        "identifies risk gaps, and matches findings against an existing control repository.\n\n"
        "*What it does:*\n"
        ">:one: Reads a methodology recipe that defines how controls should be identified and documented\n"
        ">:two: Ingests source documents (`.docx`, `.pdf`, `.txt`) and extracts all controls found in the document\n"
        ">:three: Identifies risks present in the document and evaluates whether existing controls provide adequate coverage\n"
        ">:four: Matches extracted controls against your control repository to identify existing, partial, and net-new controls\n"
        ">:five: Generates draft recommendations for risks with no or only partial coverage\n"
        ">:six: Exports results to Excel with tabs for Draft Controls, Gap Analysis, and Recommendations\n\n"
        "*Available commands:*\n"
        "• `@RiskControlBot ingest` + file upload — analyze a document end to end\n"
        "• `@RiskControlBot recipe` + `.docx` upload — load a new methodology recipe\n"
        "• `@RiskControlBot repository` + `.csv` upload — load or replace the control repository\n"
        "• `@RiskControlBot evidence CTRL-XXX` + file upload — log evidence against a control\n"
        "• `@RiskControlBot gap analysis` — view the full gap analysis for the most recent document\n"
        "• `/list-controls` — list all controls extracted from the most recent document\n"
        "• `/control-detail CTRL-001` — view full detail for a specific control\n"
        "• `/gap-analysis` — view gap findings and recommendations\n"
        "• `/list-evidence CTRL-001` — view evidence logged for a control\n"
        "• `/log-evidence CTRL-001 <type> <description>` — log text-based evidence\n"
        "• `/load-repository` — check or reload the control repository\n"
        "• `/audit-log` — view recent activity, ingestion history, and evidence logged"
    )


def _autoload_recipe():
    """Load the methodology recipe from the project directory if not already in the database."""
    import os
    from ingestion.document_parser import parse_docx

    recipe_path = os.path.join(os.path.dirname(__file__), "..", "recipe", "methodology_recipe.docx")
    recipe_path = os.path.abspath(recipe_path)

    if not os.path.exists(recipe_path):
        return

    session = get_session()
    try:
        from database.db import Recipe
        existing = session.query(Recipe).first()
        if existing:
            return

        with open(recipe_path, "rb") as f:
            file_bytes = f.read()

        recipe = save_recipe(name="methodology_recipe.docx", file_bytes=file_bytes, uploaded_by="system")
        print(f"Recipe auto-loaded: {recipe.name} (ID: {recipe.id})")
    finally:
        session.close()


def _post_startup_audit():
    if not SLACK_AUDIT_CHANNEL:
        return
    try:
        app.client.chat_postMessage(
            channel=SLACK_AUDIT_CHANNEL,
            text=_build_audit_log(),
        )
    except Exception as e:
        print(f"  Could not post startup audit log: {e}", flush=True)


def start():
    init_db()
    _autoload_recipe()
    _post_startup_audit()
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
