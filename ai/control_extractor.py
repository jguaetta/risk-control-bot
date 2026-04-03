import re
import time
import json
import anthropic
from config import ANTHROPIC_API_KEY

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """You are a risk and controls documentation specialist.
You have been given a methodology recipe that defines how controls should be identified and documented.
Your job is to analyze source documents and extract controls strictly according to the recipe methodology.

CRITICAL RULES:
- A control is an action, process, or mechanism that PREVENTS, DETECTS, or CORRECTS a risk.
- A control can NEVER be the failure of a risk or the risk itself. Do not document risk events, failures, or negative outcomes as controls.
- A risk statement must NEVER be written as the failure of a control activity. Never use the phrase "failure to" in any risk_mitigated field or description. A risk is an event or condition that could cause harm — express it as such (e.g. "sanctioned individuals accessing financial services", not "failure to screen sanctioned individuals").
- Extract any control that is identifiable as an action, process, or mechanism that PREVENTS, DETECTS, or CORRECTS a risk — even if only partially described. A control does not need to be fully documented to be extracted.
- Every field must be populated using only information explicitly stated in the source document. For fields that cannot be determined from the document, set the value to "TBD". Never assume, infer, or fabricate values.
- A partially documented control with several TBD fields is valid and should be extracted. Do not skip a control simply because some fields are missing.

For each control you identify, you must return structured JSON with the following fields:
- control_id: a unique identifier (e.g. CTRL-001, CTRL-002, ...)
- control_type: the type of control as defined in the recipe methodology (Preventive, Detective, or Corrective)
- title: a short descriptive title for the process or mechanism. Do not include the word "control" in the title.
- description: a concise, well-written paragraph that naturally incorporates ALL six of the following components.
    Do not use labels or bullet points — write in clear, professional prose that flows naturally.
    Do NOT use self-referential language such as "this control", "the control", or "this control is" anywhere in the description. Describe the process, action, or mechanism directly.
    If any component is not explicitly stated in the document, incorporate the text "TBD" in its place within the paragraph.
    Be concise — include only the language necessary to satisfy each component. Do not elaborate beyond what is needed to cover the six criteria. Every sentence must tie directly to one of the six components below.
    Write in definitive, active language. Never use passive or conditional language including: "must", "should", "could", "may", "is required to", "are required to", "is expected to", "is responsible for", "is designed to", "is intended to", or any similar construction. Describe every action as something that is definitively done.
    Begin the description directly with the WHO and WHEN — do not open with a setup or introductory sentence about the function, team, or program. The first sentence dives straight into the control action.
    Do not include process steps, data entry instructions, or workflow procedures. Do not describe what fields are completed, what information is submitted, or how a form or system is populated. Only describe the action that spots or stops the risk, the outcomes, and the method.
    The paragraph must cover:
    WHO: the specific team, role, system, or individual that performs the control activity
    WHEN: the timing or trigger for when the control executes (e.g. real-time, daily, per-event, at submission)
    WHAT: the specific action that directly spots or stops the risk — this must be the detection or prevention mechanism itself, not the design or maintenance of a tool. Ask yourself: what is the moment the risk is caught or blocked?
    OUTCOMES: explicitly state BOTH the happy path (what happens when the control passes — e.g. the transaction is approved, the item proceeds, the record is accepted) AND the sad path (what happens when the control flags or fails — e.g. the item is blocked, escalated, or rejected). Both outcomes must be present.
    HOW: the method or mechanism used to execute the control (e.g. automated system check, manual review, reconciliation, completeness validation)
    WHY: the purpose of the control and the specific risk it mitigates
- risk_mitigated: the specific risk or risk category this control addresses (e.g. "Sanctions Risk", "Consumer Protection Risk", "Payment Processing Risk")
- risk_event: a formally written risk event statement written as a single flowing sentence that naturally incorporates all three of the following components:
    SITUATION (When + Who): the specific context or trigger when exposure occurs, AND who is exposed or who poses the threat. The "who" must be explicitly named — it cannot be omitted. For threat-actor risks, name the threat actor (e.g. "unauthorized parties", "malicious actors", "sanctioned individuals"). For operational risks, name the affected party (e.g. "teen customers and their sponsoring adults", "customers", "the organization"). The who must appear naturally within the opening of the sentence (e.g. "When users access their accounts, unauthorized parties..." or "When the feature is launched, teen customers and their sponsoring adults...")
    WHAT COULD GO WRONG: the specific action or behavior that realizes the risk, including the manner in which it occurs (e.g. "...attempt to initiate transactions in a fraudulent manner...")
    BAD OUTCOME: the specific harmful consequence, introduced with "leading to" (e.g. "...leading to theft of customer funds and account compromise.")
    The sentence must follow this natural flow: "When [context/trigger], [who] [what could go wrong] in a [manner], leading to [specific bad outcome]."
    Example 1: "When users access their accounts, unauthorized parties attempt to initiate transactions in a fraudulent manner, leading to theft of customer funds and account compromise."
    Example 2: "When accessing the platform, malicious actors mask their true location or device identifiers in a sophisticated manner, leading to circumvention of geographic restrictions and sanctions violations."
    CRITICAL: A risk event can NEVER reference the failure of a control. Do not mention controls, control failures, or the absence of controls anywhere in the risk event. Never use "failure to" anywhere in the risk event.
- control_owner: the team or role accountable for this control (i.e. who owns and maintains it)
- control_performer: the specific individual, team, application, model, or third party that physically executes the control activity. This may be the same as the control_owner or different (e.g. an automated system, a vendor, or a specific role within the owning team)
- frequency: how often the control operates (e.g. real-time, daily, weekly, per-event)
- page_number: the page number in the source document where the control language appears
- section_heading: the section heading under which the control language appears
- source_excerpt: the exact quoted text from the document that evidences this control
- expected_evidence: a description of what valid evidence of this control's execution would look like, based on the control type

Return ONLY a valid JSON array of control objects. If no controls are found, return an empty array: []. Do not include any other text."""


_MAX_CHUNK_CHARS = 6_000  # smaller chunks → smaller JSON responses → avoids firewall truncation


def _call_with_retry(fn, max_retries: int = 7):
    """Call fn(), retrying on rate limit, connection, and server errors with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return fn()
        except anthropic.RateLimitError:
            if attempt == max_retries - 1:
                raise
            wait = 30 * (2 ** attempt)
            print(f"    Rate limited — waiting {wait}s before retry ({attempt + 1}/{max_retries})...", flush=True)
            time.sleep(wait)
        except anthropic.APIConnectionError:
            if attempt == max_retries - 1:
                raise
            wait = 15 * (2 ** attempt)
            print(f"    Connection error — waiting {wait}s before retry ({attempt + 1}/{max_retries})...", flush=True)
            time.sleep(wait)
        except anthropic.APIStatusError as e:
            if attempt == max_retries - 1:
                raise
            if e.status_code in (500, 529):
                wait = 15 * (2 ** attempt)
                print(f"    Server error {e.status_code} — waiting {wait}s before retry ({attempt + 1}/{max_retries})...", flush=True)
                time.sleep(wait)
            else:
                raise


def _extract_controls_from_chunk(recipe_content: str, pages: list[dict], ctrl_offset: int, _page_attempt: int = 0) -> list[dict]:
    """Extract controls from a single chunk of pages, renumbering from ctrl_offset."""
    pages_summary = _format_pages_for_prompt(pages)

    user_message = f"""
METHODOLOGY RECIPE:
{recipe_content}

---

SOURCE DOCUMENT (structured by page and section):
{pages_summary}

---

Using the methodology defined in the recipe above, identify and extract all controls present in the source document.
Start control IDs at CTRL-{ctrl_offset:03d}.
Return a JSON array of control objects as specified.
"""

    def _call():
        return client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=8192,
            temperature=0,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

    message = _call_with_retry(_call)

    response_text = message.content[0].text.strip()
    response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
    response_text = re.sub(r'\n?```\s*$', '', response_text).strip()

    if not response_text:
        print(f"    WARNING: Claude returned empty response for chunk ({len(pages)} pages)", flush=True)
        return []

    try:
        parsed = json.loads(response_text)
        if isinstance(parsed, list) and len(parsed) == 0:
            print(f"    NOTE: Claude returned [] for chunk ({len(pages)} pages) — no controls found in this section", flush=True)
        return parsed
    except json.JSONDecodeError:
        if len(pages) <= 1:
            if _page_attempt == 0:
                # First failure — transient error, retry once as-is
                print(f"    Single-page JSON error — retrying (attempt 1/3)...", flush=True)
                time.sleep(5)
                return _extract_controls_from_chunk(recipe_content, pages, ctrl_offset, 1)
            if _page_attempt == 1:
                # Second failure — response too large, split the page text in half
                page = pages[0]
                text = page.get("text", "")
                split_pos = text.rfind(" ", 0, len(text) // 2) or len(text) // 2
                if len(text) > 500 and split_pos > 0:
                    print(f"    Single-page JSON error — splitting page text and retrying (attempt 2/3)...", flush=True)
                    page_a = {**page, "text": text[:split_pos]}
                    page_b = {**page, "text": text[split_pos:]}
                    left = _extract_controls_from_chunk(recipe_content, [page_a], ctrl_offset)
                    right = _extract_controls_from_chunk(recipe_content, [page_b], ctrl_offset + len(left))
                    return left + right
                # Text too short to split — one more straight retry
                print(f"    Single-page JSON error — retrying (attempt 2/3)...", flush=True)
                time.sleep(5)
                return _extract_controls_from_chunk(recipe_content, pages, ctrl_offset, 2)
            # Third failure — give up on this page
            section = pages[0].get("section", "") or f"Page {pages[0].get('page', '?')}"
            print(f"    Warning: skipping page after 3 failed attempts — section: {section}", flush=True)
            return [{"_skipped": True, "section": section}]
        print(f"    JSON parse error — splitting chunk and retrying...", flush=True)
        mid = len(pages) // 2
        left = _extract_controls_from_chunk(recipe_content, pages[:mid], ctrl_offset)
        right = _extract_controls_from_chunk(recipe_content, pages[mid:], ctrl_offset + len(left))
        return left + right


def extract_controls(recipe_content: str, document_parsed: dict) -> list[dict]:
    """
    Extract controls from a parsed document using the recipe methodology.
    Automatically chunks large documents to stay within token limits.
    """
    pages = document_parsed["pages"]

    # Build chunks by accumulating pages up to _MAX_CHUNK_CHARS
    chunks = []
    current_chunk = []
    current_size = 0

    for page in pages:
        page_size = len(page.get("text", ""))
        if current_chunk and current_size + page_size > _MAX_CHUNK_CHARS:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0
        current_chunk.append(page)
        current_size += page_size

    if current_chunk:
        chunks.append(current_chunk)

    all_controls = []
    skipped_sections = []
    ctrl_offset = 1

    for i, chunk in enumerate(chunks):
        if len(chunks) > 1:
            print(f"  Processing chunk {i + 1}/{len(chunks)} ({len(chunk)} pages)...", flush=True)
        if i > 0:
            time.sleep(5)
        results = _extract_controls_from_chunk(recipe_content, chunk, ctrl_offset)

        if not isinstance(results, list):
            print(f"    WARNING: chunk {i + 1} returned non-list ({type(results).__name__}) — skipping", flush=True)
            continue

        chunk_controls = 0
        for item in results:
            if not isinstance(item, dict):
                print(f"    WARNING: non-dict item in chunk {i + 1} results — skipping item", flush=True)
                continue
            if item.get("_skipped"):
                skipped_sections.append(item["section"])
            else:
                all_controls.append(item)
                chunk_controls += 1

        print(f"    Chunk {i + 1}: {chunk_controls} control(s) extracted", flush=True)
        ctrl_offset += len(all_controls) + 1

    print(f"  Total controls extracted: {len(all_controls)}", flush=True)
    return all_controls, skipped_sections


COVERAGE_EVALUATION_PROMPT = """You are a risk and controls documentation specialist.
You will be given a single risk statement and a list of extracted controls.
Your only job is to determine whether the extracted controls collectively and fully address ALL aspects of this risk.

Answer with a JSON object containing exactly two fields:
{
  "fully_addressed": true or false,
  "controls_addressing": "comma-separated list of control IDs that address this risk, or None if none do",
  "gap_description": "if fully_addressed is false, explain specifically what aspects of the risk are not covered. If fully_addressed is true, leave this as an empty string."
}

RULES:
- fully_addressed is true ONLY if the controls collectively address every aspect of the risk — who is exposed, what could go wrong, and the potential bad outcome
- fully_addressed is false if any aspect of the risk is not covered, even partially
- Never reference control failures or use "failure to" in the gap_description
- Return ONLY the JSON object. No other text."""


RECOMMENDATION_PROMPT = """You are a risk and controls documentation specialist.
You will be given a risk statement and gap description. Your job is to draft one or more recommended controls to fully close the gap.

Determine how many controls are needed to fully mitigate the risk. If a single well-designed control can fully address the risk, return one. If full mitigation requires multiple controls (e.g. a preventive control AND a detective control), return all that are needed.

CRITICAL RULES FOR RECOMMENDED CONTROLS:
- Recommendations are DRAFT ONLY and require SME validation
- Do not use the word "control" in the title
- Do not use self-referential language in descriptions
- Follow the WHO/WHEN/WHAT/OUTCOMES/HOW/WHY framework in a single flowing paragraph for each description
- If a component cannot be determined from context, use "TBD"
- Never use "failure to" anywhere

The description paragraph MUST include all six components with the following precise definitions. Be concise — include only the language necessary to satisfy each component. Do not elaborate beyond what is needed. Every sentence must tie directly to one of the six components below.
Write in definitive, active language. Never use passive or conditional language including: "must", "should", "could", "may", "is required to", "are required to", "is expected to", "is responsible for", "is designed to", "is intended to", or any similar construction. Describe every action as something that is definitively done.
Begin the description directly with the WHO and WHEN — do not open with a setup or introductory sentence about the function, team, or program. The first sentence dives straight into the control action.
- WHO: the specific team, role, system, or individual that performs the control activity
- WHEN: the timing or trigger for when the control executes (e.g. real-time, daily, per-event, at submission)
- WHAT: the specific action that directly spots or stops the risk — this must be the detection or prevention mechanism itself, not the design or maintenance of a tool or template. Ask yourself: what is the moment the risk is caught or blocked?
- OUTCOMES: explicitly state BOTH the happy path (what happens when the control passes — e.g. the transaction is approved, the submission proceeds, the record is accepted) AND the sad path (what happens when the control fails or flags — e.g. the submission is blocked, the item is escalated, the record is rejected). Both outcomes must be present.
- HOW: the method or mechanism used to execute the control (e.g. automated system check, manual review, reconciliation, completeness validation)
- WHY: the purpose of the control and the specific risk it mitigates

Each recommended control must include a risk_event that describes the inherent exposure the control is designed to mitigate. Risk events must be written as if no controls exist — they reflect the underlying threat or condition, not the absence of a control. If removing the control would remove the risk, the risk_event is wrong.

The risk_event must follow this structure:
"When [context/trigger], [who — threat actor or affected party] [what could go wrong] in a [manner], leading to [specific bad outcome]."
- The "who" must always be explicitly named
- Never reference controls, control failures, or the absence of controls anywhere in the risk_event

Return ONLY a valid JSON array. Even if only one control is needed, return it as a single-element array:
[
  {
    "control_type": "Preventive|Detective|Corrective",
    "title": "...",
    "description": "...",
    "control_owner": "TBD or inferred owner",
    "control_performer": "TBD or the specific individual, team, application, model, or third party that executes this control",
    "frequency": "TBD or inferred frequency",
    "expected_evidence": "...",
    "risk_event": "..."
  }
]
No other text."""


CONTROL_REPOSITORY_MATCH_PROMPT = """You are a risk and controls documentation specialist.
You will be given an extracted control and a list of candidate controls from an existing repository.

Determine if any candidate describes the same control:
- EXACT: same mechanism, same purpose, same scope — this is the same control. Be conservative — only mark exact if it is clearly the same control.
- PARTIAL: the candidate addresses the same risk area or uses a related mechanism, even if the scope, method, frequency, or owner differs. Err on the side of marking partial if there is any meaningful overlap in purpose.
- NONE: no meaningful overlap in purpose or risk area.

Return ONLY a valid JSON object:
{
  "match_type": "exact" | "partial" | "none",
  "protecht_id": "<Protecht ID from the best matching candidate, or null if none>"
}
No other text."""


REPOSITORY_MATCH_PROMPT = """You are a risk and controls documentation specialist.
You will be given a risk statement, a gap description, and a list of candidate controls from an existing control repository.

For each candidate, determine whether it:
- EXACTLY addresses the gap: the control fully closes the identified gap — same risk, same scope, complete mitigation. Be conservative — only mark exact if the candidate clearly and completely closes the gap.
- PARTIALLY addresses the gap: the candidate addresses the same risk area or closes part of the gap, even if it does not fully mitigate all aspects. Err on the side of marking partial if there is any meaningful overlap — a control that reduces but does not eliminate the risk should be marked partial.
- Does NOT match: no meaningful overlap with this risk or gap.

Return ONLY a valid JSON object:
{
  "exact_matches": [<id>, ...],
  "partial_matches": [<id>, ...]
}

Where each value is the integer "id" field from the candidate list.
Return empty arrays if no matches exist. No other text."""


_STOP_WORDS = {
    'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
    'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
    'could', 'should', 'may', 'might', 'that', 'this', 'these', 'those',
    'when', 'which', 'who', 'whom', 'how', 'what', 'where', 'why', 'if',
    'then', 'than', 'as', 'up', 'out', 'about', 'into', 'through', 'each',
    'all', 'both', 'more', 'most', 'other', 'some', 'such', 'no', 'not',
    'only', 'same', 'so', 'too', 'very', 'just', 'can', 'their', 'they',
    'them', 'its', 'it', 'he', 'she', 'we', 'our', 'your', 'leading',
    'manner', 'attempt', 'attempts', 'also', 'made', 'make', 'used', 'using',
    'based', 'including', 'related', 'specific', 'given', 'without', 'certain'
}


def _extract_keywords(text: str) -> list[str]:
    words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())
    return list({w for w in words if w not in _STOP_WORDS})[:10]


def _has_repository() -> bool:
    from database.db import get_session, RepositoryControl
    session = get_session()
    try:
        return session.query(RepositoryControl).first() is not None
    finally:
        session.close()


def _find_repository_matches(risk_statement: str, gap_description: str) -> dict:
    """Pre-filter repository controls by keyword, score by relevance, then use Claude to classify."""
    from database.db import get_session, RepositoryControl
    from sqlalchemy import or_

    session = get_session()
    try:
        keywords = _extract_keywords(f"{risk_statement} {gap_description}")

        if keywords:
            conditions = []
            for kw in keywords:
                conditions.append(RepositoryControl.control_name.ilike(f"%{kw}%"))
                conditions.append(RepositoryControl.control_description.ilike(f"%{kw}%"))
            # Fetch a broad pool then rank by keyword match count in Python
            raw_candidates = session.query(RepositoryControl).filter(or_(*conditions)).limit(200).all()

            def _score(c):
                text = f"{c.control_name} {c.control_description}".lower()
                return sum(1 for kw in keywords if kw in text)

            candidates = sorted(raw_candidates, key=_score, reverse=True)[:50]
        else:
            candidates = session.query(RepositoryControl).limit(50).all()

        if not candidates:
            return {"exact_matches": [], "partial_matches": []}

        candidates_data = [
            {
                "id": c.id,
                "protecht_id": c.protecht_id,
                "control_name": c.control_name,
                "control_description": c.control_description,
                "control_type": c.control_type,
            }
            for c in candidates
        ]

        user_message = (
            f"RISK STATEMENT:\n{risk_statement}\n\n"
            f"GAP DESCRIPTION:\n{gap_description}\n\n"
            f"CANDIDATE CONTROLS FROM REPOSITORY:\n{json.dumps(candidates_data, indent=2)}\n\n"
            f"Which of these controls exactly or partially address this gap?"
        )

        message = _call_with_retry(lambda: client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            temperature=0,
            system=REPOSITORY_MATCH_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        ))

        response_text = message.content[0].text.strip()
        response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
        response_text = re.sub(r'\n?```\s*$', '', response_text)

        try:
            return json.loads(response_text)
        except json.JSONDecodeError:
            return {"exact_matches": [], "partial_matches": []}
    finally:
        session.close()


def match_controls_to_repository(controls: list[dict]) -> list[dict]:
    """
    For each extracted control, find the best match in the repository.
    Returns list of {control_id, match_type, protecht_id} dicts.
    Only runs if repository controls are loaded.
    """
    if not _has_repository():
        return []

    from database.db import get_session, RepositoryControl
    from sqlalchemy import or_

    results = []
    total = len(controls)

    for i, ctrl in enumerate(controls):
        print(f"  Matching control {i + 1}/{total}: {ctrl.get('control_id')}...")
        search_text = f"{ctrl.get('title', '')} {ctrl.get('description', '')} {ctrl.get('risk_mitigated', '')}"
        keywords = _extract_keywords(search_text)

        session = get_session()
        try:
            if keywords:
                conditions = []
                for kw in keywords:
                    conditions.append(RepositoryControl.control_name.ilike(f"%{kw}%"))
                    conditions.append(RepositoryControl.control_description.ilike(f"%{kw}%"))
                raw_candidates = session.query(RepositoryControl).filter(or_(*conditions)).limit(200).all()

                def _score(c):
                    text = f"{c.control_name} {c.control_description}".lower()
                    return sum(1 for kw in keywords if kw in text)

                candidates = sorted(raw_candidates, key=_score, reverse=True)[:50]
            else:
                candidates = []

            if not candidates:
                results.append({"control_id": ctrl.get("control_id"), "match_type": "none", "protecht_id": None})
                continue

            candidates_data = [
                {
                    "protecht_id": c.protecht_id,
                    "control_name": c.control_name,
                    "control_description": c.control_description,
                    "control_type": c.control_type,
                }
                for c in candidates
            ]

            user_message = (
                f"EXTRACTED CONTROL:\n"
                f"Title: {ctrl.get('title')}\n"
                f"Description: {ctrl.get('description')}\n"
                f"Type: {ctrl.get('control_type')}\n\n"
                f"REPOSITORY CANDIDATES:\n{json.dumps(candidates_data, indent=2)}\n\n"
                f"Does any candidate describe the same control?"
            )

            message = _call_with_retry(lambda: client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=256,
                temperature=0,
                system=CONTROL_REPOSITORY_MATCH_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            ))

            response_text = message.content[0].text.strip()
            response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
            response_text = re.sub(r'\n?```\s*$', '', response_text)

            try:
                result = json.loads(response_text)
                results.append({
                    "control_id": ctrl.get("control_id"),
                    "match_type": result.get("match_type", "none"),
                    "protecht_id": result.get("protecht_id"),
                })
            except json.JSONDecodeError:
                results.append({"control_id": ctrl.get("control_id"), "match_type": "none", "protecht_id": None})

        finally:
            session.close()

        time.sleep(1)  # small delay to avoid burst rate limiting

    return results


RISK_IDENTIFICATION_PROMPT = """You are a risk and controls documentation specialist.
Your job is to identify distinct HIGH-LEVEL risk themes present in the source document.

CRITICAL RULES:
- Think in terms of risk CATEGORIES, not individual scenarios. Multiple similar scenarios should be consolidated into one risk statement.
- Do NOT create a separate risk for every product feature, edge case, or population segment. Group them under a shared theme.
- A risk statement represents a broad category of exposure — multiple controls may address it.
- Only create a separate risk if it represents a genuinely distinct type of harm or threat actor that cannot reasonably be grouped with another risk.
- Never use "failure to" anywhere.
- Never reference controls or control failures.

For each risk, return:
- name: a short descriptive title (3–7 words, e.g. "Product Launch Compliance Risk", "Third-Party Partner Obligation Risk")
- statement: a single flowing sentence: "When [context/trigger], [who — threat actor or affected party] [what could go wrong] in a [manner], leading to [specific bad outcome]."
  The "who" must always be explicitly named. Keep the statement broad enough that multiple controls could plausibly address it.

Return ONLY a valid JSON array of objects. No other text.
Example: [{"name": "Identity Verification Risk", "statement": "When customers onboard, bad actors attempt to circumvent identity verification in a fraudulent manner, leading to unauthorized account access and financial crime."}, ...]"""


def _identify_risks_in_pages(pages: list[dict]) -> list[dict]:
    """Identify risks in a single chunk of pages. Returns list of {name, statement} dicts."""
    pages_summary = _format_pages_for_prompt(pages)

    message = _call_with_retry(lambda: client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        temperature=0,
        system=RISK_IDENTIFICATION_PROMPT,
        messages=[{"role": "user", "content": f"SOURCE DOCUMENT:\n{pages_summary}\n\nList all risks present in this document."}],
    ))

    response_text = message.content[0].text.strip()
    response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
    response_text = re.sub(r'\n?```\s*$', '', response_text)

    try:
        results = json.loads(response_text)
        # Normalise: accept plain strings from older prompt format
        normalised = []
        for r in results:
            if isinstance(r, dict) and "statement" in r:
                normalised.append(r)
            elif isinstance(r, str):
                normalised.append({"name": "", "statement": r})
        return normalised
    except json.JSONDecodeError:
        return []


_CONSOLIDATE_RISKS_PROMPT = """You are a risk and controls documentation specialist.
You will be given a list of risks (each with a name and statement) collected from different sections of a document.
Many of these will be duplicates or near-duplicates of the same underlying risk.

Your job: consolidate this list into distinct high-level risk themes by merging risks that describe the same category of exposure.
- Only keep risks separate if they represent genuinely distinct types of harm or threat actors
- Keep statements broad enough that multiple controls could plausibly address each one
- Preserve the sentence structure: "When [context], [who] [what could go wrong] in a [manner], leading to [bad outcome]."
- The name must be a short descriptive title (3–7 words) derived from the statement
- The name must be consistent with the statement — do not reuse the same name for different risks
- Never use "failure to" anywhere
- Never reference controls or control failures

Return ONLY a valid JSON array of objects with "name" and "statement" fields. No other text."""


def _consolidate_risks(raw_risks: list[dict]) -> list[dict]:
    """Consolidate raw per-chunk risks into high-level themes using Claude."""
    if len(raw_risks) <= 15:
        return raw_risks

    user_message = f"RAW RISK LIST:\n{json.dumps(raw_risks, indent=2)}\n\nConsolidate into high-level risk themes by merging duplicates and near-duplicates."

    message = _call_with_retry(lambda: client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        temperature=0,
        system=_CONSOLIDATE_RISKS_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    ))

    response_text = message.content[0].text.strip()
    response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
    response_text = re.sub(r'\n?```\s*$', '', response_text).strip()

    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return raw_risks


def _identify_risks(document_parsed: dict) -> list[dict]:
    """Step 1: Identify high-level risks. Returns list of {name, statement} dicts."""
    pages = document_parsed["pages"]

    # Build chunks same way as extract_controls
    chunks = []
    current_chunk = []
    current_size = 0
    for page in pages:
        page_size = len(page.get("text", ""))
        if current_chunk and current_size + page_size > _MAX_CHUNK_CHARS:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0
        current_chunk.append(page)
        current_size += page_size
    if current_chunk:
        chunks.append(current_chunk)

    all_risks = []
    seen = set()
    for chunk in chunks:
        risks = _identify_risks_in_pages(chunk)
        for r in risks:
            stmt = r.get("statement", "")
            if stmt and stmt not in seen:
                seen.add(stmt)
                all_risks.append(r)

    # Consolidate across chunks into high-level themes
    consolidated = _consolidate_risks(all_risks)
    print(f"  Risk identification: {len(all_risks)} raw → {len(consolidated)} consolidated", flush=True)
    return consolidated


def _filter_relevant_controls(risk_statement: str, controls: list[dict], max_controls: int = 50) -> list[dict]:
    """Return up to max_controls most relevant controls for a given risk statement."""
    keywords = _extract_keywords(risk_statement)
    if not keywords:
        return controls[:max_controls]

    scored = []
    for ctrl in controls:
        searchable = " ".join([
            ctrl.get("title", ""),
            ctrl.get("description", ""),
            ctrl.get("risk_mitigated", ""),
            ctrl.get("risk_event", ""),
        ]).lower()
        score = sum(1 for kw in keywords if kw in searchable)
        scored.append((score, ctrl))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [ctrl for _, ctrl in scored[:max_controls]]


def _evaluate_coverage(risk_statement: str, controls: list[dict]) -> dict:
    """Evaluate whether the given controls fully address the given risk."""
    relevant = _filter_relevant_controls(risk_statement, controls)

    user_message = f"""RISK STATEMENT:
{risk_statement}

EXTRACTED CONTROLS:
{json.dumps(relevant, indent=2)}

Do the extracted controls collectively and fully address ALL aspects of this risk?"""

    message = _call_with_retry(lambda: client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        temperature=0,
        system=COVERAGE_EVALUATION_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    ))

    response_text = message.content[0].text.strip()
    response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
    response_text = re.sub(r'\n?```\s*$', '', response_text)

    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return {"fully_addressed": False, "controls_addressing": "None", "gap_description": ""}


REPOSITORY_COVERAGE_PROMPT = """You are a risk and controls documentation specialist.
You will be given a risk statement, an initial gap description, and one or more controls from an existing control repository that have been identified as potentially relevant.

Your job is to evaluate how effectively the repository controls mitigate this risk and assign a coverage tier:
- Tier 3 — Fully Covered: the repository controls collectively and fully address ALL aspects of this risk. No gaps remain.
- Tier 2 — Partially Adequate Coverage: the repository controls address some but not all aspects of this risk. A gap remains.
- Tier 1 — No Coverage: the repository controls do not meaningfully mitigate this risk despite appearing related.

Be rigorous. A control only qualifies as full coverage if it addresses the who, what, and bad outcome in the risk statement.

Return ONLY a valid JSON object:
{
  "coverage_tier": 1 | 2 | 3,
  "gap_description": "description of what remains unmitigated, or empty string if fully covered"
}
No other text."""


def _fetch_repository_controls_by_ids(ids: list) -> list[dict]:
    """Fetch repository control details for a list of integer IDs."""
    from database.db import get_session, RepositoryControl
    session = get_session()
    try:
        controls = session.query(RepositoryControl).filter(RepositoryControl.id.in_(ids)).all()
        return [
            {
                "id": c.id,
                "protecht_id": c.protecht_id,
                "control_name": c.control_name,
                "control_description": c.control_description,
                "control_type": c.control_type,
            }
            for c in controls
        ]
    finally:
        session.close()


def _evaluate_repository_coverage(risk_statement: str, gap_description: str, repo_controls: list[dict]) -> dict:
    """Re-evaluate coverage tier after repository matches are found."""
    user_message = (
        f"RISK STATEMENT:\n{risk_statement}\n\n"
        f"INITIAL GAP DESCRIPTION:\n{gap_description}\n\n"
        f"REPOSITORY CONTROLS:\n{json.dumps(repo_controls, indent=2)}\n\n"
        f"How effectively do these repository controls mitigate this risk?"
    )

    message = _call_with_retry(lambda: client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        temperature=0,
        system=REPOSITORY_COVERAGE_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    ))

    response_text = message.content[0].text.strip()
    response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
    response_text = re.sub(r'\n?```\s*$', '', response_text).strip()

    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return {"coverage_tier": 2, "gap_description": gap_description}


def _generate_recommendations(risk_statement: str, gap_description: str) -> list[dict]:
    """Generate one or more draft recommended controls to fully close a gap."""
    user_message = f"""RISK STATEMENT:
{risk_statement}

GAP DESCRIPTION:
{gap_description}

Draft all recommended controls needed to fully mitigate this risk."""

    message = _call_with_retry(lambda: client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        temperature=0,
        system=RECOMMENDATION_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    ))

    response_text = message.content[0].text.strip()
    response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
    response_text = re.sub(r'\n?```\s*$', '', response_text)

    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return []


RISK_EVENT_FIX_PROMPT = """You are a risk and controls documentation specialist.
You will be given a control title, description, and the risk statement that triggered this recommendation.
Your only job is to write a valid risk_event for this control.

The risk_event must describe the inherent exposure the control is designed to mitigate, written as if no controls exist.
A risk exists independently of any controls — never reference controls, control failures, or the absence of controls.

The risk_event must follow this structure:
"When [context/trigger], [who — threat actor or affected party] [what could go wrong] in a [manner], leading to [specific bad outcome]."
- The "who" must always be explicitly named
- Never use "failure to" anywhere

Return ONLY a valid JSON object with a single field:
{"risk_event": "..."}
No other text."""


def _is_valid_risk_event(risk_event: str) -> bool:
    """Return True if the risk_event is a properly written event statement."""
    if not risk_event:
        return False
    cleaned = risk_event.strip().lower()
    if cleaned in ("none", "tbd", "n/a", ""):
        return False
    if not cleaned.startswith("when "):
        return False
    return True


def _fix_risk_event(rec: dict, risk_statement: str) -> str:
    """Generate a valid risk_event for a recommendation that is missing or invalid."""
    user_message = f"""CONTROL TITLE: {rec.get('title', 'TBD')}

CONTROL DESCRIPTION:
{rec.get('description', 'TBD')}

TRIGGERING RISK STATEMENT:
{risk_statement}

Write a valid risk_event for this control."""

    message = _call_with_retry(lambda: client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        temperature=0,
        system=RISK_EVENT_FIX_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    ))

    response_text = message.content[0].text.strip()
    response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
    response_text = re.sub(r'\n?```\s*$', '', response_text)

    try:
        return json.loads(response_text).get("risk_event", "TBD")
    except json.JSONDecodeError:
        return "TBD"


def analyze_gaps(recipe_content: str, document_parsed: dict, extracted_controls: list[dict]) -> dict:
    """
    Three-step gap analysis:
    Step 1 — Identify and lock all risks in the document
    Step 2 — Evaluate coverage for each risk individually
    Step 3 — Generate recommendations for every Tier 1 and Tier 2 risk

    Tier assignment:
      Tier 1 — No controls address this risk at all (determined by code)
      Tier 2 — Controls exist but do not fully address all aspects of the risk
      Tier 3 — Controls fully and adequately address the entire risk

    Returns:
        Dict with 'gap_findings' and 'recommendations' lists
    """
    # Step 1: Lock in the risk list and assign RE-00# IDs
    identified_risks = _identify_risks(document_parsed)

    gap_findings = []
    recommendations = []
    repository_matches = []
    rec_counter = 1
    use_repository = _has_repository()

    print(f"  Identified {len(identified_risks)} risks. Evaluating coverage...")

    # Step 2: Evaluate coverage for each risk individually
    for i, risk in enumerate(identified_risks):
        risk_name = risk.get("name", "") if isinstance(risk, dict) else ""
        risk_statement = risk.get("statement", risk) if isinstance(risk, dict) else risk
        print(f"  Risk {i + 1}/{len(identified_risks)}: {risk_statement[:80]}...")
        risk_event_id = f"RE-{i + 1:03d}"
        coverage = _evaluate_coverage(risk_statement, extracted_controls)

        controls_addressing = coverage.get("controls_addressing", "None")
        fully_addressed = coverage.get("fully_addressed", False)
        gap_description = coverage.get("gap_description", "")

        # Tier 1: code determines — no controls address the risk
        no_controls = (
            not controls_addressing
            or controls_addressing.strip().lower() == "none"
        )

        if no_controls:
            tier = 1
            controls_addressing = "None"
            if not gap_description:
                gap_description = "No controls exist in the document to address this risk."
        elif fully_addressed:
            tier = 3
            gap_description = ""
        else:
            tier = 2

        gap_findings.append({
            "risk_event_id": risk_event_id,
            "risk_name": risk_name,
            "risk_statement": risk_statement,
            "coverage_tier": tier,
            "controls_addressing": controls_addressing,
            "gap_description": gap_description,
        })

        # Step 3: Repository matching + recommendations for Tier 1 and Tier 2
        if tier in (1, 2):
            exact_ids = []
            partial_ids = []

            if use_repository:
                repo_result = _find_repository_matches(risk_statement, gap_description)
                exact_ids = repo_result.get("exact_matches", [])
                partial_ids = repo_result.get("partial_matches", [])

                matched_ids = exact_ids + partial_ids
                if matched_ids:
                    # Re-evaluate coverage tier based on how effectively
                    # repository controls actually mitigate this risk
                    repo_controls = _fetch_repository_controls_by_ids(matched_ids)
                    coverage_eval = _evaluate_repository_coverage(
                        risk_statement, gap_description, repo_controls
                    )
                    tier = coverage_eval.get("coverage_tier", tier)
                    gap_description = coverage_eval.get("gap_description", gap_description)
                    # Populate controls_addressing with matched Protecht IDs so
                    # the Excel Draft Controls tab can display the repository controls
                    repo_ctrl_ids = ", ".join(
                        c.get("protecht_id") or str(c.get("id", ""))
                        for c in repo_controls
                    )
                    if repo_ctrl_ids:
                        controls_addressing = repo_ctrl_ids
                    # Update the gap finding already appended above
                    gap_findings[-1]["coverage_tier"] = tier
                    gap_findings[-1]["gap_description"] = gap_description
                    gap_findings[-1]["controls_addressing"] = controls_addressing

                for rid in exact_ids:
                    repository_matches.append({
                        "risk_statement": risk_statement,
                        "gap_tier": tier,
                        "gap_description": gap_description,
                        "repository_control_id": rid,
                        "match_type": "exact",
                    })
                for rid in partial_ids:
                    repository_matches.append({
                        "risk_statement": risk_statement,
                        "gap_tier": tier,
                        "gap_description": gap_description,
                        "repository_control_id": rid,
                        "match_type": "partial",
                    })

            # Only generate new recommendations if no exact match found
            if not exact_ids:
                recs = _generate_recommendations(risk_statement, gap_description)
                for rec in recs:
                    if not _is_valid_risk_event(rec.get("risk_event", "")):
                        rec["risk_event"] = _fix_risk_event(rec, risk_statement)
                    rec["recommendation_id"] = f"REC-{rec_counter:03d}"
                    rec["gap_tier"] = tier
                    rec["risk_event_id"] = risk_event_id
                    rec["risk_name"] = risk_name
                    rec["risk_statement"] = risk_statement
                    recommendations.append(rec)
                    rec_counter += 1

    return {
        "gap_findings": gap_findings,
        "recommendations": recommendations,
        "repository_matches": repository_matches,
    }


def _format_pages_for_prompt(pages: list[dict]) -> str:
    """Format parsed document pages into a readable string for the prompt."""
    formatted = []
    for page in pages:
        page_num = page.get("page", "?")
        section = page.get("section", "")
        text = page.get("text", "")
        header = f"[Page {page_num}]"
        if section:
            header += f" [{section}]"
        formatted.append(f"{header}\n{text}")
    return "\n\n".join(formatted)
