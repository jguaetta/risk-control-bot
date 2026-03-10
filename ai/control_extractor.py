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
- Only extract controls that are explicitly described in the document. Do not infer or fabricate controls that are not clearly present.
- Every field must be populated using only information explicitly stated in the source document.
- If a field cannot be determined from the document, set its value to "TBD". Never assume, infer, or fabricate values.

For each control you identify, you must return structured JSON with the following fields:
- control_id: a unique identifier (e.g. CTRL-001, CTRL-002, ...)
- control_type: the type of control as defined in the recipe methodology (Preventive, Detective, or Corrective)
- title: a short descriptive title for the process or mechanism. Do not include the word "control" in the title.
- description: a well-written paragraph that naturally incorporates ALL six of the following components.
    Do not use labels or bullet points — write in clear, professional prose that flows naturally.
    Do NOT use self-referential language such as "this control", "the control", or "this control is" anywhere in the description. Describe the process, action, or mechanism directly.
    If any component is not explicitly stated in the document, incorporate the text "TBD" in its place within the paragraph.
    The paragraph must cover:
    WHO: who performs or owns the control (team, role, or system)
    WHEN: when the control is executed (e.g. real-time, daily, per-event)
    WHAT: the specific action taken that spots or stops the risk
    OUTCOMES: all possible outcomes of the control decision (e.g. allow, block, escalate, alert, reverse)
    HOW: the method by which the control is executed (e.g. automated scoring, reconciliation, due diligence, verification, review)
    WHY: the purpose of the control and the risk it mitigates
- risk_mitigated: the specific risk or risk category this control addresses
- control_owner: the team or role responsible for executing and maintaining this control
- frequency: how often the control operates (e.g. real-time, daily, weekly, per-event)
- page_number: the page number in the source document where the control language appears
- section_heading: the section heading under which the control language appears
- source_excerpt: the exact quoted text from the document that evidences this control
- expected_evidence: a description of what valid evidence of this control's execution would look like, based on the control type

Return ONLY a valid JSON array of control objects. Do not include any other text."""


def extract_controls(recipe_content: str, document_parsed: dict) -> list[dict]:
    """
    Use Claude to extract controls from a parsed document using the recipe methodology.

    Args:
        recipe_content: Full text of the methodology recipe
        document_parsed: Output from document_parser.parse_document()

    Returns:
        List of control dicts
    """
    pages_summary = _format_pages_for_prompt(document_parsed["pages"])

    user_message = f"""
METHODOLOGY RECIPE:
{recipe_content}

---

SOURCE DOCUMENT (structured by page and section):
{pages_summary}

---

Using the methodology defined in the recipe above, identify and extract all controls present in the source document.
Return a JSON array of control objects as specified.
"""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )

    response_text = message.content[0].text.strip()

    # Strip markdown code fences if present
    if response_text.startswith("```"):
        response_text = response_text.split("```")[1]
        if response_text.startswith("json"):
            response_text = response_text[4:]

    controls = json.loads(response_text)
    return controls


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
