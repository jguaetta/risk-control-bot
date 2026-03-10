import sys
import os
import json

sys.path.insert(0, os.path.dirname(__file__))

from ai.control_extractor import extract_controls

SAMPLE_RECIPE = """
RISK AND CONTROLS DOCUMENTATION METHODOLOGY

1. CONTROL IDENTIFICATION CRITERIA
A control is any process, procedure, or mechanism that mitigates a risk. Controls must be:
- Clearly defined with an owner and frequency
- Tied to a specific risk or risk category
- Verifiable through evidence of execution

2. CONTROL TYPES
- Preventive: Stops a risk event from occurring (e.g. blocking a transaction, requiring approval)
- Detective: Identifies a risk event after it occurs (e.g. monitoring, reconciliation, alerts)
- Corrective: Remedies a risk event after detection (e.g. reversals, escalations, remediation)

3. DOCUMENTATION REQUIREMENTS
Each control must capture:
- Control type (Preventive, Detective, or Corrective)
- A clear title and description of what the control does
- The risk it mitigates
- Who owns the control
- How frequently it operates
- What evidence of execution looks like

4. EVIDENCE REQUIREMENTS BY CONTROL TYPE
- Preventive controls: Evidence should show an instance where the control blocked or prevented an event
  (e.g. a blocked transaction log, a rejected approval request)
- Detective controls: Evidence should show monitoring output or alerts generated
  (e.g. exception reports, reconciliation results, alert logs)
- Corrective controls: Evidence should show remediation actions taken
  (e.g. reversal records, escalation tickets, remediation documentation)
"""

SAMPLE_DOCUMENT = {
    "full_text": "",
    "pages": [
        {
            "page": 1,
            "section": "Overview",
            "text": (
                "Overview\n"
                "This document describes the risk management framework for the payments processing system. "
                "The framework establishes controls to ensure transactions are processed accurately and securely."
            )
        },
        {
            "page": 2,
            "section": "Transaction Risk Controls",
            "text": (
                "Transaction Risk Controls\n"
                "All payment transactions are evaluated by the risk model prior to processing. "
                "Transactions that fail to meet the minimum risk score threshold of 0.75 are automatically blocked "
                "and flagged for review. The risk model owner is the Risk Engineering team, and this control "
                "operates in real-time on every transaction."
            )
        },
        {
            "page": 3,
            "section": "Monitoring and Alerts",
            "text": (
                "Monitoring and Alerts\n"
                "The payments operations team runs a daily reconciliation report comparing processed transactions "
                "against settlement records. Any discrepancies exceeding $500 trigger an automated alert to the "
                "Payments Operations manager. The reconciliation is owned by the Payments Operations team and "
                "occurs every business day."
            )
        },
        {
            "page": 4,
            "section": "Dispute Resolution",
            "text": (
                "Dispute Resolution\n"
                "When a customer dispute is validated, the Disputes team initiates a reversal within 2 business days. "
                "All reversals are logged in the disputes management system with a case ID, amount, reason code, "
                "and approver. The Disputes team owns this process and it is triggered on a per-case basis."
            )
        }
    ]
}

SAMPLE_DOCUMENT["full_text"] = "\n".join(p["text"] for p in SAMPLE_DOCUMENT["pages"])


def main():
    print("Running control extraction test...\n")
    print("=" * 60)

    controls = extract_controls(SAMPLE_RECIPE, SAMPLE_DOCUMENT)

    print(f"Extracted {len(controls)} controls:\n")
    for ctrl in controls:
        print(f"  Control ID:       {ctrl.get('control_id')}")
        print(f"  Title:            {ctrl.get('title')}")
        print(f"  Type:             {ctrl.get('control_type')}")
        print(f"  Page:             {ctrl.get('page_number')}")
        print(f"  Section:          {ctrl.get('section_heading')}")
        print(f"  Description:      {ctrl.get('description')}")
        print(f"  Source Excerpt:   {ctrl.get('source_excerpt')}")
        print(f"  Expected Evidence:{ctrl.get('expected_evidence')}")
        print("-" * 60)

    print("\nFull JSON output:")
    print(json.dumps(controls, indent=2))


if __name__ == "__main__":
    main()
