import csv
import io
from database.db import get_session, RepositoryControl


def load_repository(file_bytes: bytes, filename: str) -> int:
    """
    Parse a CSV control repository and store in DB, replacing any existing records.
    Returns the count of controls loaded.
    """
    content = file_bytes.decode("utf-8-sig")  # handle BOM
    reader = csv.DictReader(io.StringIO(content))

    session = get_session()
    try:
        session.query(RepositoryControl).delete()

        count = 0
        for row in reader:
            ctrl = RepositoryControl(
                protecht_id=row.get("Protecht ID", "").strip(),
                owning_workstream=row.get("Owning Workstream", "").strip(),
                control_name=row.get("Control Name", "").strip(),
                control_description=row.get("Control Description", "").strip(),
                control_owner=row.get("Control Owner (DRI)", "").strip(),
                control_performer=row.get("Control Performer", "").strip(),
                control_type=row.get("Control Type", "").strip(),
                automation_type=row.get(
                    "Fully Fully Manual or Fully Fully Automated / System Enforced / System Enforced?", ""
                ).strip(),
                frequency=row.get("Control Frequency", "").strip(),
                impacted_products=row.get("Impacted Products/Services", "").strip(),
                supporting_documentation=row.get("Supporting Documentation & Ownership", "").strip(),
            )
            session.add(ctrl)
            count += 1

        session.commit()
        return count
    finally:
        session.close()


def get_repository_count() -> int:
    session = get_session()
    try:
        return session.query(RepositoryControl).count()
    finally:
        session.close()
