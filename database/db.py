from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, ForeignKey, text
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
from config import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class Recipe(Base):
    __tablename__ = "recipes"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    uploaded_by = Column(String)
    uploaded_at = Column(DateTime, default=datetime.utcnow)


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    uploaded_by = Column(String)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    recipe_id = Column(Integer, ForeignKey("recipes.id"))
    google_doc_id = Column(String)
    google_doc_url = Column(String)

    google_sheet_id = Column(String)
    google_sheet_url = Column(String)

    recipe = relationship("Recipe")
    controls = relationship("Control", back_populates="document")
    recommended_controls = relationship("RecommendedControl", back_populates="document")
    gap_findings = relationship("GapFinding", back_populates="document")
    repository_matches = relationship("RepositoryMatch", back_populates="document")


class Control(Base):
    __tablename__ = "controls"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False)
    control_id = Column(String)          # e.g. CTRL-001
    control_type = Column(String)        # e.g. preventive, detective
    title = Column(String)
    description = Column(Text)
    risk_mitigated = Column(Text)        # the risk category this control addresses
    risk_event = Column(Text)            # formal risk event statement
    control_owner = Column(String)       # team or role accountable for the control
    control_performer = Column(String)   # individual, team, app, model, or third party that executes it
    frequency = Column(String)           # how often the control operates
    page_number = Column(Integer)
    section_heading = Column(String)
    source_excerpt = Column(Text)        # exact quoted text from document
    expected_evidence = Column(Text)     # what evidence looks like for this type
    repository_match_type = Column(String)   # "existing", "partial", or None
    repository_protecht_id = Column(String)  # Protecht ID of matched repository control
    status = Column(String, default="open")
    created_at = Column(DateTime, default=datetime.utcnow)

    document = relationship("Document", back_populates="controls")
    evidence_logs = relationship("EvidenceLog", back_populates="control")


class EvidenceLog(Base):
    __tablename__ = "evidence_logs"

    id = Column(Integer, primary_key=True)
    control_id = Column(Integer, ForeignKey("controls.id"), nullable=False)
    submitted_by = Column(String)
    evidence_type = Column(String)       # file, message, link, etc.
    evidence_content = Column(Text)      # description or file reference
    slack_file_id = Column(String)       # if a file was uploaded via Slack
    slack_message_ts = Column(String)    # message timestamp for traceability
    notes = Column(Text)
    logged_at = Column(DateTime, default=datetime.utcnow)

    control = relationship("Control", back_populates="evidence_logs")


class GapFinding(Base):
    __tablename__ = "gap_findings"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False)
    risk_statement = Column(Text)
    coverage_tier = Column(Integer)          # 1 = no coverage, 2 = inadequate, 3 = adequate
    controls_addressing = Column(Text)       # comma-separated control IDs
    gap_description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    document = relationship("Document", back_populates="gap_findings")


class RecommendedControl(Base):
    __tablename__ = "recommended_controls"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False)
    recommendation_id = Column(String)       # e.g. REC-001
    gap_tier = Column(Integer)
    risk_statement = Column(Text)
    risk_event = Column(Text)
    control_type = Column(String)
    title = Column(String)
    description = Column(Text)
    control_owner = Column(String)
    control_performer = Column(String)
    frequency = Column(String)
    expected_evidence = Column(Text)
    validation_status = Column(String, default="draft")   # draft, validated, rejected
    sme_notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    document = relationship("Document", back_populates="recommended_controls")


class RepositoryControl(Base):
    __tablename__ = "repository_controls"

    id = Column(Integer, primary_key=True)
    protecht_id = Column(String)
    owning_workstream = Column(String)
    control_name = Column(String)
    control_description = Column(Text)
    control_owner = Column(String)
    control_performer = Column(String)
    control_type = Column(String)
    automation_type = Column(String)
    frequency = Column(String)
    impacted_products = Column(String)
    supporting_documentation = Column(Text)
    loaded_at = Column(DateTime, default=datetime.utcnow)

    matches = relationship("RepositoryMatch", back_populates="repository_control")


class RepositoryMatch(Base):
    __tablename__ = "repository_matches"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False)
    risk_statement = Column(Text)
    gap_tier = Column(Integer)
    gap_description = Column(Text)
    repository_control_id = Column(Integer, ForeignKey("repository_controls.id"), nullable=False)
    match_type = Column(String)          # "exact" or "partial"
    created_at = Column(DateTime, default=datetime.utcnow)

    document = relationship("Document", back_populates="repository_matches")
    repository_control = relationship("RepositoryControl", back_populates="matches")


def _migrate():
    """Add any columns missing from existing tables."""
    with engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info(controls)"))
        existing = {row[1] for row in result}
        missing_columns = {
            "repository_match_type": "VARCHAR",
            "repository_protecht_id": "VARCHAR",
            "risk_event": "TEXT",
            "control_performer": "VARCHAR",
        }
        for col, col_type in missing_columns.items():
            if col not in existing:
                conn.execute(text(f"ALTER TABLE controls ADD COLUMN {col} {col_type}"))
        conn.commit()


def init_db():
    Base.metadata.create_all(engine)
    _migrate()


def get_session():
    return Session()
