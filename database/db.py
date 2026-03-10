from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, ForeignKey
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

    recipe = relationship("Recipe")
    controls = relationship("Control", back_populates="document")


class Control(Base):
    __tablename__ = "controls"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False)
    control_id = Column(String)          # e.g. CTRL-001
    control_type = Column(String)        # e.g. preventive, detective
    title = Column(String)
    description = Column(Text)
    risk_mitigated = Column(Text)        # the risk this control addresses
    control_owner = Column(String)       # team or role responsible
    frequency = Column(String)           # how often the control operates
    page_number = Column(Integer)
    section_heading = Column(String)
    source_excerpt = Column(Text)        # exact quoted text from document
    expected_evidence = Column(Text)     # what evidence looks like for this type
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


def init_db():
    Base.metadata.create_all(engine)


def get_session():
    return Session()
