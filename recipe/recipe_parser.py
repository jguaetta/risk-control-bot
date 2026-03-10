import io
from docx import Document as DocxDocument
from database.db import get_session, Recipe


def parse_recipe_docx(file_bytes: bytes) -> str:
    """Extract full text content from the recipe DOCX."""
    doc = DocxDocument(io.BytesIO(file_bytes))
    sections = []
    for para in doc.paragraphs:
        text = para.text.strip()
        if text:
            sections.append(text)
    return "\n".join(sections)


def save_recipe(name: str, file_bytes: bytes, uploaded_by: str) -> Recipe:
    """Parse and save a recipe to the database. Returns the saved Recipe."""
    content = parse_recipe_docx(file_bytes)
    session = get_session()
    try:
        recipe = Recipe(name=name, content=content, uploaded_by=uploaded_by)
        session.add(recipe)
        session.commit()
        session.refresh(recipe)
        return recipe
    finally:
        session.close()


def get_latest_recipe() -> Recipe | None:
    """Return the most recently uploaded recipe."""
    session = get_session()
    try:
        return session.query(Recipe).order_by(Recipe.uploaded_at.desc()).first()
    finally:
        session.close()


def get_recipe_by_id(recipe_id: int) -> Recipe | None:
    session = get_session()
    try:
        return session.query(Recipe).filter(Recipe.id == recipe_id).first()
    finally:
        session.close()
