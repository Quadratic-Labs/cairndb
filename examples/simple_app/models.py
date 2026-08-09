"""Application-defined SQLAlchemy ORM models.

CairnDB is schema-agnostic.  The application owns its tables.  These models
are used by both the event handlers (for SQL generation) and the client
(for query-time ORM access).
"""

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    email: Mapped[str]


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[str] = mapped_column(primary_key=True)
    author_id: Mapped[str]
    title: Mapped[str]
    body: Mapped[str]
