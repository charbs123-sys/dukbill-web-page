from typing import Optional

from config import DB_CONFIG
from sqlalchemy import ForeignKey, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

DB_URL = (
    f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Create SQLAlchemy engine
engine = create_engine(DB_URL, echo=False, pool_pre_ping=True)


class Base(DeclarativeBase):
    pass


class Users(Base):
    __tablename__ = "users"
    user_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    auth0_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    # basiq_id: Mapped[Optional[str]] = mapped_column(String(255))
    name: Mapped[Optional[str]] = mapped_column(String(255))
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    phone: Mapped[Optional[str]] = mapped_column(String(20))
    company: Mapped[Optional[str]] = mapped_column(String(255))
    picture: Mapped[Optional[str]] = mapped_column(String(255))
    isBroker: Mapped[bool] = mapped_column(nullable=False, default=False)
    profile_complete: Mapped[bool] = mapped_column(nullable=False, default=False)


class Brokers(Base):
    __tablename__ = "brokers"
    broker_id: Mapped[str] = mapped_column(String(6), primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.user_id"), nullable=False)


class Clients(Base):
    __tablename__ = "clients"
    client_id: Mapped[str] = mapped_column(String(6), primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.user_id"), nullable=False)


class ClientBroker(Base):
    __tablename__ = "client_broker"
    client_broker_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    client_id: Mapped[str] = mapped_column(
        String(6), ForeignKey("clients.client_id"), nullable=False
    )
    broker_id: Mapped[str] = mapped_column(
        String(6), ForeignKey("brokers.broker_id"), nullable=False
    )
    broker_verify: Mapped[bool] = mapped_column(nullable=False, default=False)
    brokerAccess: Mapped[bool] = mapped_column(nullable=False, default=False)


class Emails(Base):
    __tablename__ = "emails"
    email_id: Mapped[int] = mapped_column(autoincrement=True, primary_key=True)
    client_id: Mapped[str] = mapped_column(
        ForeignKey("clients.client_id"), nullable=False
    )
    domain: Mapped[Optional[str]] = mapped_column(String(255))
    email_address: Mapped[Optional[str]] = mapped_column(String(255))


class IDMERITVerification(Base):
    __tablename__ = "idmerit_verification"
    idmerit_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    client_id: Mapped[int] = mapped_column(
        ForeignKey("clients.client_id"), primary_key=True
    )
    unique_uuid: Mapped[Optional[str]] = mapped_column(String(255))


def initialize_database():
    """Create all tables if they don't exist"""
    # Base.metadata.drop_all(engine)
    # print("deleted DB")
    Base.metadata.create_all(engine)
    print("SQL Database initialized.")
