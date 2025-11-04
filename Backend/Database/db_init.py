from sqlalchemy import create_engine
import os

DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', 3306)
DB_NAME = os.environ.get('DB_NAME', 'dukbill')

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    echo=False,
    pool_pre_ping=True
)

from sqlalchemy.orm import DeclarativeBase
from typing import List
from typing import Optional
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey
from sqlalchemy import String

class Base(DeclarativeBase):
    pass

class Users(Base):
    __tablename__ = "users"
    user_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    auth0_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    basiq_id: Mapped[Optional[str]] = mapped_column(String(255))
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
    broker_id: Mapped[str] = mapped_column(ForeignKey("brokers.broker_id"), nullable=False)
    broker_verify: Mapped[bool] = mapped_column(nullable=False, default=False)
    brokerAccess: Mapped[bool] = mapped_column(nullable=False, default=False)

class Emails(Base):
    __tablename__ = "emails"
    email_id: Mapped[int] = mapped_column(autoincrement=True, primary_key=True)
    client_id: Mapped[str] = mapped_column(ForeignKey("clients.client_id"), nullable=False)
    domain: Mapped[Optional[str]] = mapped_column(String(255))
    email_address: Mapped[Optional[str]] = mapped_column(String(255))

def initialize_database():
    """Create all tables if they don't exist"""
    #Base.metadata.drop_all(engine)
    #print("All tables dropped successfully")
    Base.metadata.create_all(engine)
    print("Database tables created/verified successfully")
