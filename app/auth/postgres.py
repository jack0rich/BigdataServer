from sqlalchemy import create_engine, Column, Integer, String, BLOB, TIMESTAMP
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker
import os
import utils as u
from datetime import datetime, timezone
from dotenv import load_dotenv
from app.auth.utils import encrypt_key
from app.utils.logger import logger
from uuid import uuid4


load_dotenv(dotenv_path='../../pg-docker/.env')

DB_URL = (f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
          f"@localhost:5432/{os.getenv('POSTGRES_DB')}?sslmode=disable")
engine = create_engine(DB_URL, echo=True)
Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    username = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    api_key_encrypted = Column(BLOB, nullable=False)
    created_at = Column(TIMESTAMP, default=datetime.now(timezone.utc))  


Session = sessionmaker(bind=engine)


class Database:
    def __init__(self):
        self.session = Session()

    def insert_user(self, username, password_hash, api_key_encrypted) -> bool:
        try:
            user = User(username=username, password_hash=password_hash,
                         api_key_encrypted=api_key_encrypted)
            self.session.add(user)
            self.session.commit()
            logger.info(f"User {username} inserted successfully.")
            return True
        except Exception as e:
            logger.error(f"Error inserting user: {e}")
            self.session.rollback()
            return False

    def fetch_user_by_name(self, user_name):
        try:
            user = self.session.query(User).filter(User.username == user_name).first()
            return user
        except Exception as e:
            logger.error(f"Error fetching user by ID: {e}")
            return None
        finally:
            self.session.close()

    def update_user_password(self, username, new_password_hash) -> bool:
        """
        更新用户密码
        """
        try:
            user = self.session.query(User).filter(User.username == username).first()
            if user:
                user.password_hash = new_password_hash
                self.session.commit()
                logger.info(f"Password updated for user {user.username}.")
                return True
            else:
                logger.warning("User not found.")
                return False
        except Exception as e:
            logger.error(f"Error updating password: {e}")
            self.session.rollback()

    def delete_user(self, username) -> bool:
        """
        删除用户
        """
        try:
            user = self.session.query(User).filter(User.username == username).first()
            if user:
                self.session.delete(user)
                self.session.commit()
                logger.info(f"User {user.username} deleted.")
                return True
            else:
                logger.warning("User not found.")
                return False
        except Exception as e:
            logger.error(f"Error deleting user: {e}")
            self.session.rollback()
            return False



def creat_user(username: str, password: str) -> bool:
    hashed_pw = u.hash_password(password)
    encrypt_api_key = u.encrypt_key(u.generate_secure_api_key())
    db = Database()
    return db.insert_user(
        username=username,
        password_hash=hashed_pw,
        api_key_encrypted=encrypt_api_key
    )


def update_user(username: str, password: str) -> bool:
    hashed_pw = u.hash_password(password)
    db = Database()
    return  db.update_user_password(
        username=username,
        new_password_hash=hashed_pw
    )


def delete_user(username: str) -> bool:
    db = Database()
    return db.delete_user(
        username=username
    )


def fetch_user_by_name(username: str):
    db = Database()
    return db.fetch_user_by_name(
        user_name=username
    )


if __name__ == '__main__':
    update_user('jack', '9999')


