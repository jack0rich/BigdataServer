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

    def insert_user(self, username, password_hash, api_key_encrypted):
        try:
            user = User(username=username, password_hash=password_hash,
                         api_key_encrypted=api_key_encrypted)
            self.session.add(user)
            self.session.commit()
            logger.info(f"User {username} inserted successfully.")
        except Exception as e:
            logger.error(f"Error inserting user: {e}")
            self.session.rollback()

    def fetch_user_by_id(self, user_id):
        try:
            user = self.session.query(User).filter(User.id == user_id).first()
            return user
        except Exception as e:
            logger.error(f"Error fetching user by ID: {e}")
            return None
        finally:
            self.session.close()

    def update_user_password(self, user_id, new_password_hash) -> bool:
        """
        更新用户密码
        """
        try:
            user = self.session.query(User).filter(User.id == user_id).first()
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

    def delete_user(self, user_id):
        """
        删除用户
        """
        try:
            user = self.session.query(User).filter(User.id == user_id).first()
            if user:
                self.session.delete(user)
                self.session.commit()
                logger.info(f"User {user.username} deleted.")
            else:
                logger.warning("User not found.")
        except Exception as e:
            logger.error(f"Error deleting user: {e}")
            self.session.rollback()



def creat_user(username: str, password: str):
    hashed_pw = u.hash_password(password)
    encrypt_api_key = u.encrypt_key(u.generate_secure_api_key())
    db = Database()
    db.insert_user(
        username=username,
        password_hash=hashed_pw,
        api_key_encrypted=encrypt_api_key
    )



if __name__ == '__main__':
    creat_user('jack', '0907')

