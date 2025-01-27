from sqlalchemy import create_engine, Column, Integer, String, BLOB, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
from app.utils.logger import logger
from uuid import uuid4


load_dotenv(dotenv_path='../../pg-docker/.env')

DB_URL = (f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
          f"@localhost:5432/{os.getenv('POSTGRES_DB')}")
engine = create_engine(DB_URL, echo=True)
Base = declarative_base()


class User(Base):
    __tablename__ = 'user'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    username = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    api_key_encrypted = Column(BLOB, nullable=False)
    created_at = Column(TIMESTAMP, default='CURRENT_TIMESTAMP')


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
            print(f"Error updating password: {e}")
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
                print(f"User {user.username} deleted.")
            else:
                print("User not found.")
        except Exception as e:
            print(f"Error deleting user: {e}")
            self.session.rollback()

