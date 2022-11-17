from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column,  String, Numeric, TIMESTAMP
from sqlalchemy.ext.declarative import as_declarative

from textblob import TextBlob

engine = create_engine('postgresql://postgres:password@localhost/db', pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db = SessionLocal()


@as_declarative()
class Base():
    id = Column(String, primary_key=True, index=True)

class Comment(Base):
    __tablename__ = "comment"

    text = Column(String)
    sentiment = Column(Numeric)
    timestamp = Column(TIMESTAMP)


if __name__ == "__main__":
    comments = db.query(Comment).order_by(Comment.timestamp).all()
    for comment in comments:
        comment.sentiment = round(TextBlob(comment.text).sentiment[0], 3)
    # db.commit()