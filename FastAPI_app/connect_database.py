from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from models import Base  # Importing Base from models.py
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access environment variables
DATABASE_URL = os.getenv('DATABASE_URL')

engine = create_engine(DATABASE_URL)
Base.metadata.bind = engine

# Create a session factory, bound to the scope of the application
DBSession = scoped_session(sessionmaker(bind=engine))

# Test the connection
if __name__ == "__main__":
    try:
        # Attempt to create a session to test the connection
        session = DBSession()
        session.close()  # Close the session after successful connection
        print("Successful connection!")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
