import os
from pydantic import BaseModel, validator
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv("config.env")

class Config(BaseModel):
    API_ID: int
    API_HASH: str
    BOT_TOKEN: str
    SESSION_STRINGS: List[str]   # comma separated in .env
    
    MAX_CONCURRENT: int = 3
    BATCH_SIZE: int = 50
    FLOOD_SLEEP: int = 5
    
    # Premium / Trial
    TRIAL_DOWNLOADS: int = 50
    TRIAL_DAYS: int = 7
    
    # Forward (optional)
    FORWARD_CHAT_IDS: Optional[List[str]] = None
    
    # Razorpay (optional)
    RAZORPAY_KEY: Optional[str] = None
    RAZORPAY_SECRET: Optional[str] = None

    @validator('API_ID')
    def positive(cls, v):
        if v <= 0: raise ValueError('API_ID must be positive')
        return v

    @validator('SESSION_STRINGS', pre=True)
    def split_sessions(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(',') if s.strip()]
        return v

config = Config(
    API_ID=int(os.getenv("API_ID")),
    API_HASH=os.getenv("API_HASH"),
    BOT_TOKEN=os.getenv("BOT_TOKEN"),
    SESSION_STRINGS=os.getenv("SESSION_STRINGS", ""),
    MAX_CONCURRENT=int(os.getenv("MAX_CONCURRENT", "3")),
    BATCH_SIZE=int(os.getenv("BATCH_SIZE", "50")),
    FLOOD_SLEEP=int(os.getenv("FLOOD_SLEEP", "5")),
    TRIAL_DOWNLOADS=int(os.getenv("TRIAL_DOWNLOADS", "50")),
    TRIAL_DAYS=int(os.getenv("TRIAL_DAYS", "7")),
    FORWARD_CHAT_IDS=os.getenv("FORWARD_CHAT_IDS", "").split(',') if os.getenv("FORWARD_CHAT_IDS") else None,
    RAZORPAY_KEY=os.getenv("RAZORPAY_KEY"),
    RAZORPAY_SECRET=os.getenv("RAZORPAY_SECRET"),
          )
