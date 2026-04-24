from brain import get_panel_data

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from db import Base, engine, get_db, SessionLocal
from models import User
from auth import hash_password, verify_password, create_access_token

app = FastAPI(title="FlowSignal API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # later we lock this down
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)


@app.get("/")
def root():
    return {"message": "FlowSignal backend is running"}


@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == form_data.username).first()

    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is disabled"
        )

    token = create_access_token({"sub": user.email})
    return {"access_token": token, "token_type": "bearer"}


@app.get("/panel-data")
def panel_data():
    return get_panel_data()


@app.post("/create-first-user")
def create_first_user(email: str, password: str):
    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.email == email).first()
        if existing:
            raise HTTPException(status_code=400, detail="User already exists")

        user = User(
            email=email,
            hashed_password=hash_password(password),
            is_active=True
        )
        db.add(user)
        db.commit()
        db.refresh(user)

        return {"message": "First user created", "email": user.email}
    finally:
        db.close()
