from fastapi import APIRouter

router = APIRouter()


@router.get("/trading/health")
def trading_health():
    return {
        "ok": True,
        "message": "Trading route module loaded",
    }
