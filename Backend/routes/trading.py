from fastapi import APIRouter

from fundamentals.ingestion import start_fundamental_ingestion_scheduler
from routes.fundamentals import router as fundamentals_router
from routes.deriv import router as deriv_router
from routes.user_auth import router as user_auth_router
from routes.user_deriv import router as user_deriv_router

router = APIRouter()
router.include_router(fundamentals_router)
router.include_router(deriv_router)
router.include_router(user_auth_router)
router.include_router(user_deriv_router)


@router.on_event("startup")
def start_fundamental_collection():
    result = start_fundamental_ingestion_scheduler()
    print("FUNDAMENTAL_SCHEDULER_START =", result)
    try:
        import api
        from services.customer_forex_guard import install_owner_forex_mutation_guard
        guard = install_owner_forex_mutation_guard(api.app, api.SESSIONS)
        print("CUSTOMER_FOREX_MUTATION_GUARD =", guard)
    except Exception as exc:
        print("CUSTOMER_FOREX_MUTATION_GUARD_ERROR =", str(exc))
        raise


@router.get("/trading/health")
def trading_health():
    return {
        "ok": True,
        "message": "Trading route module loaded",
    }
