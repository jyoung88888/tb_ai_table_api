"""
ESS Charge 관련 API 엔드포인트
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List
import logging

from app.models.schemas import AggregationRequest, AggregationResponse
from app.services.ess_charge_service import get_ess_charge_service, ESSChargeService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ess-charge", tags=["ESS Charge"])

@router.post("/aggregate", response_model=AggregationResponse)
async def aggregate_ess_charge_data(
    request: AggregationRequest,
    service: ESSChargeService = Depends(get_ess_charge_service)
):
    """
    tb_solar_day와 tb_nrt_bms_daily_stat의 데이터를 조합하여 tb_ai_ess_charge_amt에 적재
    지정된 날짜(YYYY-MM-DD) 하루분의 데이터만 처리

    - **target_date**: 대상 날짜 (YYYY-MM-DD) - 필수

    **예시**: `{"target_date": "2024-01-15"}`
    """
    try:
        logger.info(f"📊 [ESS Charge] 데이터 집계 API 호출 - {request.target_date}")

        result = await service.aggregate_and_insert(
            target_date=request.target_date
        )

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])

        return AggregationResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [ESS Charge] API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")

@router.get("/verify", response_model=List[dict])
async def verify_ess_charge_data(
    limit: int = 10,
    service: ESSChargeService = Depends(get_ess_charge_service)
):
    """
    적재된 ESS Charge 데이터 확인 (최근 N건)

    - **limit**: 조회할 레코드 수 (기본값: 10)
    """
    try:
        logger.info(f"📊 [ESS Charge] 데이터 조회 API 호출 (limit={limit})")

        results = await service.verify_data(limit=limit)

        # datetime 객체를 문자열로 변환
        formatted_results = []
        for row in results:
            formatted_row = {}
            for key, value in row.items():
                if hasattr(value, 'strftime'):
                    formatted_row[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_row[key] = value
            formatted_results.append(formatted_row)

        return formatted_results

    except Exception as e:
        logger.error(f"❌ [ESS Charge] API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")
