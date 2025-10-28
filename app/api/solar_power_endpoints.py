"""
Solar Power 관련 API 엔드포인트
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List
import logging

from app.models.schemas import AggregationRequest, AggregationResponse
from app.services.solar_power_service import get_solar_power_service, SolarPowerService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/solar-power", tags=["Solar Power"])

@router.post("/aggregate", response_model=AggregationResponse)
async def aggregate_solar_power_data(
    request: AggregationRequest,
    service: SolarPowerService = Depends(get_solar_power_service)
):
    """
    tb_solar_day와 tb_weather_info의 데이터를 조합하여 tb_ai_solar_power에 적재
    지정된 날짜(YYYY-MM-DD) 하루분의 데이터만 처리

    - **target_date**: 대상 날짜 (YYYY-MM-DD) - 필수

    **예시**: `{"target_date": "2024-01-15"}`
    """
    try:
        logger.info(f"📊 [Solar Power] 데이터 집계 API 호출 - {request.target_date}")

        result = await service.aggregate_and_insert(
            target_date=request.target_date
        )

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])

        return AggregationResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Solar Power] API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")

@router.get("/verify", response_model=List[dict])
async def verify_solar_power_data(
    limit: int = 10,
    service: SolarPowerService = Depends(get_solar_power_service)
):
    """
    적재된 Solar Power 데이터 확인 (최근 N건)

    - **limit**: 조회할 레코드 수 (기본값: 10)
    """
    try:
        logger.info(f"📊 [Solar Power] 데이터 조회 API 호출 (limit={limit})")

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
        logger.error(f"❌ [Solar Power] API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")
