"""
Power Usage 관련 API 엔드포인트 (미정)
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List
import logging

from app.models.schemas import AggregationRequest, AggregationResponse
from app.services.power_usage_service import get_power_usage_service, PowerUsageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/power-usage", tags=["Power Usage"])

@router.post("/aggregate", response_model=AggregationResponse)
async def aggregate_power_usage_data(
    request: AggregationRequest,
    service: PowerUsageService = Depends(get_power_usage_service)
):
    """
    전력 사용량 데이터를 tb_ai_pwr_usage에 적재 (미정)
    지정된 날짜(YYYY-MM-DD) 하루분의 데이터만 처리

    - **target_date**: 대상 날짜 (YYYY-MM-DD) - 필수

    **주의**: 데이터 매핑이 아직 확정되지 않았습니다.

    **예시**: `{"target_date": "2024-01-15"}`
    """
    try:
        logger.info(f"📊 [Power Usage] 데이터 집계 API 호출 (미정) - {request.target_date}")

        result = await service.aggregate_and_insert(
            target_date=request.target_date
        )

        if not result["success"]:
            raise HTTPException(status_code=501, detail=result["message"])

        return AggregationResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Power Usage] API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")

@router.get("/verify", response_model=List[dict])
async def verify_power_usage_data(
    limit: int = 10,
    service: PowerUsageService = Depends(get_power_usage_service)
):
    """
    적재된 Power Usage 데이터 확인 (최근 N건) - 미정

    - **limit**: 조회할 레코드 수 (기본값: 10)

    **주의**: 데이터 매핑이 아직 확정되지 않았습니다.
    """
    try:
        logger.info(f"📊 [Power Usage] 데이터 조회 API 호출 (limit={limit}) - 미정")

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
        logger.error(f"❌ [Power Usage] API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")
