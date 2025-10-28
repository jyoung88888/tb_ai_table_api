"""
통합 데이터 집계 API 엔드포인트
하나의 날짜 입력으로 Solar Power, ESS Charge, Power Usage 모두 처리
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Dict
import logging

from app.models.schemas import AggregationRequest, AggregationResponse
from app.services.solar_power_service import get_solar_power_service, SolarPowerService
from app.services.ess_charge_service import get_ess_charge_service, ESSChargeService
from app.services.power_usage_service import get_power_usage_service, PowerUsageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/aggregate", tags=["Data Aggregation"])

@router.post("/all", response_model=Dict[str, AggregationResponse])
async def aggregate_all_data(
    request: AggregationRequest,
    solar_service: SolarPowerService = Depends(get_solar_power_service),
    ess_service: ESSChargeService = Depends(get_ess_charge_service),
    power_service: PowerUsageService = Depends(get_power_usage_service)
):
    """
    하나의 날짜 입력으로 Solar Power, ESS Charge, Power Usage 모두 집계 및 적재

    - **target_date**: 대상 날짜 (YYYY-MM-DD) - 필수

    **예시**: `{"target_date": "2024-01-15"}`

    **응답**: 각 서비스별 처리 결과를 반환
    """
    try:
        logger.info(f"📊 [통합 집계] 모든 데이터 집계 시작 - {request.target_date}")

        results = {}

        # Solar Power 집계
        try:
            solar_result = await solar_service.aggregate_and_insert(
                target_date=request.target_date
            )
            results["solar_power"] = AggregationResponse(**solar_result)
            logger.info(f"✅ [Solar Power] 완료: {solar_result['inserted_count']}건")
        except Exception as e:
            logger.error(f"❌ [Solar Power] 실패: {str(e)}")
            results["solar_power"] = AggregationResponse(
                success=False,
                inserted_count=0,
                target_date=request.target_date,
                message=f"Solar Power 집계 실패: {str(e)}"
            )

        # ESS Charge 집계
        try:
            ess_result = await ess_service.aggregate_and_insert(
                target_date=request.target_date
            )
            results["ess_charge"] = AggregationResponse(**ess_result)
            logger.info(f"✅ [ESS Charge] 완료: {ess_result['inserted_count']}건")
        except Exception as e:
            logger.error(f"❌ [ESS Charge] 실패: {str(e)}")
            results["ess_charge"] = AggregationResponse(
                success=False,
                inserted_count=0,
                target_date=request.target_date,
                message=f"ESS Charge 집계 실패: {str(e)}"
            )

        # Power Usage 집계
        try:
            power_result = await power_service.aggregate_and_insert(
                target_date=request.target_date
            )
            results["power_usage"] = AggregationResponse(**power_result)
            logger.info(f"✅ [Power Usage] 완료: {power_result['inserted_count']}건")
        except Exception as e:
            logger.error(f"❌ [Power Usage] 실패: {str(e)}")
            results["power_usage"] = AggregationResponse(
                success=False,
                inserted_count=0,
                target_date=request.target_date,
                message=f"Power Usage 집계 실패: {str(e)}"
            )

        logger.info(f"📊 [통합 집계] 완료 - {request.target_date}")
        return results

    except Exception as e:
        logger.error(f"❌ [통합 집계] API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")
