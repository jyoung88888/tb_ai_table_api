from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

# ============================================================
# 공통 스키마
# ============================================================

class AggregationRequest(BaseModel):
    """데이터 집계 요청 스키마"""
    target_date: str = Field(..., description="대상 날짜 (YYYY-MM-DD)", example="2024-01-15")

class AggregationResponse(BaseModel):
    """데이터 집계 응답 스키마"""
    success: bool = Field(..., description="성공 여부")
    inserted_count: int = Field(..., description="적재된 레코드 수")
    target_date: str = Field(..., description="처리된 날짜")
    message: str = Field(..., description="응답 메시지")

# ============================================================
# Solar Power 스키마
# ============================================================

class AISolarPowerBase(BaseModel):
    """TB AI Solar Power 기본 스키마"""
    ymdhms: datetime = Field(..., description="날짜시간")
    tmn: Optional[float] = Field(None, description="최저기온")
    tmx: Optional[float] = Field(None, description="최고기온")
    ics: Optional[float] = Field(None, description="일조량")
    pre_pwr_generation: Optional[float] = Field(None, description="예측 발전량")
    today_generation: Optional[float] = Field(None, description="당일 발전량")
    accum_generation: Optional[float] = Field(None, description="누적 발전량")

class AISolarPowerCreate(AISolarPowerBase):
    """TB AI Solar Power 생성 스키마"""
    pass

class AISolarPowerResponse(AISolarPowerBase):
    """TB AI Solar Power 응답 스키마"""
    reg_dt: datetime = Field(..., description="등록일자")

    class Config:
        from_attributes = True

# ============================================================
# ESS Charge 스키마
# ============================================================

class AIESSChargeBase(BaseModel):
    """TB AI ESS Charge Amount 기본 스키마"""
    ymdhms: datetime = Field(..., description="날짜시간")
    pre_pwr_generation: Optional[float] = Field(None, description="예측 발전량")
    today_generation: Optional[float] = Field(None, description="당일 발전량")
    pre_charge: Optional[float] = Field(None, description="예측 충전량")
    charge_amount: Optional[float] = Field(None, description="충전량")

class AIESSChargeCreate(AIESSChargeBase):
    """TB AI ESS Charge Amount 생성 스키마"""
    pass

class AIESSChargeResponse(AIESSChargeBase):
    """TB AI ESS Charge Amount 응답 스키마"""
    reg_dt: datetime = Field(..., description="등록일자")

    class Config:
        from_attributes = True

# ============================================================
# Power Usage 스키마 (미정)
# ============================================================

class AIPowerUsageBase(BaseModel):
    """TB AI Power Usage 기본 스키마 (미정)"""
    ymdhms: datetime = Field(..., description="날짜시간")
    # TODO: 필드가 확정되면 추가 예정

class AIPowerUsageCreate(AIPowerUsageBase):
    """TB AI Power Usage 생성 스키마 (미정)"""
    pass

class AIPowerUsageResponse(AIPowerUsageBase):
    """TB AI Power Usage 응답 스키마 (미정)"""
    reg_dt: datetime = Field(..., description="등록일자")

    class Config:
        from_attributes = True
