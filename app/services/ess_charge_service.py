"""
ESS 충전량 데이터 집계 서비스
tb_ai_ess_charge_amt 테이블에 데이터 적재
"""
import asyncio
import logging
import pymysql.cursors
from typing import Dict, Any, List
from app.core.config import settings

logger = logging.getLogger(__name__)

class ESSChargeService:
    """ESS 충전량 데이터 집계 및 적재 클래스"""

    def __init__(self, db_manager):
        """
        Args:
            db_manager: DatabaseManager 인스턴스
        """
        self.db = db_manager
        self.solar_day_table = settings.table_names.get('solar_day', 'tb_solar_day')
        self.bms_daily_stat_table = settings.table_names.get('bms_daily_stat', 'tb_nrt_bms_daily_stat')
        self.ai_ess_charge_table = settings.table_names.get('ai_ess_charge_amt', 'tb_ai_ess_charge_amt')

    async def aggregate_and_insert(self, target_date: str) -> Dict[str, Any]:
        """
        tb_solar_day와 tb_nrt_bms_daily_stat의 데이터를 조합하여 tb_ai_ess_charge_amt에 적재
        지정된 날짜(YYYY-MM-DD) 하루분의 데이터를 집계하여 하나의 레코드로 적재

        집계 방법:
        - pre_pwr_generation, today_generation, pre_charge, charge_amount: SUM

        Args:
            target_date: 대상 날짜 (YYYY-MM-DD)

        Returns:
            Dict: 결과 정보 (success, inserted_count, target_date, message)
        """
        logger.info(f"📊 [ESS Charge] 데이터 집계 및 적재 시작 - {target_date}")

        try:
            params = [target_date, target_date]

            logger.info(f"📅 [ESS Charge] 대상 날짜: {target_date}")

            # 집계 쿼리 작성 (날짜별로 하나의 레코드로 집계)
            # 모든 필드: SUM
            query = f"""
            REPLACE INTO {self.ai_ess_charge_table}
                (ymdhms, pre_pwr_generation, today_generation, pre_charge, charge_amount)
            SELECT
                %s as ymdhms,
                SUM(sd.forecast_quantity) as pre_pwr_generation,
                SUM(sd.today_generation) as today_generation,
                CASE
                    WHEN REPLACE(TRIM(bms.forecast_quantity), ',', '') REGEXP '^-?[0-9]*\\.?[0-9]+$'
                    THEN CAST(REPLACE(TRIM(bms.forecast_quantity), ',', '') AS DECIMAL(20,2))
                    ELSE 0
                END AS pre_charge,
                CASE
                    WHEN REPLACE(TRIM(bms.D_BAT_SOC), ',', '') REGEXP '^-?[0-9]*\\.?[0-9]+$'
                    THEN CAST(REPLACE(TRIM(bms.D_BAT_SOC), ',', '') AS DECIMAL(20,2))
                    ELSE 0
                END AS charge_amount
            FROM {self.solar_day_table} sd
            INNER JOIN {self.bms_daily_stat_table} bms
                ON DATE(sd.ymdhms) = DATE(bms.T_CREATE_DT)
            WHERE DATE(sd.ymdhms) = %s
            """

            logger.info(f"🔍 [ESS Charge] 파라미터: {params}")

            async with self.db.get_async_connection() as connection:
                def _execute():
                    cursor = connection.cursor()
                    try:
                        cursor.execute(query, params)
                        connection.commit()
                        affected_rows = cursor.rowcount
                        logger.info(f"✅ [ESS Charge] 영향받은 행 수: {affected_rows}")
                        return affected_rows
                    finally:
                        cursor.close()

                inserted_count = await asyncio.get_event_loop().run_in_executor(None, _execute)

            logger.info(f"✅ [ESS Charge] 데이터 집계 및 적재 완료: {inserted_count}건")

            return {
                "success": True,
                "inserted_count": inserted_count,
                "target_date": target_date,
                "message": f"{target_date} 날짜의 데이터를 집계하여 {inserted_count}건의 ESS Charge 데이터를 적재했습니다."
            }

        except Exception as e:
            logger.error(f"❌ [ESS Charge] 데이터 집계 및 적재 실패: {str(e)}")
            return {
                "success": False,
                "inserted_count": 0,
                "target_date": target_date,
                "message": f"ESS Charge 데이터 적재 중 오류 발생: {str(e)}"
            }

    async def verify_data(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        적재된 데이터 확인

        Args:
            limit: 조회할 레코드 수

        Returns:
            List[Dict]: 적재된 데이터 리스트
        """
        query = f"""
        SELECT
            ymdhms, pre_pwr_generation, today_generation,
            pre_charge, charge_amount
        FROM {self.ai_ess_charge_table}
        ORDER BY ymdhms DESC
        LIMIT %s
        """

        try:
            async with self.db.get_async_connection() as connection:
                def _fetch():
                    cursor = connection.cursor(pymysql.cursors.DictCursor)
                    try:
                        cursor.execute(query, (limit,))
                        results = cursor.fetchall()
                        return results
                    finally:
                        cursor.close()

                results = await asyncio.get_event_loop().run_in_executor(None, _fetch)
                logger.info(f"📊 [ESS Charge] 최근 {len(results)}건의 데이터 조회 완료")
                return results

        except Exception as e:
            logger.error(f"❌ [ESS Charge] 데이터 조회 실패: {str(e)}")
            return []

# 전역 인스턴스
_ess_charge_service = None

async def get_ess_charge_service():
    """ESS Charge Service 의존성 주입"""
    global _ess_charge_service
    if _ess_charge_service is None:
        from app.core.database import db_manager
        _ess_charge_service = ESSChargeService(db_manager)
    return _ess_charge_service
