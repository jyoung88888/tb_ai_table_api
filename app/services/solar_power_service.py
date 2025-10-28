"""
태양광 발전 데이터 집계 서비스
tb_ai_solar_power 테이블에 데이터 적재
"""
import asyncio
import logging
import pymysql.cursors
from typing import Dict, Any, List
from app.core.config import settings

logger = logging.getLogger(__name__)

class SolarPowerService:
    """태양광 발전 데이터 집계 및 적재 클래스"""

    def __init__(self, db_manager):
        """
        Args:
            db_manager: DatabaseManager 인스턴스
        """
        self.db = db_manager
        self.solar_day_table = settings.table_names.get('solar_day', 'tb_solar_day')
        self.weather_info_table = settings.table_names.get('weather_info', 'tb_weather_info')
        self.ai_solar_power_table = settings.table_names.get('ai_solar_power', 'tb_ai_solar_power')

    async def aggregate_and_insert(self, target_date: str) -> Dict[str, Any]:
        """
        tb_solar_day와 tb_weather_info의 데이터를 조합하여 tb_ai_solar_power에 적재
        지정된 날짜(YYYY-MM-DD) 하루분의 데이터를 집계하여 하나의 레코드로 적재

        집계 방법:
        - pre_pwr_generation, today_generation, accum_generation: SUM
        - tmx: MAX
        - tmn: MIN (0이 아닌 값 중)
        - ics: AVG (평균)

        Args:
            target_date: 대상 날짜 (YYYY-MM-DD)

        Returns:
            Dict: 결과 정보 (success, inserted_count, target_date, message)
        """
        logger.info(f"📊 [Solar Power] 데이터 집계 및 적재 시작 - {target_date}")

        try:
            params = [target_date, target_date]

            logger.info(f"📅 [Solar Power] 대상 날짜: {target_date}")

            # 집계 쿼리 작성 (날짜별로 하나의 레코드로 집계)
            # pre_pwr_generation, today_generation, accum_generation: SUM
            # tmx: MAX
            # tmn: MIN (0이 아닌 값 중)
            # ics: AVG
            query = f"""
            REPLACE INTO {self.ai_solar_power_table}
                (ymdhms, tmn, tmx, ics, pre_pwr_generation, today_generation, accum_generation)
            SELECT
                %s as ymdhms,
                MIN(CASE WHEN wi.tmn > 0 THEN wi.tmn ELSE NULL END) as tmn,
                MAX(wi.tmx) as tmx,
                AVG(wi.ics) as ics,
                SUM(sd.forecast_quantity) as pre_pwr_generation,
                SUM(sd.today_generation) as today_generation,
                SUM(sd.accum_generation) as accum_generation

            FROM {self.solar_day_table} sd
            INNER JOIN {self.weather_info_table} wi
                ON sd.ymdhms = wi.tm
            WHERE DATE(sd.ymdhms) = %s
            """

            logger.info(f"🔍 [Solar Power] 파라미터: {params}")

            async with self.db.get_async_connection() as connection:
                def _execute():
                    cursor = connection.cursor()
                    try:
                        cursor.execute(query, params)
                        connection.commit()
                        affected_rows = cursor.rowcount
                        logger.info(f"✅ [Solar Power] 영향받은 행 수: {affected_rows}")
                        return affected_rows
                    finally:
                        cursor.close()

                inserted_count = await asyncio.get_event_loop().run_in_executor(None, _execute)

            logger.info(f"✅ [Solar Power] 데이터 집계 및 적재 완료: {inserted_count}건")

            return {
                "success": True,
                "inserted_count": inserted_count,
                "target_date": target_date,
                "message": f"{target_date} 날짜의 데이터를 집계하여 {inserted_count}건의 Solar Power 데이터를 적재했습니다."
            }

        except Exception as e:
            logger.error(f"❌ [Solar Power] 데이터 집계 및 적재 실패: {str(e)}")
            return {
                "success": False,
                "inserted_count": 0,
                "target_date": target_date,
                "message": f"Solar Power 데이터 적재 중 오류 발생: {str(e)}"
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
            ymdhms, tmn, tmx, ics,
            pre_pwr_generation, today_generation, accum_generation
        FROM {self.ai_solar_power_table}
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
                logger.info(f"📊 [Solar Power] 최근 {len(results)}건의 데이터 조회 완료")
                return results

        except Exception as e:
            logger.error(f"❌ [Solar Power] 데이터 조회 실패: {str(e)}")
            return []

# 전역 인스턴스
_solar_power_service = None

async def get_solar_power_service():
    """Solar Power Service 의존성 주입"""
    global _solar_power_service
    if _solar_power_service is None:
        from app.core.database import db_manager
        _solar_power_service = SolarPowerService(db_manager)
    return _solar_power_service
