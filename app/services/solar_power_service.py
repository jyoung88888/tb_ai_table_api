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
        - ics: SUM (합)

        Args:
            target_date: 대상 날짜 (YYYY-MM-DD)

        Returns:
            Dict: 결과 정보 (success, inserted_count, target_date, message)
        """
        logger.info(f"📊 [Solar Power] 데이터 집계 및 적재 시작 - {target_date}")

        try:
            # 날짜 범위 계산 (인덱스 활용을 위해 범위 조건 사용)
            # target_date (YYYY-MM-DD) 기준으로 하루 범위 설정
            params = [target_date, target_date, target_date, target_date, target_date]

            logger.info(f"📅 [Solar Power] 대상 날짜: {target_date}")

            # 집계 쿼리 작성 (날짜별로 하나의 레코드로 집계)
            # 각 테이블을 먼저 집계한 뒤, 집계 결과끼리 1:1로 조인하여 N×M 행 생성 방지
            # pre_pwr_generation, today_generation, accum_generation: SUM
            # tmx: MAX
            # tmn: MIN (0이 아닌 값 중)
            # ics: SUM
            # 인덱스 활용을 위해 DATE() 함수 대신 범위 조건 사용
            query = f"""
            INSERT INTO {self.ai_solar_power_table}
                (ymdhms, tmn, tmx, ics, pre_pwr_generation, today_generation, accum_generation)
            SELECT * FROM (
                SELECT
                    %s as ymdhms,
                    wi_agg.tmn,
                    wi_agg.tmx,
                    wi_agg.ics,
                    sd_agg.pre_pwr_generation,
                    sd_agg.today_generation,
                    sd_agg.accum_generation
                FROM
                    (
                        SELECT
                            SUM(forecast_quantity) as pre_pwr_generation,
                            SUM(today_generation) as today_generation,
                            SUM(accum_generation) as accum_generation
                        FROM {self.solar_day_table}
                        WHERE ymdhms >= %s AND ymdhms < DATE_ADD(%s, INTERVAL 1 DAY)
                    ) sd_agg
                CROSS JOIN
                    (
                        SELECT
                            MIN(CASE WHEN tmn > 0 THEN tmn ELSE NULL END) as tmn,
                            MAX(tmx) as tmx,
                            SUM(ics) as ics
                        FROM {self.weather_info_table}
                        WHERE tm >= %s AND tm < DATE_ADD(%s, INTERVAL 1 DAY)
                    ) wi_agg
            ) AS new_data
            ON DUPLICATE KEY UPDATE
                tmn = new_data.tmn,
                tmx = new_data.tmx,
                ics = new_data.ics,
                pre_pwr_generation = new_data.pre_pwr_generation,
                today_generation = new_data.today_generation,
                accum_generation = new_data.accum_generation
            """

            logger.info(f"🔍 [Solar Power] 파라미터: {params}")

            async with self.db.get_async_connection() as connection:
                def _execute():
                    cursor = connection.cursor()
                    try:
                        cursor.execute(query, params)
                        connection.commit()
                        affected_rows = cursor.rowcount
                        # ON DUPLICATE KEY UPDATE의 rowcount:
                        # 1 = 새로운 행 삽입
                        # 2 = 기존 행 업데이트
                        # 0 = 업데이트했지만 값 변화 없음
                        logger.info(f"✅ [Solar Power] rowcount: {affected_rows} (1=INSERT, 2=UPDATE, 0=변화없음)")
                        return affected_rows
                    finally:
                        cursor.close()

                affected_rows = await asyncio.get_event_loop().run_in_executor(None, _execute)

            logger.info(f"✅ [Solar Power] 데이터 집계 및 적재 완료 (영향받은 행: {affected_rows})")

            return {
                "success": True,
                "affected_rows": affected_rows,
                "target_date": target_date,
                "message": f"{target_date} 날짜의 Solar Power 데이터 UPSERT 완료 (영향받은 행: {affected_rows})"
            }

        except Exception as e:
            logger.error(f"❌ [Solar Power] 데이터 집계 및 적재 실패: {str(e)}")
            return {
                "success": False,
                "affected_rows": 0,
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
