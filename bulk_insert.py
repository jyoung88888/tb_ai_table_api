import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

def insert_date(date_str):
    print(f"Processing: {date_str}")
    response = requests.post(
        "http://localhost:9758/api/v1/solar-power/aggregate",
        json={"target_date": date_str}
    )
    return date_str, response.json()

start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 10, 25)

dates = []
current = start_date
while current <= end_date:
    dates.append(current.strftime("%Y-%m-%d"))
    current += timedelta(days=1)

# 10개씩 동시 처리
with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(insert_date, dates)
    for date, result in results:
        print(f"{date}: {result}")