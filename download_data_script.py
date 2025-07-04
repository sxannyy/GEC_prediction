import os
import requests
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Для работы скрипта надо в одну директорию с ним разместить файл со значениями GEC (https://simurg.iszf.irk.ru/gec)
# Конфигурация
output_dir = "data/raw_images"
os.makedirs(output_dir, exist_ok=True)

source_ids = [10, 11, 12, 13, 14]
channels = {10: 171, 11: 193, 12: 211, 13: 304, 14: 335}

def get_image_id(timestamp: pd.Timestamp, source_id: int) -> str | None:
    date_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"https://api.helioviewer.org/v2/getClosestImage/?date={date_str}&sourceId={source_id}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("id")
    except Exception as e:
        print(f"Ошибка запроса getClosestImage для {date_str} sourceId={source_id}: {e}")
        return None

def download_image(image_id: str, save_path: str):
    if os.path.exists(save_path):
        return
    url = f"https://api.helioviewer.org/v2/downloadImage/?id={image_id}&scale=16"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(r.content)
    except Exception as e:
        print(f"Не удалось скачать изображение id={image_id}: {e}")

def download_one(ts: pd.Timestamp, sid: int):
    fname = f"{ts.strftime('%Y%m%d_%H%M%S')}_AIA_{channels[sid]}.jpg"
    save_path = os.path.join(output_dir, fname)
    if os.path.exists(save_path):
        return
    image_id = get_image_id(ts, sid)
    if image_id is None:
        print(f"Нет изображения для {ts} sourceId={sid}")
        return
    download_image(image_id, save_path)

def download_images_from_df(df: pd.DataFrame, max_workers=32):
    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for ts in df["timestamp"]:
            for sid in source_ids:
                tasks.append(executor.submit(download_one, ts, sid))
        for _ in tqdm(as_completed(tasks), total=len(tasks)):
            pass

# Загрузка данных и запуск
df = pd.read_csv("gec.csv", sep=r'\s+')
df = df[df['Hour'] == 12]
df["timestamp"] = pd.to_datetime(df[["Year", "Month", "Day", "Hour"]], utc=True)

df = df[["timestamp", "igsg"]].rename(columns={"igsg": "gec"})
df = df[df["timestamp"].between("2014-01-01", "2024-12-31")].reset_index(drop=True)

download_images_from_df(df)
