"""UTF-8 BOM付き CSV 出力モジュール。"""

import csv
import logging
import os
from collections import OrderedDict
from datetime import datetime

from src import get_app_dir

logger = logging.getLogger(__name__)

BOM = "\ufeff"


def get_data_dir() -> str:
    """data/ フォルダのパスを返す。"""
    return os.path.join(get_app_dir(), "data")


def generate_filename(merged_data: OrderedDict[datetime, dict]) -> str:
    """データの日付範囲からファイル名を生成する。"""
    if not merged_data:
        now = datetime.now()
        return f"ondotori_{now:%Y%m%d}_{now:%Y%m%d}.csv"

    times = list(merged_data.keys())
    start = min(times)
    end = max(times)
    return f"ondotori_{start:%Y%m%d}_{end:%Y%m%d}.csv"


def export_csv(
    merged_data: OrderedDict[datetime, dict],
    column_order: list[str],
    output_dir: str | None = None,
    filename: str | None = None,
) -> str:
    """統合データを UTF-8 BOM付き CSV で出力する。

    Args:
        merged_data: merge_all_devices の出力
        column_order: 列名の並び順
        output_dir: 出力先ディレクトリ（省略時は data/）
        filename: ファイル名（省略時は自動生成）

    Returns:
        出力したファイルのパス
    """
    output_dir = output_dir or get_data_dir()
    os.makedirs(output_dir, exist_ok=True)

    filename = filename or generate_filename(merged_data)
    filepath = os.path.join(output_dir, filename)

    header = ["Date/Time"] + column_order

    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for dt, row in merged_data.items():
            dt_str = dt.strftime("%Y/%m/%d %H:%M")
            values = [dt_str]
            for col in column_order:
                val = row.get(col)
                values.append(val if val is not None else "")
            writer.writerow(values)

    logger.info("CSV出力完了: %s (%d行)", filepath, len(merged_data))
    return filepath
