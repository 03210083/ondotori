"""UTF-8 BOM付き CSV 出力モジュール（累積型）。"""

import csv
import logging
import os
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from src import get_app_dir

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

FILENAME = "ondotori_data.csv"


def get_data_dir() -> str:
    """data/ フォルダのパスを返す。"""
    return os.path.join(get_app_dir(), "data")


def load_existing_csv(filepath: str) -> tuple[list[str], OrderedDict[datetime, dict]]:
    """既存CSVを読み込み、(列名リスト, データ辞書) を返す。

    ファイルが存在しない場合は空を返す。
    """
    if not os.path.exists(filepath):
        return [], OrderedDict()

    try:
        with open(filepath, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return [], OrderedDict()

            columns = header[1:]  # Date/Time を除く
            data = OrderedDict()
            for row in reader:
                if not row or not row[0].strip():
                    continue
                try:
                    dt = datetime.strptime(row[0].strip(), "%Y/%m/%d %H:%M").replace(tzinfo=JST)
                except ValueError:
                    continue
                values = {}
                for j, col in enumerate(columns):
                    cell = row[j + 1] if j + 1 < len(row) else ""
                    if cell.strip():
                        try:
                            values[col] = float(cell)
                        except ValueError:
                            values[col] = cell
                    else:
                        values[col] = None
                data[dt] = values

        logger.info("既存CSV読み込み: %s (%d行, %d列)", filepath, len(data), len(columns))
        return columns, data

    except (OSError, csv.Error) as e:
        logger.warning("既存CSVの読み込みに失敗しました (%s)。新規作成します。", e)
        return [], OrderedDict()


def merge_with_existing(
    existing_data: OrderedDict[datetime, dict],
    new_data: OrderedDict[datetime, dict],
    existing_columns: list[str],
    new_columns: list[str],
) -> tuple[list[str], OrderedDict[datetime, dict]]:
    """既存データと新規データをマージする。

    同一タイムスタンプの場合、新規データで上書き（欠損補完に対応）。
    列は既存列を維持しつつ、新規列があれば追加。
    """
    # 列の統合（順序を維持しつつ新規列を追加）
    column_set = set(existing_columns)
    merged_columns = list(existing_columns)
    for col in new_columns:
        if col not in column_set:
            merged_columns.append(col)
            column_set.add(col)

    # データのマージ
    merged = OrderedDict(existing_data)
    for dt, row in new_data.items():
        if dt in merged:
            # 既存行に新規データを上書き（Noneでない値のみ）
            for col, val in row.items():
                if val is not None:
                    merged[dt][col] = val
        else:
            merged[dt] = row

    # 時刻順にソート
    sorted_merged = OrderedDict(sorted(merged.items()))

    logger.info("データマージ完了: %d行 (既存%d + 新規%d)", len(sorted_merged), len(existing_data), len(new_data))
    return merged_columns, sorted_merged


def export_csv(
    merged_data: OrderedDict[datetime, dict],
    column_order: list[str],
    output_dir: str | None = None,
    filename: str | None = None,
) -> str:
    """新規データを既存CSVに累積して出力する。

    Args:
        merged_data: 今回取得したデータ（merge_all_devices の出力）
        column_order: 今回のデータの列名リスト
        output_dir: 出力先ディレクトリ（省略時は data/）
        filename: ファイル名（省略時は ondotori_data.csv）

    Returns:
        出力したファイルのパス
    """
    output_dir = output_dir or get_data_dir()
    os.makedirs(output_dir, exist_ok=True)

    filename = filename or FILENAME
    filepath = os.path.join(output_dir, filename)

    # 既存CSVを読み込み
    existing_columns, existing_data = load_existing_csv(filepath)

    # マージ
    if existing_data:
        final_columns, final_data = merge_with_existing(
            existing_data, merged_data, existing_columns, column_order,
        )
    else:
        final_columns = column_order
        final_data = merged_data

    # 書き出し
    header = ["Date/Time"] + final_columns

    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for dt, row in final_data.items():
            dt_str = dt.strftime("%Y/%m/%d %H:%M")
            values = [dt_str]
            for col in final_columns:
                val = row.get(col)
                values.append(val if val is not None else "")
            writer.writerow(values)

    logger.info("CSV出力完了: %s (全%d行)", filepath, len(final_data))
    return filepath
