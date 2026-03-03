"""データの10分整列・タイムライン統合を行うモジュール。"""

import logging
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def round_to_10min(unix_ts: int) -> datetime:
    """UNIXタイムスタンプを最寄りの10分に丸める。

    Args:
        unix_ts: UNIXタイムスタンプ（秒）

    Returns:
        JSTの10分丸めdatetime
    """
    dt = datetime.fromtimestamp(unix_ts, tz=JST)
    minute = dt.minute
    remainder = minute % 10
    if remainder < 5:
        rounded = dt.replace(second=0, microsecond=0) - timedelta(minutes=remainder)
    else:
        rounded = dt.replace(second=0, microsecond=0) + timedelta(minutes=10 - remainder)
    return rounded


def align_device_data(raw_data: dict, channels: list[dict]) -> dict[datetime, dict]:
    """1子機のAPIレスポンスデータを10分間隔に整列する。

    Args:
        raw_data: API レスポンス（data 配列を含む）
        channels: チャンネル情報のリスト

    Returns:
        {datetime: {col_name: value, ...}, ...} の辞書
    """
    aligned = {}
    for record in raw_data.get("data", []):
        unix_ts = record.get("unixtime")
        if unix_ts is None:
            continue
        rounded_dt = round_to_10min(int(unix_ts))

        row = {}
        for ch in channels:
            ch_key = f"ch{ch['num']}"
            value = record.get(ch_key)
            if value is None:
                continue
            # エラー値（E始まり）は除外
            if isinstance(value, str) and value.startswith("E"):
                logger.debug("エラー値を除外: %s=%s at %s", ch_key, value, rounded_dt)
                continue
            try:
                row[ch["col_name"]] = float(value)
            except (ValueError, TypeError):
                logger.debug("数値変換失敗: %s=%s at %s", ch_key, value, rounded_dt)
                continue

        if row:
            aligned[rounded_dt] = row

    return aligned


def merge_all_devices(
    all_data: dict[str, dict[datetime, dict]],
    column_order: list[str],
) -> OrderedDict[datetime, dict]:
    """全子機のデータを共通タイムラインに統合する。

    Args:
        all_data: {device_serial: {datetime: {col_name: value}}} の辞書
        column_order: 列名の並び順リスト

    Returns:
        時刻順に並んだ OrderedDict[datetime, {col_name: value or None}]
    """
    # 全タイムスタンプを収集
    all_times: set[datetime] = set()
    for device_data in all_data.values():
        all_times.update(device_data.keys())

    # 時刻順にソート
    sorted_times = sorted(all_times)

    # 統合
    merged = OrderedDict()
    for dt in sorted_times:
        row = {}
        for col in column_order:
            row[col] = None
        for device_data in all_data.values():
            if dt in device_data:
                for col, val in device_data[dt].items():
                    row[col] = val
        merged[dt] = row

    logger.info(
        "データ統合完了: %d タイムスロット, %d 列",
        len(merged), len(column_order),
    )
    return merged


def get_column_order(devices: list[dict]) -> list[str]:
    """config の devices リストから列名の並び順を生成する。"""
    columns = []
    for dev in devices:
        for ch in dev.get("channels", []):
            columns.append(ch["col_name"])
    return columns
