"""欠損検知・管理を行うモジュール。"""

import json
import logging
import os
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from src import get_app_dir
from .api_client import DEVICE_WAIT, OndotoriAPIError, OndotoriClient
from .data_processor import align_device_data

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def get_gaps_path() -> str:
    """gaps.json のパスを返す。"""
    return os.path.join(get_app_dir(), "gaps.json")


def load_gaps(path: str | None = None) -> list[dict]:
    """gaps.json を読み込む。"""
    path = path or get_gaps_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("gaps", [])
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("gaps.json の読み込みに失敗しました (%s)。空のリストを使用します。", e)
        return []


def save_gaps(gaps: list[dict], path: str | None = None) -> None:
    """gaps.json に保存する。"""
    path = path or get_gaps_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"gaps": gaps}, f, ensure_ascii=False, indent=2)
    logger.info("gaps.json を保存しました (%d件)。", len(gaps))


def detect_gaps(
    merged_data: OrderedDict[datetime, dict],
    devices: list[dict],
) -> list[dict]:
    """統合データから欠損を検知し、gapレコードを生成する。

    Args:
        merged_data: merge_all_devices の出力
        devices: config の devices リスト

    Returns:
        新規に検出された gap レコードのリスト
    """
    now = datetime.now(tz=JST).isoformat()
    new_gaps = []

    for dt, row in merged_data.items():
        for dev in devices:
            for ch in dev.get("channels", []):
                col_name = ch["col_name"]
                if row.get(col_name) is None:
                    new_gaps.append({
                        "datetime": dt.isoformat(),
                        "serial": dev["serial"],
                        "name": dev["name"],
                        "channel": ch["num"],
                        "registered": now,
                        "retries": 0,
                        "status": "unresolved",
                        "resolved_at": None,
                    })

    logger.info("新規欠損 %d件を検出しました。", len(new_gaps))
    return new_gaps


def merge_gaps(existing: list[dict], new_gaps: list[dict]) -> list[dict]:
    """既存の gap リストに新規 gap をマージする（重複は追加しない）。"""
    existing_keys = set()
    for g in existing:
        key = (g["datetime"], g["serial"], g["channel"])
        existing_keys.add(key)

    added = 0
    for g in new_gaps:
        key = (g["datetime"], g["serial"], g["channel"])
        if key not in existing_keys:
            existing.append(g)
            existing_keys.add(key)
            added += 1

    if added:
        logger.info("gaps に %d件追加しました。", added)
    return existing


def retry_gaps(
    client: OndotoriClient,
    gaps: list[dict],
    devices: list[dict],
) -> list[dict]:
    """未解消の gap に対して API 再取得を試行する。

    Args:
        client: OndotoriClient インスタンス
        gaps: gap レコードのリスト
        devices: config の devices リスト

    Returns:
        更新された gap リスト
    """
    # デバイスのルックアップ用辞書
    dev_lookup = {}
    for dev in devices:
        dev_lookup[dev["serial"]] = dev

    unresolved = [g for g in gaps if g["status"] == "unresolved"]
    if not unresolved:
        logger.info("未解消の欠損はありません。")
        return gaps

    logger.info("未解消の欠損 %d件に対して再取得を試行します。", len(unresolved))

    # シリアルごとにグループ化して効率的に取得
    serial_groups: dict[str, list[dict]] = {}
    for g in unresolved:
        serial_groups.setdefault(g["serial"], []).append(g)

    for serial, serial_gaps in serial_groups.items():
        dev = dev_lookup.get(serial)
        if not dev:
            logger.warning("デバイス %s が config に見つかりません。スキップします。", serial)
            continue

        # この子機の未解消 gap の時間範囲を算出
        gap_times = [datetime.fromisoformat(g["datetime"]) for g in serial_gaps]
        from_dt = min(gap_times) - timedelta(minutes=10)
        to_dt = max(gap_times) + timedelta(minutes=10)
        from_ts = int(from_dt.timestamp())
        to_ts = int(to_dt.timestamp())

        try:
            raw = client.get_data(serial, dev["base_serial"], from_ts, to_ts)
            aligned = align_device_data(raw, dev["channels"])

            for g in serial_gaps:
                gap_dt = datetime.fromisoformat(g["datetime"])
                if gap_dt in aligned:
                    ch_col = None
                    for ch in dev["channels"]:
                        if ch["num"] == g["channel"]:
                            ch_col = ch["col_name"]
                            break
                    if ch_col and ch_col in aligned[gap_dt]:
                        g["status"] = "resolved"
                        g["resolved_at"] = datetime.now(tz=JST).isoformat()
                        logger.info("欠損解消: %s %s ch%d", g["datetime"], g["name"], g["channel"])
                    else:
                        g["retries"] += 1
                else:
                    g["retries"] += 1

        except OndotoriAPIError as e:
            logger.warning("再取得失敗 (%s): %s", serial, e)
            for g in serial_gaps:
                g["retries"] += 1

        time.sleep(DEVICE_WAIT)

    resolved = sum(1 for g in gaps if g["status"] == "resolved")
    logger.info("再取得完了: %d件解消", resolved)
    return gaps


def check_continuous_gaps(gaps: list[dict], threshold_days: int = 3) -> list[dict]:
    """同一子機で連続3日以上欠損があるケースを検出する。

    Returns:
        警告対象の子機情報リスト [{serial, name, days, start, end}]
    """
    # シリアルごとに欠損日をまとめる
    serial_dates: dict[str, set[str]] = {}
    serial_names: dict[str, str] = {}
    for g in gaps:
        if g["status"] != "unresolved":
            continue
        dt = datetime.fromisoformat(g["datetime"])
        date_str = dt.strftime("%Y-%m-%d")
        serial_dates.setdefault(g["serial"], set()).add(date_str)
        serial_names[g["serial"]] = g["name"]

    warnings = []
    for serial, dates in serial_dates.items():
        sorted_dates = sorted(dates)
        if len(sorted_dates) < threshold_days:
            continue

        # 連続日数を計算
        consecutive = 1
        max_consecutive = 1
        start_date = sorted_dates[0]
        best_start = start_date
        best_end = start_date

        for i in range(1, len(sorted_dates)):
            prev = datetime.strptime(sorted_dates[i - 1], "%Y-%m-%d")
            curr = datetime.strptime(sorted_dates[i], "%Y-%m-%d")
            if (curr - prev).days == 1:
                consecutive += 1
                if consecutive > max_consecutive:
                    max_consecutive = consecutive
                    best_start = start_date
                    best_end = sorted_dates[i]
            else:
                consecutive = 1
                start_date = sorted_dates[i]

        if max_consecutive >= threshold_days:
            warnings.append({
                "serial": serial,
                "name": serial_names[serial],
                "days": max_consecutive,
                "start": best_start,
                "end": best_end,
            })
            logger.warning(
                "⚠ %s: %d日間連続欠損 (%s ~ %s)（電池確認要）",
                serial_names[serial], max_consecutive, best_start, best_end,
            )

    return warnings
