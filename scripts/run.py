#!/usr/bin/env python3
"""おんどとりデータ取得ツール CLI エントリポイント。

Usage:
    python run.py init            config.json を対話的に作成
    python run.py devices         子機一覧を取得・表示・config保存
    python run.py fetch           データ取得 → 整列 → 欠損管理 → CSV出力
    python run.py fetch --retry   欠損補完も実行
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

# プロジェクトルートを sys.path に追加
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src import get_app_dir

from src.api_client import DEVICE_WAIT, OndotoriClient
from src.config_manager import (
    create_default_config,
    get_last_fetch,
    load_config,
    save_config,
    update_devices,
    update_last_fetch,
)
from src.csv_exporter import export_csv
from src.data_processor import (
    align_device_data,
    get_column_order,
    merge_all_devices,
)
from src.gap_manager import (
    check_continuous_gaps,
    detect_gaps,
    load_gaps,
    merge_gaps,
    retry_gaps,
    save_gaps,
)

JST = timezone(timedelta(hours=9))


def setup_logging():
    """ロガーを設定する。コンソール + ログファイル出力。"""
    log_dir = os.path.join(get_app_dir(), "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, datetime.now().strftime("%Y%m%d") + ".log")

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # コンソールハンドラ
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    # ファイルハンドラ
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console)
    root_logger.addHandler(file_handler)

    return logging.getLogger("run")


def cmd_init(args):
    """config.json を対話的に作成する。"""
    logger = logging.getLogger("run")
    print("=== おんどとり設定ファイル作成 ===\n")

    api_key = input("API Key (45文字): ").strip()
    login_id = input("Login ID (8文字): ").strip()
    login_pass = input("Login Password: ").strip()
    base_serials_str = input("親機シリアル番号 (カンマ区切り): ").strip()
    start_date = input("取得開始日 (YYYY-MM-DD): ").strip()

    base_serials = [s.strip() for s in base_serials_str.split(",") if s.strip()]

    config = {
        "api_key": api_key,
        "login_id": login_id,
        "login_pass": login_pass,
        "base_serials": base_serials,
        "start_date": start_date,
        "devices": [],
        "last_fetch": {},
    }
    save_config(config)
    logger.info("config.json を作成しました。")
    print("\nconfig.json を作成しました。")


def cmd_devices(args):
    """子機一覧を取得して表示・保存する。"""
    logger = logging.getLogger("run")
    config = load_config()
    client = OndotoriClient(config["api_key"], config["login_id"], config["login_pass"])

    logger.info("子機一覧を取得中...")
    devices = client.get_devices(config["base_serials"])

    print(f"\n=== 子機一覧 ({len(devices)}台) ===\n")
    for i, dev in enumerate(devices, 1):
        ch_info = ", ".join(
            f"{ch['col_name']} [{ch['unit']}]" for ch in dev["channels"]
        )
        print(f"  {i:2d}. {dev['name']} ({dev['serial']}) - {dev['model']}")
        print(f"      チャンネル: {ch_info}")

    config = update_devices(config, devices)
    save_config(config)
    logger.info("子機一覧を config.json に保存しました。")
    print(f"\n子機一覧を config.json に保存しました。")


def cmd_fetch(args):
    """データ取得 → 整列 → 欠損管理 → CSV出力。"""
    logger = logging.getLogger("run")
    config = load_config()
    client = OndotoriClient(config["api_key"], config["login_id"], config["login_pass"])

    devices = config.get("devices", [])
    if not devices:
        logger.error("config.json にデバイス情報がありません。先に 'devices' コマンドを実行してください。")
        print("エラー: デバイス情報がありません。先に 'python run.py devices' を実行してください。")
        sys.exit(1)

    to_dt = datetime.now(tz=JST)
    to_ts = int(to_dt.timestamp())
    default_from_dt = datetime.strptime(config["start_date"], "%Y-%m-%d").replace(tzinfo=JST)

    # 子機ごとにデータ取得（子機ごとの last_fetch から期間決定）
    all_data = {}
    for i, dev in enumerate(devices):
        serial = dev["serial"]
        last = get_last_fetch(config, serial)
        if last:
            from_dt = datetime.fromisoformat(last)
        else:
            from_dt = default_from_dt
        from_ts = int(from_dt.timestamp())

        logger.info(
            "[%d/%d] %s (%s) 取得中... %s ~",
            i + 1, len(devices), dev["name"], serial,
            from_dt.strftime("%Y/%m/%d %H:%M"),
        )
        try:
            raw = client.get_data(serial, dev["base_serial"], from_ts, to_ts)
            record_count = len(raw.get("data", []))
            logger.info("  %d件取得", record_count)
            aligned = align_device_data(raw, dev["channels"])
            all_data[serial] = aligned
            # 取得成功した子機の last_fetch を更新
            config = update_last_fetch(config, serial, to_dt.isoformat())
        except Exception as e:
            logger.error("  取得失敗: %s", e)
            all_data[serial] = {}

        if i < len(devices) - 1:
            time.sleep(DEVICE_WAIT)

    # データ統合
    column_order = get_column_order(devices)
    merged = merge_all_devices(all_data, column_order)

    if not merged:
        logger.warning("取得データが空です。")
        print("取得データが空です。")
        return

    # 欠損管理
    gaps = load_gaps()
    new_gaps = detect_gaps(merged, devices)
    gaps = merge_gaps(gaps, new_gaps)

    # 欠損補完（--retry 指定時）
    if args.retry:
        logger.info("欠損補完を実行中...")
        gaps = retry_gaps(client, gaps, devices)

    # 連続欠損警告
    warnings = check_continuous_gaps(gaps)
    for w in warnings:
        print(f"⚠ {w['name']}: {w['days']}日間連続欠損 ({w['start']} ~ {w['end']})（電池確認要）")

    save_gaps(gaps)

    # CSV出力
    filepath = export_csv(merged, column_order)
    print(f"\nCSV出力: {filepath}")

    save_config(config)

    logger.info("データ更新完了。")
    print("データ更新完了。")


def main():
    parser = argparse.ArgumentParser(
        description="おんどとりデータ取得ツール",
    )
    subparsers = parser.add_subparsers(dest="command", help="コマンド")

    # init
    subparsers.add_parser("init", help="config.json を対話的に作成")

    # devices
    subparsers.add_parser("devices", help="子機一覧を取得・表示")

    # fetch
    fetch_parser = subparsers.add_parser("fetch", help="データ取得 → CSV出力")
    fetch_parser.add_argument(
        "--retry", action="store_true", help="欠損補完も実行",
    )

    args = parser.parse_args()

    setup_logging()

    if args.command == "init":
        cmd_init(args)
    elif args.command == "devices":
        cmd_devices(args)
    elif args.command == "fetch":
        cmd_fetch(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
