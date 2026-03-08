"""config.json の読み書き・デフォルト生成を管理するモジュール。"""

import json
import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "api_key": "",
    "login_id": "",
    "login_pass": "",
    "base_serials": [],
    "start_date": "2026-02-10",
    "devices": [],
    "last_fetch": {},
}

REQUIRED_KEYS = ["api_key", "login_id", "login_pass", "base_serials", "start_date"]


def get_config_path() -> str:
    """config.json のパスを返す（ondotori/ 直下）。"""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.json")


def load_config(path: str | None = None) -> dict:
    """config.json を読み込む。破損時はデフォルト値で起動し警告を出す。"""
    path = path or get_config_path()
    if not os.path.exists(path):
        logger.info("config.json が見つかりません。デフォルト設定を使用します。")
        return dict(DEFAULT_CONFIG)
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
        # 必須キーの存在チェック
        for key in REQUIRED_KEYS:
            if key not in config:
                config[key] = DEFAULT_CONFIG[key]
                logger.warning("config.json に '%s' がありません。デフォルト値を使用します。", key)
        return config
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("config.json の読み込みに失敗しました (%s)。デフォルト設定を使用します。", e)
        return dict(DEFAULT_CONFIG)


def save_config(config: dict, path: str | None = None) -> None:
    """config.json に保存する。"""
    path = path or get_config_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    logger.info("config.json を保存しました。")


def create_default_config(path: str | None = None) -> dict:
    """デフォルトの config.json を生成して保存する。"""
    path = path or get_config_path()
    config = dict(DEFAULT_CONFIG)
    save_config(config, path)
    return config


def update_devices(config: dict, devices: list[dict]) -> dict:
    """デバイス一覧を config に反映する。"""
    config["devices"] = devices
    return config


def update_last_fetch(config: dict, serial: str, last_fetch: str) -> dict:
    """子機ごとの最終取得日時を config に反映する。"""
    if "last_fetch" not in config or not isinstance(config["last_fetch"], dict):
        config["last_fetch"] = {}
    config["last_fetch"][serial] = last_fetch
    return config


def get_last_fetch(config: dict, serial: str) -> str | None:
    """子機の最終取得日時を返す。未取得なら None。"""
    last_fetch = config.get("last_fetch", {})
    if isinstance(last_fetch, dict):
        return last_fetch.get(serial)
    return None
