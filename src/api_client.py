"""おんどとり WebStorage API 通信層。"""

import logging
import time

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.webstorage.jp"
DEVICES_ENDPOINT = "/v1/devices/current"
DATA_ENDPOINT = "/v1/devices/data-rtr500"

# レートリミットしきい値: 残数がこの値以下になったら待機
RATE_LIMIT_THRESHOLD = 1
RATE_LIMIT_WAIT = 10  # 秒

# 子機間のウェイト（秒）
DEVICE_WAIT = 3


class OndotoriAPIError(Exception):
    """API通信エラー。"""


class OndotoriClient:
    """おんどとり WebStorage API クライアント。"""

    def __init__(self, api_key: str, login_id: str, login_pass: str):
        self.api_key = api_key
        self.login_id = login_id
        self.login_pass = login_pass
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "X-HTTP-Method-Override": "GET",
        })

    def _auth_body(self) -> dict:
        """認証パラメータを返す。"""
        return {
            "api-key": self.api_key,
            "login-id": self.login_id,
            "login-pass": self.login_pass,
        }

    def _check_rate_limit(self, response: requests.Response) -> None:
        """レートリミットヘッダを監視し、残数が少なければ待機する。"""
        remaining = response.headers.get("X-RateLimit-Remaining")
        remaining_data = response.headers.get("X-RateLimit-Remaining-DataCount")

        if remaining is not None:
            remaining = int(remaining)
            if remaining <= RATE_LIMIT_THRESHOLD:
                logger.warning(
                    "レートリミット残数が少なくなっています (remaining=%d)。%d秒待機します。",
                    remaining, RATE_LIMIT_WAIT,
                )
                time.sleep(RATE_LIMIT_WAIT)

        if remaining_data is not None:
            remaining_data = int(remaining_data)
            if remaining_data <= 1000:
                logger.warning(
                    "データ取得レートリミット残数が少なくなっています (remaining=%d)。%d秒待機します。",
                    remaining_data, RATE_LIMIT_WAIT,
                )
                time.sleep(RATE_LIMIT_WAIT)

    def _request(self, endpoint: str, body: dict, retry: bool = True) -> dict:
        """APIリクエストを送信する。失敗時は1回リトライ。"""
        url = BASE_URL + endpoint
        try:
            response = self.session.post(url, json=body)
            self._check_rate_limit(response)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as e:
            if retry:
                logger.warning("API通信エラー (%s)。リトライします...", e)
                time.sleep(2)
                return self._request(endpoint, body, retry=False)
            logger.error("API通信エラー（リトライ後も失敗）: %s", e)
            raise OndotoriAPIError(f"API通信に失敗しました: {e}") from e

    def get_devices(self, base_serials: list[str]) -> list[dict]:
        """子機一覧を取得する。

        base_serials で指定した親機に紐づく子機のみ返す。

        Returns:
            子機情報のリスト。各要素は config.json の devices 形式に変換済み。
        """
        body = self._auth_body()
        result = self._request(DEVICES_ENDPOINT, body)

        base_set = set(base_serials) if base_serials else None

        devices = []
        for dev in result.get("devices", []):
            # 親機シリアルは baseunit.serial にネストされている
            baseunit = dev.get("baseunit", {})
            dev_base_serial = baseunit.get("serial", "")

            # base_serials が指定されている場合、該当する親機の子機のみ抽出
            if base_set and dev_base_serial not in base_set:
                continue

            channels = []
            for ch in dev.get("channel", []):
                ch_num = int(ch.get("num", 1))
                ch_name = ch.get("name", "").strip()
                if not ch_name:
                    ch_name = f"Ch.{ch_num}"
                unit = ch.get("unit", "")
                col_name = f"{dev.get('name', dev['serial'])} {ch_name}"
                channels.append({
                    "num": ch_num,
                    "name": ch_name,
                    "unit": unit,
                    "col_name": col_name,
                })
            devices.append({
                "serial": dev["serial"],
                "base_serial": dev_base_serial,
                "model": dev.get("model", ""),
                "name": dev.get("name", dev["serial"]),
                "channels": channels,
            })
        return devices

    def get_data(
        self,
        remote_serial: str,
        base_serial: str,
        from_ts: int,
        to_ts: int,
    ) -> dict:
        """子機の期間指定データを取得する。

        Args:
            remote_serial: 子機シリアル番号
            base_serial: 親機シリアル番号
            from_ts: 取得開始 unixtime
            to_ts: 取得終了 unixtime

        Returns:
            APIレスポンス（data配列を含む dict）
        """
        body = self._auth_body()
        body["remote-serial"] = remote_serial
        body["base-serial"] = base_serial
        body["unixtime-from"] = str(from_ts)
        body["unixtime-to"] = str(to_ts)
        return self._request(DATA_ENDPOINT, body)
