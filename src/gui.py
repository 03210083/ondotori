"""おんどとりデータ取得ツール PyQt6 GUI。"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from threading import Thread

from PyQt6.QtCore import QObject, Qt, pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from src import get_app_dir
from src.api_client import DEVICE_WAIT, OndotoriClient
from src.config_manager import (
    get_config_path,
    get_last_fetch,
    load_config,
    save_config,
    update_devices,
    update_last_fetch,
)
from src.csv_exporter import export_csv
from src.data_processor import align_device_data, get_column_order, merge_all_devices
from src.gap_manager import (
    check_continuous_gaps,
    detect_gaps,
    load_gaps,
    merge_gaps,
    retry_gaps,
    save_gaps,
)

JST = timezone(timedelta(hours=9))


class LogSignal(QObject):
    """ログメッセージをGUIスレッドに送るシグナル。"""
    message = pyqtSignal(str)


class QtLogHandler(logging.Handler):
    """logging → GUI テキスト領域へ転送するハンドラ。"""

    def __init__(self, signal: LogSignal):
        super().__init__()
        self.signal = signal
        self.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", datefmt="%H:%M:%S"))

    def emit(self, record):
        msg = self.format(record)
        self.signal.message.emit(msg)


class WorkerSignal(QObject):
    """ワーカースレッドからGUIへの通知シグナル。"""
    finished = pyqtSignal(str)  # 完了メッセージ
    devices_updated = pyqtSignal()
    status_updated = pyqtSignal(str)
    warning = pyqtSignal(str)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("おんどとり データ取得ツール")
        self.setMinimumSize(750, 650)

        self.config = load_config()
        self.worker_signal = WorkerSignal()
        self.log_signal = LogSignal()
        self._running = False

        self._setup_logging()
        self._build_ui()
        self._load_config_to_ui()
        self._connect_signals()
        self._update_status()

        # 起動時にconfig.jsonのパスをログ表示（デバッグ用）
        self._append_log(f"[起動] config: {get_config_path()}")

    def _setup_logging(self):
        self.qt_handler = QtLogHandler(self.log_signal)
        self.qt_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(self.qt_handler)

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # --- API設定 ---
        settings_group = QGroupBox("API設定")
        settings_layout = QVBoxLayout(settings_group)

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("API Key:"))
        self.api_key_edit = QLineEdit()
        self.api_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        row1.addWidget(self.api_key_edit)
        settings_layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Login ID:"))
        self.login_id_edit = QLineEdit()
        row2.addWidget(self.login_id_edit)
        row2.addWidget(QLabel("Password:"))
        self.login_pass_edit = QLineEdit()
        self.login_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        row2.addWidget(self.login_pass_edit)
        settings_layout.addLayout(row2)

        row3 = QHBoxLayout()
        row3.addWidget(QLabel("親機S/N:"))
        self.base_serial_edit = QLineEdit()
        self.base_serial_edit.setPlaceholderText("複数台はカンマ区切り (例: 58580041, 58580E2A)")
        row3.addWidget(self.base_serial_edit)
        row3.addWidget(QLabel("開始日:"))
        self.start_date_edit = QLineEdit()
        self.start_date_edit.setPlaceholderText("YYYY-MM-DD")
        row3.addWidget(self.start_date_edit)
        settings_layout.addLayout(row3)

        # CSV保存先
        row4 = QHBoxLayout()
        row4.addWidget(QLabel("CSV保存先:"))
        self.output_dir_edit = QLineEdit()
        self.output_dir_edit.setPlaceholderText("(デフォルト: exeと同じフォルダのdata/)")
        row4.addWidget(self.output_dir_edit)
        browse_btn = QPushButton("参照...")
        browse_btn.setFixedWidth(70)
        browse_btn.clicked.connect(self._browse_output_dir)
        row4.addWidget(browse_btn)
        settings_layout.addLayout(row4)

        save_btn = QPushButton("設定を保存")
        save_btn.clicked.connect(self._save_config)
        settings_layout.addWidget(save_btn)

        layout.addWidget(settings_group)

        # --- ステータス ---
        self.status_label = QLabel("ステータス: -")
        layout.addWidget(self.status_label)

        self.warning_label = QLabel("")
        self.warning_label.setStyleSheet("color: red; font-weight: bold;")
        self.warning_label.hide()
        layout.addWidget(self.warning_label)

        # --- ボタン行 ---
        btn_row = QHBoxLayout()
        self.devices_btn = QPushButton("子機一覧取得")
        self.devices_btn.clicked.connect(self._on_devices)
        btn_row.addWidget(self.devices_btn)

        self.fetch_btn = QPushButton("データ更新")
        self.fetch_btn.clicked.connect(self._on_fetch)
        btn_row.addWidget(self.fetch_btn)

        self.csv_btn = QPushButton("CSV出力")
        self.csv_btn.clicked.connect(self._on_csv)
        self.csv_btn.setEnabled(False)
        btn_row.addWidget(self.csv_btn)

        layout.addLayout(btn_row)

        # --- 子機テーブル（有効チェックボックス付き） ---
        self.device_table = QTableWidget(0, 5)
        self.device_table.setHorizontalHeaderLabels(["有効", "名前", "シリアル", "モデル", "チャンネル"])
        header = self.device_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        header.resizeSection(0, 40)
        for col in range(1, 5):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Stretch)
        self.device_table.setMaximumHeight(200)
        layout.addWidget(self.device_table)

        # --- ログ ---
        log_group = QGroupBox("ログ")
        log_layout = QVBoxLayout(log_group)
        self.log_text = QPlainTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Consolas", 9))
        self.log_text.setMaximumBlockCount(500)
        log_layout.addWidget(self.log_text)
        layout.addWidget(log_group)

    def _connect_signals(self):
        self.log_signal.message.connect(self._append_log)
        self.worker_signal.finished.connect(self._on_worker_finished)
        self.worker_signal.devices_updated.connect(self._refresh_device_table)
        self.worker_signal.status_updated.connect(self._set_status)
        self.worker_signal.warning.connect(self._show_warning)

    def _load_config_to_ui(self):
        self.api_key_edit.setText(self.config.get("api_key", ""))
        self.login_id_edit.setText(self.config.get("login_id", ""))
        self.login_pass_edit.setText(self.config.get("login_pass", ""))
        base_serials = self.config.get("base_serials", [])
        self.base_serial_edit.setText(", ".join(base_serials))
        self.start_date_edit.setText(self.config.get("start_date", ""))
        self.output_dir_edit.setText(self.config.get("output_dir", ""))
        self._refresh_device_table()

    def _browse_output_dir(self):
        current = self.output_dir_edit.text().strip()
        if not current:
            current = get_app_dir()
        folder = QFileDialog.getExistingDirectory(self, "CSV保存先を選択", current)
        if folder:
            self.output_dir_edit.setText(folder)

    def _save_config(self):
        self._sync_config_from_ui()
        save_config(self.config)
        self._append_log(f"[設定] config.json を保存しました。({get_config_path()})")

    def _sync_config_from_ui(self):
        """GUIの入力値を self.config に反映する。"""
        self.config["api_key"] = self.api_key_edit.text().strip()
        self.config["login_id"] = self.login_id_edit.text().strip()
        self.config["login_pass"] = self.login_pass_edit.text().strip()
        self.config["base_serials"] = [
            s.strip() for s in self.base_serial_edit.text().split(",") if s.strip()
        ]
        self.config["start_date"] = self.start_date_edit.text().strip()
        self.config["output_dir"] = self.output_dir_edit.text().strip()
        # 子機の有効/無効状態を保存
        self._sync_device_enabled()

    def _sync_device_enabled(self):
        """テーブルのチェック状態を config の devices に反映する。"""
        devices = self.config.get("devices", [])
        for i, dev in enumerate(devices):
            if i < self.device_table.rowCount():
                widget = self.device_table.cellWidget(i, 0)
                if widget:
                    cb = widget.findChild(QCheckBox)
                    if cb:
                        dev["enabled"] = cb.isChecked()

    def _refresh_device_table(self):
        devices = self.config.get("devices", [])
        self.device_table.setRowCount(len(devices))
        for i, dev in enumerate(devices):
            # チェックボックス（有効/無効）
            cb = QCheckBox()
            cb.setChecked(dev.get("enabled", True))
            cb_widget = QWidget()
            cb_layout = QHBoxLayout(cb_widget)
            cb_layout.addWidget(cb)
            cb_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            cb_layout.setContentsMargins(0, 0, 0, 0)
            self.device_table.setCellWidget(i, 0, cb_widget)

            self.device_table.setItem(i, 1, QTableWidgetItem(dev.get("name", "")))
            self.device_table.setItem(i, 2, QTableWidgetItem(dev.get("serial", "")))
            self.device_table.setItem(i, 3, QTableWidgetItem(dev.get("model", "")))
            ch_str = ", ".join(ch["col_name"] for ch in dev.get("channels", []))
            self.device_table.setItem(i, 4, QTableWidgetItem(ch_str))

    def _update_status(self):
        devices = self.config.get("devices", [])
        enabled_count = sum(1 for d in devices if d.get("enabled", True))
        last_fetches = self.config.get("last_fetch", {})
        if last_fetches:
            latest = max(last_fetches.values())
            last_str = latest[:19].replace("T", " ")
        else:
            last_str = "未取得"
        self.status_label.setText(
            f"子機数: {enabled_count}/{len(devices)}台 | 最終更新: {last_str} | 開始日: {self.config.get('start_date', '-')}"
        )

    def _set_buttons_enabled(self, enabled: bool):
        self.devices_btn.setEnabled(enabled)
        self.fetch_btn.setEnabled(enabled)
        self.csv_btn.setEnabled(enabled)

    def _run_in_thread(self, target):
        if self._running:
            return
        self._sync_config_from_ui()
        self._running = True
        self._set_buttons_enabled(False)
        thread = Thread(target=target, daemon=True)
        thread.start()

    def _make_client(self) -> OndotoriClient:
        return OndotoriClient(
            self.config["api_key"],
            self.config["login_id"],
            self.config["login_pass"],
        )

    def _get_enabled_devices(self) -> list[dict]:
        """有効な子機のみ返す。"""
        return [d for d in self.config.get("devices", []) if d.get("enabled", True)]

    # --- ワーカー: 子機一覧 ---
    def _on_devices(self):
        self._run_in_thread(self._worker_devices)

    def _worker_devices(self):
        logger = logging.getLogger("gui")
        try:
            client = self._make_client()
            logger.info("子機一覧を取得中...")
            devices = client.get_devices(self.config["base_serials"])
            # 既存のenabled状態を引き継ぐ
            old_enabled = {d["serial"]: d.get("enabled", True) for d in self.config.get("devices", [])}
            for dev in devices:
                dev["enabled"] = old_enabled.get(dev["serial"], True)
            self.config = update_devices(self.config, devices)
            save_config(self.config)
            self.worker_signal.devices_updated.emit()
            self.worker_signal.finished.emit(f"子機一覧取得完了: {len(devices)}台")
        except Exception as e:
            self.worker_signal.finished.emit(f"エラー: {e}")

    # --- ワーカー: データ取得 ---
    def _on_fetch(self):
        self._run_in_thread(self._worker_fetch)

    def _worker_fetch(self):
        logger = logging.getLogger("gui")
        try:
            client = self._make_client()
            devices = self._get_enabled_devices()
            if not devices:
                self.worker_signal.finished.emit("エラー: 有効なデバイスがありません。")
                return

            to_dt = datetime.now(tz=JST)
            to_ts = int(to_dt.timestamp())
            default_from_dt = datetime.strptime(
                self.config["start_date"], "%Y-%m-%d"
            ).replace(tzinfo=JST)

            all_data = {}
            for i, dev in enumerate(devices):
                serial = dev["serial"]
                last = get_last_fetch(self.config, serial)
                from_dt = datetime.fromisoformat(last) if last else default_from_dt
                from_ts = int(from_dt.timestamp())

                logger.info(
                    "[%d/%d] %s (%s) 取得中...",
                    i + 1, len(devices), dev["name"], serial,
                )
                self.worker_signal.status_updated.emit(
                    f"データ更新中... {i + 1}/{len(devices)} — {dev['name']}"
                )
                try:
                    raw = client.get_data(serial, dev["base_serial"], from_ts, to_ts)
                    count = len(raw.get("data", []))
                    logger.info("  %d件取得", count)
                    aligned = align_device_data(raw, dev["channels"])
                    all_data[serial] = aligned
                    self.config = update_last_fetch(self.config, serial, to_dt.isoformat())
                except Exception as e:
                    logger.error("  取得失敗: %s", e)
                    all_data[serial] = {}

                if i < len(devices) - 1:
                    time.sleep(DEVICE_WAIT)

            column_order = get_column_order(devices)
            self.merged = merge_all_devices(all_data, column_order)
            self.column_order = column_order

            if not self.merged:
                self.worker_signal.finished.emit("取得データが空です。")
                return

            # 欠損管理（有効な子機のみ）
            gaps = load_gaps()
            new_gaps = detect_gaps(self.merged, devices)
            gaps = merge_gaps(gaps, new_gaps)

            warnings = check_continuous_gaps(gaps)
            for w in warnings:
                self.worker_signal.warning.emit(
                    f"{w['name']}: {w['days']}日間連続欠損 ({w['start']} ~ {w['end']}) 電池確認要"
                )

            save_gaps(gaps)
            save_config(self.config)

            # CSV出力
            output_dir = self.config.get("output_dir", "").strip() or None
            filepath = export_csv(self.merged, column_order, output_dir=output_dir)
            self.worker_signal.finished.emit(f"データ更新完了。CSV: {filepath}")

        except Exception as e:
            self.worker_signal.finished.emit(f"エラー: {e}")

    # --- CSV出力（手動） ---
    def _on_csv(self):
        if not hasattr(self, "merged") or not self.merged:
            self._append_log("出力するデータがありません。先にデータ更新を実行してください。")
            return
        output_dir = self.config.get("output_dir", "").strip() or None
        filepath = export_csv(self.merged, self.column_order, output_dir=output_dir)
        self._append_log(f"CSV出力: {filepath}")

    # --- シグナルハンドラ ---
    def _append_log(self, msg: str):
        self.log_text.appendPlainText(msg)

    def _on_worker_finished(self, msg: str):
        self._running = False
        self._set_buttons_enabled(True)
        self.csv_btn.setEnabled(hasattr(self, "merged") and bool(self.merged))
        self._append_log(msg)
        self._update_status()

    def _set_status(self, msg: str):
        self.status_label.setText(msg)

    def _show_warning(self, msg: str):
        self.warning_label.setText(f"⚠ {msg}")
        self.warning_label.show()


def run_gui():
    """GUI アプリケーションを起動する。"""
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
