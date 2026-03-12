# おんどとり データ取得ツール

おんどとり WebStorage API からセンサーデータを取得し、10分間隔に整列して累積CSVに出力するツール。

## 機能

- PyQt6 GUI + `--auto` ヘッドレスモード
- 子機データの差分取得・累積CSV出力（UTF-8 BOM）
- 欠損検知・自動リトライ
- 子機一覧の五十音（名前順）ソート
- PyInstaller exe化対応、GitHub Actions CI

## 対応機種

### 親機（ベースステーション）

| 機種 | 通信方式 | データ取得 | 備考 |
|---|---|---|---|
| **RTR500BW** | WiFi (HTTPS) | **対応** | `/v1/devices/data-rtr500` |
| **RTR500BM** | 4Gモバイル (HTTPS) | **対応** | `/v1/devices/data-rtr500` |
| RTR500MBS | 3Gモバイル (FTP) | **非対応** | 現在値のみ取得可。期間指定データ取得API非対応 |
| RTR500BC | — | **非対応** | API利用不可（T&D公式ドキュメントで明記） |
| RTR-500 | — | **非対応** | 現在値のみ |
| TR-700W | — | **非対応** | 現在値のみ |

### 子機（リモートユニット）

| 機種グループ | データ取得 | APIエンドポイント |
|---|---|---|
| **RTR-5xxシリーズ** (RTR502B, RTR503B, RTR505B, RTR507B等) | **対応** | `/v1/devices/data-rtr500` (親機経由) |
| **TR-7シリーズ** (TR-71A, TR-72A, TR-7wb, TR-7nw等) | **対応** | `/v1/devices/data` |
| **TR4A, TR32B** | **対応** | `/v1/devices/data` |
| TR-5i, TR-7Ui | **非対応** | APIサポートなし |

> 子機のモデル名が `RTR` で始まる場合は `data-rtr500` エンドポイント（`base-serial` 必須）、
> それ以外は汎用 `data` エンドポイントを自動選択します。

## 使い方

### GUI

```bash
python main.py
```

### ヘッドレス（自動実行）

```bash
python main.py --auto           # データ更新 → CSV出力
python main.py --auto --retry   # 欠損補完も実行
```

### CLI

```bash
python scripts/run.py init      # config.json を対話的に作成
python scripts/run.py devices   # 子機一覧を取得・表示
python scripts/run.py fetch     # データ取得 → CSV出力
```

## 設定

`config.json`（gitignore済み）に以下を記載:

| キー | 説明 |
|---|---|
| `api_key` | WebStorage API キー (45文字) |
| `login_id` | ログインID |
| `login_pass` | パスワード |
| `base_serials` | 親機S/Nリスト（空配列で全子機取得） |
| `start_date` | データ取得開始日 (YYYY-MM-DD) |
| `output_dir` | CSV出力先（空欄でデフォルト `data/`） |

## 出力

- `ondotori_data.csv`: 全期間の累積データ（UTF-8 BOM、10分間隔）
- 列順は子機名の五十音（名前順）ソート
