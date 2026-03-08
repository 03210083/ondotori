#!/usr/bin/env python3
"""おんどとりデータ取得ツール エントリポイント。

Usage:
    python main.py              GUI を起動
    python main.py --auto       ヘッドレス実行（データ更新 → CSV出力）
    python main.py --auto --retry  ヘッドレス実行 + 欠損補完
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(description="おんどとり データ取得ツール")
    parser.add_argument("--auto", action="store_true", help="GUIなしでデータ更新・CSV出力して終了")
    parser.add_argument("--retry", action="store_true", help="欠損補完も実行（--auto時のみ有効）")
    args = parser.parse_args()

    if args.auto:
        # ヘッドレス実行: 既存CLIのfetchロジックを流用
        from scripts.run import cmd_fetch, setup_logging

        setup_logging()

        # --retry フラグ付きの名前空間を渡す
        fetch_args = argparse.Namespace(retry=args.retry)
        cmd_fetch(fetch_args)
    else:
        from src.gui import run_gui

        run_gui()


if __name__ == "__main__":
    main()
