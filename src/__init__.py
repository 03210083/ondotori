"""おんどとりデータ取得ツール。"""

import os
import sys


def get_app_dir() -> str:
    """アプリケーションのベースディレクトリを返す。

    PyInstaller exe 実行時は exe の配置場所、
    開発時はプロジェクトルートを返す。
    """
    if getattr(sys, "frozen", False):
        # PyInstaller exe 実行時
        return os.path.dirname(sys.executable)
    else:
        # 開発時: src/ の1つ上
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
