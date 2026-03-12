"""五十音ソート用ユーティリティ。"""

import unicodedata


def sort_key_japanese(name: str) -> str:
    """五十音ソート用のキーを生成する。

    カタカナをひらがなに統一し、NFKCで半角カナ等を正規化する。
    ASCII文字はそのまま（大文字小文字無視）。
    """
    # 全角カタカナ→ひらがなに変換
    normalized = ""
    for ch in name:
        cp = ord(ch)
        # カタカナ (U+30A1-U+30F6) → ひらがな (U+3041-U+3096)
        if 0x30A1 <= cp <= 0x30F6:
            normalized += chr(cp - 0x60)
        else:
            normalized += ch
    # NFKCで半角カナ等を正規化
    normalized = unicodedata.normalize("NFKC", normalized)
    return normalized.lower()
