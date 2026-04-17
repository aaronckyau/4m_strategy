"""
hash_password.py — 一次性工具：生成 bcrypt hash 並更新 .env
用法：
  cd Aurum_Infinity_AI
  python hash_password.py
"""
import getpass
import bcrypt
import os
import re
from pathlib import Path


def main():
    print("=== 生成 Admin 密碼 Hash ===\n")
    password = getpass.getpass("請輸入新的 Admin 密碼: ")
    confirm  = getpass.getpass("再次輸入確認: ")

    if password != confirm:
        print("ERROR: 兩次密碼不一致，請重新執行。")
        return

    if len(password) < 8:
        print("ERROR: 密碼長度至少 8 個字元。")
        return

    hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=12))
    hash_str = hashed.decode('utf-8')

    print(f"\n生成的 bcrypt hash：\n{hash_str}\n")

    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        content = env_path.read_text(encoding='utf-8')
        if 'ADMIN_PASSWORD' in content:
            new_content = re.sub(
                r'^ADMIN_PASSWORD=.*$',
                f'ADMIN_PASSWORD={hash_str}',
                content,
                flags=re.MULTILINE,
            )
            env_path.write_text(new_content, encoding='utf-8')
            print(f".env 已自動更新 ADMIN_PASSWORD。")
        else:
            with open(env_path, 'a', encoding='utf-8') as f:
                f.write(f'\nADMIN_PASSWORD={hash_str}\n')
            print(f".env 已新增 ADMIN_PASSWORD。")
    else:
        print(f"找不到 .env，請手動將以下內容加入 .env：")
        print(f"ADMIN_PASSWORD={hash_str}")

    print("\n完成！請重啟 Flask 服務讓新密碼生效。")


if __name__ == '__main__':
    main()
