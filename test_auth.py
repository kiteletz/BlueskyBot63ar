from atproto import Client
import os

# 環境変数から認証情報を取得
handle = os.environ.get('BLUESKY_HANDLE')
pat = os.environ.get('BLUESKY_PAT')

# クライアント初期化とログイン
client = Client()
try:
    client.login(handle, pat)
    print("Authentication successful!")
    # 自分のプロフィールを取得（actorにハンドルを指定）
    profile = client.get_profile(actor=handle)
    print(f"Logged in as: {profile.display_name} (@{profile.handle})")
except Exception as e:
    print(f"Authentication failed: {e}")
