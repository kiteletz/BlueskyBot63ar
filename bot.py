import pandas as pd
import random
import time
import os
import re
import logging
import sys
from datetime import datetime, timezone  # 修正: timezoneを追加
from atproto import Client, models

# ログ設定（ファイルとターミナル両方に出力）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# 環境変数から認証情報を取得
HANDLE = os.environ.get('BLUESKY_HANDLE')
PAT = os.environ.get('BLUESKY_PAT')
EXCEL_FILE = 'posts.xlsx'

def load_posts():
    """Excelから投稿リストを読み込み、ランダムシャッフル"""
    try:
        df = pd.read_excel(EXCEL_FILE)
        posts = []
        for _, row in df.iterrows():
            hashtags = [tag.strip() for tag in str(row['hashtags']).split(',') if pd.notna(row['hashtags']) and tag.strip()]
            post = {
                'text': str(row['text']),
                'hashtags': hashtags,
                'image_path': str(row['image_path'])
            }
            posts.append(post)
        random.shuffle(posts)
        return posts
    except Exception as e:
        logging.error(f"Error loading Excel: {e}")
        return []

def create_facets(text, hashtags):
    """ハッシュタグをファセットとして設定（絵文字対応）"""
    facets = []
    full_text = text + ' ' + ' '.join([f"#{tag}" for tag in hashtags])
    logging.info(f"Full text: {full_text}")
    for tag in hashtags:
        tag_with_hash = f"#{tag}"
        matches = list(re.finditer(re.escape(tag_with_hash), full_text))
        for match in matches:
            start, end = match.span()
            byte_start = len(full_text[:start].encode('utf-8'))
            byte_end = len(full_text[:end].encode('utf-8'))
            facet = models.AppBskyRichtextFacet.Main(
                features=[
                    models.AppBskyRichtextFacet.Tag(tag=tag)
                ],
                index=models.AppBskyRichtextFacet.ByteSlice(
                    byte_start=byte_start,
                    byte_end=byte_end
                )
            )
            facets.append(facet)
            logging.info(f"Facet for {tag_with_hash}: byte_start={byte_start}, byte_end={byte_end}")
    return facets

def post_to_bluesky(client, text, hashtags, image_path):
    """画像付きまたはテキストのみ投稿（ハッシュタグをファセットでリンク化）"""
    try:
        full_text = text + ' ' + ' '.join([f"#{tag}" for tag in hashtags])
        facets = create_facets(full_text, hashtags)
        logging.info(f"Facets generated: {facets}")
        
        # 画像の有無をチェック
        if os.path.exists(image_path) and image_path.strip():
            with open(image_path, 'rb') as f:
                img_data = f.read()
            client.send_image(
                text=full_text,
                image=img_data,
                image_alt='',
                facets=facets
            )
            logging.info(f"Posted with image: {full_text[:50]}... with {image_path}")
        else:
            client.com.atproto.repo.create_record({
                "repo": client.me.did,
                "collection": "app.bsky.feed.post",
                "record": {
                    "text": full_text,
                    "facets": facets,
                    "createdAt": datetime.now(timezone.utc).isoformat()  # 修正: UTCでRFC-3339
                }
            })
            logging.info(f"Posted without image: {full_text[:50]}... (image_path: {image_path or 'empty'} not found, but posted anyway)")
        return True
    except Exception as e:
        logging.error(f"Error posting: {e}")
        return False

def update_excel(posted_text):
    """投稿済みデータをExcelから削除"""
    try:
        df = pd.read_excel(EXCEL_FILE)
        df = df[df['text'] != posted_text]
        df.to_excel(EXCEL_FILE, index=False)
        logging.info(f"Excel updated, removed: {posted_text[:50]}...")
    except Exception as e:
        logging.error(f"Error updating Excel: {e}")

def main():
    # Blueskyクライアント初期化
    client = Client()
    try:
        client.login(HANDLE, PAT)
        logging.info("Authenticated successfully")
    except Exception as e:
        logging.error(f"Authentication failed: {e}")
        return

    # 投稿リストを読み込む
    posts = load_posts()
    if not posts:
        logging.error("No posts found in Excel.")
        return

    # 1件投稿
    post = posts.pop(0)
    if post_to_bluesky(client, post['text'], post['hashtags'], post['image_path']):
        update_excel(post['text'])
        logging.info("Post successful")
    else:
        logging.error("Post failed")

if __name__ == "__main__":
    main()