import pandas as pd
import random
import time
import os
import re
import logging
import sys
from datetime import datetime, timezone
from atproto import Client, models

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# 環境変数
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
            image_path = str(row['image_path']) if pd.notna(row['image_path']) else ''
            post = {
                'text': str(row['text']),
                'hashtags': hashtags,
                'image_path': image_path
            }
            posts.append(post)
        random.shuffle(posts)
        return posts
    except Exception as e:
        logging.error(f"Error loading Excel: {e}")
        return []

def create_facets(text, hashtags):
    """ハッシュタグをファセットとして設定"""
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
    """画像付き（複数可）またはテキストのみ投稿"""
    try:
        full_text = text + ' ' + ' '.join([f"#{tag}" for tag in hashtags])
        facets = create_facets(full_text, hashtags)
        logging.info(f"Facets generated: {facets}")
        
        # 画像の有無をチェック
        if image_path and image_path.strip():
            image_paths = [p.strip() for p in image_path.split(',') if p.strip()]  # カンマ区切りで分割
            images = []
            for path in image_paths[:4]:  # Blueskyは最大4枚
                if os.path.exists(path):
                    with open(path, 'rb') as f:
                        img_data = f.read()
                    images.append((img_data, ''))
                else:
                    logging.warning(f"Image not found: {path}, skipping this image")
            if images:
                client.send_images(
                    text=full_text,
                    images=images,  # (データ, altテキスト)のリスト
                    facets=facets
                )
                logging.info(f"Posted with images: {full_text[:50]}... with {', '.join(image_paths[:4])}")
            else:
                # 画像が見つからない場合、テキストのみ投稿
                client.com.atproto.repo.create_record({
                    "repo": client.me.did,
                    "collection": "app.bsky.feed.post",
                    "record": {
                        "text": full_text,
                        "facets": facets,
                        "createdAt": datetime.now(timezone.utc).isoformat()
                    }
                })
                logging.info(f"Posted without images: {full_text[:50]}... (no valid images found)")
        else:
            # 画像パスが空の場合
            client.com.atproto.repo.create_record({
                "repo": client.me.did,
                "collection": "app.bsky.feed.post",
                "record": {
                    "text": full_text,
                    "facets": facets,
                    "createdAt": datetime.now(timezone.utc).isoformat()
                }
            })
            logging.info(f"Posted without images: {full_text[:50]}... (image_path empty)")
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