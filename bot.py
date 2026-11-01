import pandas as pd
import os
import re
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
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
JST = ZoneInfo("Asia/Tokyo")

def load_next_post():
    """posts.xlsx を読み取り専用で読み込み、条件を満たす次の投稿を返す"""
    try:
        df = pd.read_excel(EXCEL_FILE)

        # 列名が想定通りか確認（必要に応じて調整）
        # 想定: text, image_path, hashtags, 日時列（D列）
        # D列を明示的に扱うため、列番号でもアクセスできるようにする
        if df.shape[1] < 4:
            logging.error("ExcelにD列（日時）がありません")
            return None

        now = datetime.now(JST)
        logging.info(f"Current time (JST): {now.strftime('%Y/%m/%d %H:%M')}")

        candidates = []

        for idx, row in df.iterrows():
            # D列（4番目の列、index=3）を取得
            raw_dt = row.iloc[3]

            if pd.isna(raw_dt):
                continue

            # 日時のパース
            try:
                if isinstance(raw_dt, datetime):
                    post_dt = raw_dt
                    # timezone情報がない場合はJSTとして扱う
                    if post_dt.tzinfo is None:
                        post_dt = post_dt.replace(tzinfo=JST)
                else:
                    # 文字列の場合
                    post_dt = datetime.strptime(str(raw_dt).strip(), "%Y/%m/%d %H:%M")
                    post_dt = post_dt.replace(tzinfo=JST)
            except Exception as e:
                logging.warning(f"日時パース失敗 (row {idx}): {raw_dt} / {e}")
                continue

            # 条件: 現在日時 < D列の日時
            if now < post_dt:
                hashtags = []
                if 'hashtags' in df.columns and pd.notna(row.get('hashtags')):
                    hashtags = [tag.strip() for tag in str(row['hashtags']).split(',') if tag.strip()]
                elif df.shape[1] > 2 and pd.notna(row.iloc[2]):
                    hashtags = [tag.strip() for tag in str(row.iloc[2]).split(',') if tag.strip()]

                image_path = ''
                if 'image_path' in df.columns and pd.notna(row.get('image_path')):
                    image_path = str(row['image_path'])
                elif df.shape[1] > 1 and pd.notna(row.iloc[1]):
                    image_path = str(row.iloc[1])

                text = str(row['text']) if 'text' in df.columns else str(row.iloc[0])

                candidates.append({
                    'text': text,
                    'hashtags': hashtags,
                    'image_path': image_path,
                    'scheduled_at': post_dt,
                    'row_index': idx
                })

        if not candidates:
            logging.info("条件を満たす投稿がありません。終了します。")
            return None

        # D列の日時が最も小さいものを選択
        candidates.sort(key=lambda x: x['scheduled_at'])
        selected = candidates[0]

        logging.info(f"Selected post scheduled at {selected['scheduled_at'].strftime('%Y/%m/%d %H:%M')}")
        logging.info(f"Text preview: {selected['text'][:50]}...")
        return selected

    except Exception as e:
        logging.error(f"Error loading Excel: {e}")
        return None

def create_facets(text, hashtags):
    """ハッシュタグとURLをファセットとして設定"""
    facets = []
    full_text = text + ' ' + ' '.join([f"#{tag}" for tag in hashtags]) if hashtags else text
    logging.info(f"Full text: {full_text}")

    # 1. ハッシュタグのファセット
    for tag in hashtags:
        tag_with_hash = f"#{tag}"
        matches = list(re.finditer(re.escape(tag_with_hash), full_text))
        for match in matches:
            start, end = match.span()
            byte_start = len(full_text[:start].encode('utf-8'))
            byte_end = len(full_text[:end].encode('utf-8'))
            facet = models.AppBskyRichtextFacet.Main(
                features=[models.AppBskyRichtextFacet.Tag(tag=tag)],
                index=models.AppBskyRichtextFacet.ByteSlice(
                    byte_start=byte_start,
                    byte_end=byte_end
                )
            )
            facets.append(facet)

    # 2. URLのファセット
    url_pattern = r'https?://[^\s]+'
    matches = list(re.finditer(url_pattern, full_text))
    for match in matches:
        url = match.group(0)
        start, end = match.span()
        byte_start = len(full_text[:start].encode('utf-8'))
        byte_end = len(full_text[:end].encode('utf-8'))
        facet = models.AppBskyRichtextFacet.Main(
            features=[models.AppBskyRichtextFacet.Link(uri=url)],
            index=models.AppBskyRichtextFacet.ByteSlice(
                byte_start=byte_start,
                byte_end=byte_end
            )
        )
        facets.append(facet)

    return facets

def post_to_bluesky(client, text, hashtags, image_path):
    """画像付き（複数可）またはテキストのみ投稿"""
    try:
        full_text = text + ' ' + ' '.join([f"#{tag}" for tag in hashtags]) if hashtags else text
        facets = create_facets(full_text, hashtags)
        logging.info(f"Facets generated: {facets}")

        if image_path and image_path.strip():
            image_paths = [p.strip() for p in image_path.split(',') if p.strip()]
            images = []
            for path in image_paths[:4]:
                if os.path.exists(path):
                    with open(path, 'rb') as f:
                        img_data = f.read()
                    images.append((img_data, ''))
                else:
                    logging.warning(f"Image not found: {path}, skipping this image")

            if images:
                client.send_images(
                    text=full_text,
                    images=images,
                    facets=facets
                )
                logging.info(f"Posted with images: {full_text[:50]}...")
            else:
                client.com.atproto.repo.create_record({
                    "repo": client.me.did,
                    "collection": "app.bsky.feed.post",
                    "record": {
                        "text": full_text,
                        "facets": facets,
                        "createdAt": datetime.now(JST).isoformat()
                    }
                })
                logging.info(f"Posted without images: {full_text[:50]}... (no valid images)")
        else:
            client.com.atproto.repo.create_record({
                "repo": client.me.did,
                "collection": "app.bsky.feed.post",
                "record": {
                    "text": full_text,
                    "facets": facets,
                    "createdAt": datetime.now(JST).isoformat()
                }
            })
            logging.info(f"Posted without images: {full_text[:50]}...")

        return True
    except Exception as e:
        logging.error(f"Error posting: {e}")
        return False

def main():
    client = Client()
    try:
        client.login(HANDLE, PAT)
        logging.info("Authenticated successfully")
    except Exception as e:
        logging.error(f"Authentication failed: {e}")
        return

    post = load_next_post()
    if not post:
        return

    if post_to_bluesky(client, post['text'], post['hashtags'], post['image_path']):
        logging.info("Post successful")
    else:
        logging.error("Post failed")

if __name__ == "__main__":
    main()