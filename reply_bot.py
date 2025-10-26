# reply_bot.py
import os
import pandas as pd
import logging
from datetime import datetime, timedelta, timezone
from atproto import Client, models

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

HANDLE = os.environ.get('BLUESKY_HANDLE')
PAT = os.environ.get('BLUESKY_PAT')
REPLY_EXCEL = 'reply_texts.xlsx'

def create_facets(text, hashtags):
    """ハッシュタグをリンク化するfacetsを生成（bot.pyに準拠、UTF-8バイト長対応）"""
    facets = []
    for tag in hashtags:
        tag_with_hash = f"#{tag}"
        # UTF-8バイト長で位置を計算
        byte_start = len(text[:text.find(tag_with_hash)].encode('utf-8'))
        if byte_start != -1 and tag_with_hash in text:
            byte_end = byte_start + len(tag_with_hash.encode('utf-8'))
            facets.append({
                "$type": "app.bsky.richtext.facet",
                "index": {"byteStart": byte_start, "byteEnd": byte_end},
                "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": tag}]
            })
    return facets

def load_reply_texts():
    try:
        df = pd.read_excel(REPLY_EXCEL)
        reply_dict = {}
        for _, row in df.iterrows():
            key = str(row[0])  # A列
            reply_text = str(row[1]) if pd.notna(row[1]) else ""  # B列
            # B列が空（NaN、空文字列、空白のみ）の場合はスキップ
            if not reply_text.strip():
                continue
            hashtags = [tag.strip() for tag in str(row[3]).split(',') if pd.notna(row[3]) and tag.strip()]  # D列
            reply_dict[key] = {'text': reply_text, 'hashtags': hashtags}
        logging.info(f"Loaded {len(reply_dict)} reply texts with hashtags from {REPLY_EXCEL}")
        return reply_dict
    except FileNotFoundError:
        logging.error(f"{REPLY_EXCEL} not found. Please create it first.")
        return {}
    except Exception as e:
        logging.error(f"Error loading {REPLY_EXCEL}: {e}")
        return {}

def get_reply_text(reply_dict, post_first_line):
    if post_first_line in reply_dict:
        return reply_dict[post_first_line]['text'], reply_dict[post_first_line]['hashtags']
    logging.info(f"No match for '{post_first_line}', skipping")
    return None, None

def has_replied(client, post_uri, handle):
    try:
        thread = client.get_post_thread(uri=post_uri)
        for reply in thread.thread.replies:
            if reply.post.author.handle == handle:
                logging.info(f"Already replied to {post_uri} by {handle}")
                return True
        return False
    except Exception as e:
        logging.error(f"Error checking thread for {post_uri}: {e}")
        return False

def main():
    try:
        client = Client()
        client.login(HANDLE, PAT)
        logging.info(f"Authenticated successfully as {HANDLE}")

        reply_dict = load_reply_texts()
        if not reply_dict:
            logging.error("No reply texts available. Exiting.")
            return

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=10)
        feed = client.get_author_feed(actor=HANDLE, limit=100) #テスト用
        recent_posts = [
            post for post in feed.feed
            if datetime.fromisoformat(post.post.record.created_at.replace('Z', '+00:00')) >= cutoff_time
        ]

        if not recent_posts:
            logging.info("No posts found in the last 1 day.")
            return

        logging.info(f"Found {len(recent_posts)} posts in the last 1 day:")
        for i, post in enumerate(recent_posts, 1):
            text = post.post.record.text
            first_line = text.split('\n')[0]
            # 「」」が存在する場合、その位置まで切り取り
            quote_end = first_line.find('」')+1
            if quote_end != -1:
                first_line = first_line[:quote_end]
            # 20文字未満に制限
            first_line = first_line[:19]
            created_at = post.post.record.created_at
            uri = post.post.uri
            cid = post.post.cid
            
            # いいね数チェック
            likes = client.get_likes(uri=uri, limit=20)
            like_count = len(likes.likes)
            logging.info(f"Post {i} has {like_count} likes")
            if like_count < 1: #202510時点の仕様とする
                logging.info(f"Skipping post {i}: less than 1 likes") #
                continue
            
            # 返信済みチェック
            if has_replied(client, uri, HANDLE):
                logging.info(f"Skipping post {i}: already replied")
                continue
            
            # 返信テキストとハッシュタグを取得
            reply_text, hashtags = get_reply_text(reply_dict, first_line)
            if reply_text is None:
                logging.info(f"Skipping post {i}: no matching reply text")
                continue
            full_text = reply_text + ' ' + ' '.join([f"#{tag}" for tag in hashtags]) if hashtags else reply_text
            facets = create_facets(full_text, hashtags)
            
            record = {
                '$type': 'app.bsky.feed.post',
                'text': full_text,
                'reply': {
                    'parent': {'cid': cid, 'uri': uri},
                    'root': {'cid': cid, 'uri': uri}
                },
                'facets': facets if facets else [],
                'createdAt': datetime.now(timezone.utc).isoformat()
            }
#            logging.info(f"Record data: {record}")
            client.com.atproto.repo.create_record({
                "repo": client.me.did,
                "collection": "app.bsky.feed.post",
                "record": record
            })
#            logging.info(f"Posted reply to {uri}: {full_text}")
            
            logging.info(f"Post {i}:")
            logging.info(f"  Text (first line, <20 chars): {first_line}")
            logging.info(f"  Reply text: {full_text}")
#            logging.info(f"  Created At: {created_at} (UTC)")
#            logging.info(f"  CID: {cid}")
#            logging.info(f"  URI: {uri}")
            logging.info(f"  Likes: {like_count}")
#            logging.info(f"  Hashtags: {hashtags}")
            logging.info("-" * 60)

    except Exception as e:
        logging.error(f"Error: {e}")

if __name__ == "__main__":
    main()