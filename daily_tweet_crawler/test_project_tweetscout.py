#!/usr/bin/env python3
"""
TweetScout API 测试脚本 - 项目推文
每个list只爬取1页，用于快速验证流程
"""
import os
import sys
import requests
import time
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.tweet_dao import tweet_dao
from src.database.user_dao import user_dao
from src.database.quotation_dao import quotation_dao
from src.utils.logger import get_logger
from src.utils.data_mapper import data_mapper
from src.utils.tweet_enricher import tweet_enricher
from src.utils.quotation_extractor import quotation_extractor

logger = get_logger(__name__)

API_BASE_URL = "https://api.sorsa.io/v2"
API_KEY = os.environ.get("TWEETSCOUT_API_KEY", "")

PROJECT_LIST_IDS = [
    "1996848536520897010"  # 只测试第一个list
]

def fetch_one_page(list_id):
    """获取单页推文"""
    try:
        url = f"{API_BASE_URL}/list-tweets"
        headers = {"Accept": "application/json", "ApiKey": API_KEY}
        params = {"list_id": list_id}

        logger.info(f"测试 List {list_id}...")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        tweets = data.get("tweets", [])
        logger.info(f"  获取 {len(tweets)} 条推文（只取前10条测试）")
        return tweets[:10]
    except Exception as e:
        logger.error(f"  失败: {e}")
        return []

def process_and_save(api_data_list):
    """完整处理流程"""
    try:
        tweets = data_mapper.map_api_data_list_to_tweets(api_data_list)
        if not tweets:
            return 0

        users = []
        user_data_map = {}
        for api_data in api_data_list:
            user_data = api_data.get("user", {})
            if user_data:
                tweet_id = api_data.get("id_str")
                if tweet_id:
                    user_data_map[tweet_id] = user_data
                users.append(data_mapper.map_api_data_to_user(user_data))

        quotations = quotation_extractor.extract_quotations_from_api_data(api_data_list)
        valid_quotations = quotation_extractor.filter_valid_quotations(quotations)
        enriched_tweets = tweet_enricher.enrich_tweets(tweets, user_data_map)

        saved_users = sum(1 for user in users if user_dao.upsert_user(user) or True)
        saved_tweets = sum(1 for tweet in enriched_tweets if tweet_dao.upsert_tweet(tweet) or True)
        saved_quotations = sum(1 for q in valid_quotations if quotation_dao.insert_quotation(q) or True)

        logger.info(f"  保存: 用户{saved_users}, 推文{saved_tweets}, 引用{saved_quotations}")
        return saved_tweets
    except Exception as e:
        logger.error(f"  处理失败: {e}")
        return 0

def main():
    logger.info("=" * 50)
    logger.info("项目推文测试 - 每个list爬取1页")
    logger.info("=" * 50)

    total_tweets = 0
    total_saved = 0

    for list_id in PROJECT_LIST_IDS:
        tweets = fetch_one_page(list_id)
        if tweets:
            saved = process_and_save(tweets)
            total_tweets += len(tweets)
            total_saved += saved
        time.sleep(0.5)

    logger.info("\n" + "=" * 50)
    logger.info(f"测试完成: 获取 {total_tweets} 条, 保存 {total_saved} 条")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
