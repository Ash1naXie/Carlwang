#!/usr/bin/env python3
"""
TweetScout API 爬虫 - 项目推文 (临时方案 - 历史数据补充)
使用新的 Sorsa API: https://api.sorsa.io/v2
支持断点续爬，并行处理所有list
"""
import os
import sys
import requests
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# 添加项目根目录到路径
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

# TweetScout 新 API 配置
API_BASE_URL = "https://api.sorsa.io/v2"
API_KEY = os.environ.get("TWEETSCOUT_API_KEY", "")

# Project List IDs (从 config.json 的 list_ids_project)
PROJECT_LIST_IDS = [
    "1996848536520897010",
    "1996863048959820198",
    "1996887049027440697"
]

# 历史数据配置
HISTORY_DAYS = 1  # 补充1天历史（过去24小时）
MAX_PAGES_PER_BATCH = 50  # 最多50页，24小时内推文通常不会超过
PROGRESS_FILE = Path(__file__).parent / "progress_project.json"

def load_progress():
    """加载爬取进度"""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_progress(progress):
    """保存爬取进度"""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)

def fetch_tweets_batch(list_id, start_cursor=None, target_date=None, max_pages=50):
    """分批获取推文，支持断点续爬"""
    tweets = []
    cursor = start_cursor
    pages_fetched = 0

    for page in range(max_pages):
        try:
            url = f"{API_BASE_URL}/list-tweets"
            headers = {"Accept": "application/json", "ApiKey": API_KEY}
            params = {"list_id": list_id}
            if cursor:
                params["cursor"] = cursor

            logger.info(f"List {list_id} - 第 {page+1}/{max_pages} 页")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            page_tweets = data.get("tweets", [])

            if not page_tweets:
                logger.info(f"List {list_id} - 无更多数据")
                return tweets, None, True

            # 检查是否达到目标日期
            should_stop = False
            for tweet in page_tweets:
                tweet_time = datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y")
                tweet_time_utc = tweet_time.replace(tzinfo=None)

                if target_date and tweet_time_utc < target_date:
                    should_stop = True
                    break
                tweets.append(tweet)

            pages_fetched += 1

            if should_stop:
                logger.info(f"List {list_id} - 达到目标日期")
                return tweets, None, True

            cursor = data.get("next_cursor")
            if not cursor:
                logger.info(f"List {list_id} - 已到最后")
                return tweets, None, True

            time.sleep(0.5)

        except Exception as e:
            logger.error(f"List {list_id} - 第 {page+1} 页失败: {e}")
            return tweets, cursor, False

    logger.info(f"List {list_id} - 本批次完成 {pages_fetched} 页")
    return tweets, cursor, False

def process_and_save(api_data_list):
    """完整处理流程：映射-增强-入库"""
    try:
        # 1. 数据映射
        tweets = data_mapper.map_api_data_list_to_tweets(api_data_list)
        if not tweets:
            logger.warning("数据映射后无有效推文")
            return 0

        # 2. 提取用户数据
        users = []
        user_data_map = {}
        for api_data in api_data_list:
            user_data = api_data.get("user", {})
            if user_data:
                tweet_id = api_data.get("id_str")
                if tweet_id:
                    user_data_map[tweet_id] = user_data
                users.append(data_mapper.map_api_data_to_user(user_data))

        # 3. 提取引用关系
        quotations = quotation_extractor.extract_quotations_from_api_data(api_data_list)
        valid_quotations = quotation_extractor.filter_valid_quotations(quotations)

        # 4. 推文增强
        enriched_tweets = tweet_enricher.enrich_tweets(tweets, user_data_map)

        # 5. 保存用户
        saved_users = 0
        for user in users:
            try:
                user_dao.upsert_user(user)
                saved_users += 1
            except Exception as e:
                logger.error(f"保存用户失败: {e}")

        # 6. 保存推文（项目推文表）
        saved_tweets = 0
        for tweet in enriched_tweets:
            try:
                tweet_dao.upsert_tweet(tweet)
                saved_tweets += 1
            except Exception as e:
                logger.error(f"保存推文失败: {e}")

        # 7. 保存引用关系
        saved_quotations = 0
        for quotation in valid_quotations:
            try:
                quotation_dao.insert_quotation(quotation)
                saved_quotations += 1
            except Exception as e:
                logger.error(f"保存引用失败: {e}")

        logger.info(f"处理完成: 用户{saved_users}, 推文{saved_tweets}, 引用{saved_quotations}")
        return saved_tweets

    except Exception as e:
        logger.error(f"处理失败: {e}")
        return 0

def process_single_list(list_id, target_date, progress):
    """处理单个list（用于并行）"""
    logger.info(f"\n{'='*50}")
    logger.info(f"处理 List: {list_id}")

    list_progress = progress.get(list_id, {"cursor": None, "completed": False})

    if list_progress.get("completed"):
        logger.info(f"List {list_id} - 已完成，跳过")
        return {"list_id": list_id, "tweets": 0, "saved": 0, "completed": True, "cursor": None}

    cursor = list_progress.get("cursor")
    tweets, next_cursor, completed = fetch_tweets_batch(
        list_id, cursor, target_date, MAX_PAGES_PER_BATCH
    )

    logger.info(f"List {list_id} - 本批获取 {len(tweets)} 条推文")

    saved = 0
    if tweets:
        saved = process_and_save(tweets)
        logger.info(f"List {list_id} - 保存 {saved} 条推文")

    return {
        "list_id": list_id,
        "tweets": len(tweets),
        "saved": saved,
        "completed": completed,
        "cursor": next_cursor
    }

def main():
    """主函数 - 并行处理所有list"""
    logger.info("=" * 50)
    logger.info(f"TweetScout 项目推文历史补充 (过去{HISTORY_DAYS}天) - 并行模式")
    logger.info("=" * 50)

    target_date = datetime.utcnow() - timedelta(days=HISTORY_DAYS)
    logger.info(f"目标日期: {target_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    progress = load_progress()
    total_tweets = 0
    total_saved = 0

    # 并行处理所有list
    logger.info(f"开始并行处理 {len(PROJECT_LIST_IDS)} 个list...")

    with ThreadPoolExecutor(max_workers=len(PROJECT_LIST_IDS)) as executor:
        futures = {
            executor.submit(process_single_list, list_id, target_date, progress): list_id
            for list_id in PROJECT_LIST_IDS
        }

        for future in as_completed(futures):
            result = future.result()
            list_id = result["list_id"]

            total_tweets += result["tweets"]
            total_saved += result["saved"]

            # 更新进度
            progress[list_id] = {
                "cursor": result["cursor"],
                "completed": result["completed"],
                "last_update": datetime.utcnow().isoformat()
            }
            save_progress(progress)
            logger.info(f"List {list_id} - 进度已保存 (完成: {result['completed']})")

    logger.info("\n" + "=" * 50)
    logger.info(f"本次运行: 获取 {total_tweets} 条, 保存 {total_saved} 条")

    all_completed = all(p.get("completed", False) for p in progress.values())
    if all_completed:
        logger.info("✅ 所有list已完成历史补充")
    else:
        logger.info("⏸️  未完成，请再次运行继续补充")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
