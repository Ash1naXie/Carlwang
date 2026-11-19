"""
Twitter数据爬虫核心模块
"""
import json
import logging
import os
from pathlib import Path
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

# 后端可切换：默认使用 Twitter API (twitterapi)，设置环境变量 TWITTER_API_BACKEND=tweetscout 使用 TweetScout
# 为了向后兼容KOL推文爬取，现在默认使用 twitterapi
if os.getenv("TWITTER_API_BACKEND", "twitterapi").lower() == "tweetscout":
    from .api.twitter_api import twitter_api as selected_api_client
else:
    from .api.twitter_api_twitterapi import twitter_api as selected_api_client
from .database.tweet_dao import tweet_dao
from .database.user_dao import user_dao
from .database.quotation_dao import quotation_dao
from .utils.data_mapper import data_mapper
from .utils.config_manager import config
from .utils.logger import get_logger
from .utils.tweet_enricher import tweet_enricher
from .utils.simple_tweet_enricher import simple_tweet_enricher
from .utils.quotation_extractor import quotation_extractor
# from .utils.user_language_integration import UserLanguageIntegration  # 语言检测已禁用
from .models.tweet import Tweet
from .models.user import TwitterUser
# from .topic_engine import topic_engine  # 话题分析已移除
# from .kol_engine import kol_engine  # KOL分析已禁用
# Project/AI analysis is intentionally not imported by the crawler.

try:
    from .monitoring.reporter import safe_report, report_component_status, report_process_status
except Exception:  # 监控模块不可用时不影响爬虫主流程
    def safe_report(*args, **kwargs):
        return False
    def report_component_status(*args, **kwargs):
        return False
    def report_process_status(*args, **kwargs):
        return False


def _utc_text(value=None):
    value = value or datetime.utcnow()
    return value.strftime("%Y-%m-%d %H:%M:%S")

def _duration_seconds(started_at):
    return int((datetime.utcnow() - started_at).total_seconds())

def _component_code(process_code, list_id):
    if process_code == "db002_retail_core_lists":
        return f"retail_core_list_{list_id}"
    if process_code == "db002_alpha_address":
        return f"alpha_address_{list_id}"
    if process_code == "db002_global_media_watch":
        return f"global_media_list_{list_id}"
    return f"kol_list_{list_id}"

def _component_name(process_code, list_id):
    if process_code == "db002_retail_core_lists":
        return f"Retail Core {list_id}"
    if process_code == "db002_alpha_address":
        return f"Alpha Address {list_id}"
    if process_code == "db002_global_media_watch":
        return f"Global Media Watch {list_id}"
    return f"KOL List {list_id}"


LAST_SEEN_PROCESS_CODES = {
    "db002_retail_core_lists",
    "db002_kol_tweet_general",
    "db002_global_media_watch",
}
LAST_SEEN_STATE_FILE = Path(
    os.getenv(
        "TWITTER_LAST_SEEN_STATE_FILE",
        str(Path(__file__).resolve().parent.parent / "daily_kol_tweet_crawler" / "last_seen_tweet_ids.json"),
    )
)


def _tweet_id_value_from_api_data(tweet: Dict[str, Any]) -> Optional[str]:
    for key in ("id_str", "id", "tweet_id"):
        value = tweet.get(key)
        if value is not None:
            value = str(value).strip()
            if value:
                return value
    return None


class TwitterCrawler:
    """Twitter数据爬虫"""
    
    def __init__(self):
        """初始化爬虫"""
        self.logger = get_logger(__name__)
        self.api_client = selected_api_client
        self.tweet_dao = tweet_dao
        self.user_dao = user_dao
        self.quotation_dao = quotation_dao
        self.data_mapper = data_mapper
        # self.topic_engine = topic_engine  # 话题分析已移除
        # self.kol_engine = kol_engine  # KOL分析已禁用
        self.project_engine = None
        self.tweet_enricher = tweet_enricher
        self.quotation_extractor = quotation_extractor
        self._last_seen_lock = threading.Lock()
        self._last_seen_candidates = {}

        # 爬取统计
        self.crawl_count = 0
        self.success_count = 0
        self.error_count = 0
        self.last_crawl_time = None
        
        self.logger.info("Twitter爬虫初始化完成")

    def _last_seen_enabled(self, process_code: Optional[str]) -> bool:
        return bool(process_code and process_code in LAST_SEEN_PROCESS_CODES)

    def _last_seen_key(self, process_code: str, list_id: str) -> str:
        return f"{process_code}:{list_id}"

    def _load_last_seen_state(self) -> Dict[str, Any]:
        if not LAST_SEEN_STATE_FILE.exists():
            return {}
        with LAST_SEEN_STATE_FILE.open("r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            raise ValueError(f"last_seen 状态文件不是 JSON 对象: {LAST_SEEN_STATE_FILE}")
        return state

    def _write_last_seen_state(self, state: Dict[str, Any]) -> None:
        LAST_SEEN_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        temp_file = LAST_SEEN_STATE_FILE.with_suffix(".tmp")
        with temp_file.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
        temp_file.replace(LAST_SEEN_STATE_FILE)

    def _get_last_seen_tweet_id(self, process_code: Optional[str], list_id: str) -> Optional[str]:
        if not self._last_seen_enabled(process_code):
            return None
        state = self._load_last_seen_state()
        value = state.get(self._last_seen_key(process_code, list_id))
        if isinstance(value, dict):
            value = value.get("tweet_id")
        value = str(value).strip() if value else None
        if value:
            self.logger.info(f"last_seen checkpoint: process_code={process_code}, list_id={list_id}, tweet_id={value}")
        return value

    def _remember_last_seen_candidate(self, process_code: Optional[str], list_id: str, api_data: List[Dict[str, Any]]) -> None:
        if not self._last_seen_enabled(process_code) or not api_data:
            return
        newest_tweet_id = _tweet_id_value_from_api_data(api_data[0])
        if not newest_tweet_id:
            return
        with self._last_seen_lock:
            self._last_seen_candidates[list_id] = newest_tweet_id

    def _commit_last_seen_candidates(self, process_code: Optional[str]) -> None:
        if not self._last_seen_enabled(process_code):
            return
        with self._last_seen_lock:
            candidates = dict(self._last_seen_candidates)
        if not candidates:
            return

        state = self._load_last_seen_state()
        updated_at = _utc_text()
        for list_id, tweet_id in candidates.items():
            state[self._last_seen_key(process_code, list_id)] = {
                "tweet_id": tweet_id,
                "updated_at_utc": updated_at,
                "process_code": process_code,
                "list_id": list_id,
            }
        self._write_last_seen_state(state)
        self.logger.info(f"last_seen checkpoint 已更新: process_code={process_code}, lists={len(candidates)}, file={LAST_SEEN_STATE_FILE}")
    
    def crawl_tweets(self, list_id: str = None, list_ids: List[str] = None, max_pages: int = None, 
                    page_size: int = None, hours_limit: int = 2, process_code: str = None,
                    execution_id: str = None) -> bool:
        """
        爬取推文数据
        
        Args:
            list_id: 单个列表ID，如果不指定则使用配置中的默认值（向后兼容）
            list_ids: 多个列表ID列表，优先级高于list_id
            max_pages: 最大页数（不超过15页）
            page_size: 每页大小
            hours_limit: 时间限制（小时），只拉取过去N小时的推文，默认2小时（生产环境使用UTC时间）
            
        Returns:
            是否成功
        """
        self.crawl_count += 1
        self.last_crawl_time = datetime.now()
        self.last_saved_tweet_count = 0
        with self._last_seen_lock:
            self._last_seen_candidates = {}
        started_at = datetime.utcnow()
        if process_code:
            safe_report(
                report_process_status,
                process_code,
                "running",
                execution_id=execution_id,
                start_time=_utc_text(started_at),
            )
        
        try:
            self.logger.info(f"开始爬取推文数据 (第 {self.crawl_count} 次，时间限制: {hours_limit}小时)")
            
            # 1. 从API获取数据（支持多个list并行处理，最多15页，只拉取过去8小时）
            api_data_list = self._fetch_api_data(
                list_id, list_ids, max_pages, page_size, hours_limit,
                process_code=process_code,
                execution_id=execution_id,
            )
            
            if not api_data_list:
                api_stats = self.api_client.get_request_stats()
                if api_stats.get('total_requests', 0) > 0 and api_stats.get('error_count', 0) == 0:
                    self.logger.info("API请求成功，时间窗口内没有新推文，任务视为成功")
                    self._commit_last_seen_candidates(process_code)
                    self.success_count += 1
                    if process_code:
                        safe_report(report_process_status, process_code, "success", execution_id=execution_id, start_time=_utc_text(started_at), end_time=_utc_text(), duration=_duration_seconds(started_at), total_records=0)
                    return True

                self.logger.warning("未获取到任何API数据")
                self.error_count += 1
                if process_code:
                    safe_report(report_process_status, process_code, "failed", execution_id=execution_id, start_time=_utc_text(started_at), end_time=_utc_text(), duration=_duration_seconds(started_at), error_message="未获取到任何API数据")
                return False
            
            self.logger.info(f"从API获取到 {len(api_data_list)} 条原始数据")
            
            # 2. 数据映射和转换
            tweets = self._map_data_to_tweets(api_data_list)
            
            if not tweets:
                self.logger.warning("数据映射后没有有效的推文数据")
                self.error_count += 1
                if process_code:
                    safe_report(report_process_status, process_code, "failed", execution_id=execution_id, start_time=_utc_text(started_at), end_time=_utc_text(), duration=_duration_seconds(started_at), error_message="数据映射后没有有效的推文数据")
                return False
            
            self.logger.info(f"成功映射 {len(tweets)} 条推文数据")
            
            # 2.1 提取用户数据
            users = self._extract_users_from_api_data(api_data_list)
            self.logger.info(f"成功提取 {len(users)} 条用户数据")
            
            # 2.2 构建用户数据映射（用于推文增强）
            # 建立 tweet_id -> user_data 的映射关系
            user_data_map = {}
            for api_data in api_data_list:
                try:
                    tweet_id = api_data.get('id_str')
                    # 兼容两种API：TweetScout使用'user'，Twitter API使用'author'
                    user_data = api_data.get('user') or api_data.get('author')
                    if tweet_id and user_data and isinstance(user_data, dict):
                        user_data_map[tweet_id] = user_data
                except Exception as e:
                    self.logger.warning(f"构建用户数据映射失败: {e}")
                    continue
            
            is_predx_raw = self.tweet_dao.table_name == 'predx_raw_tweets'

            # 2.3 提取引用关系数据
            if is_predx_raw:
                valid_quotations = []
                self.logger.info("PredX raw 模式跳过用户/引用关系落库，只写 predx_raw_tweets")
            else:
                self.logger.info("开始提取引用关系数据...")
                quotations = self.quotation_extractor.extract_quotations_from_api_data(api_data_list)
                valid_quotations = self.quotation_extractor.filter_valid_quotations(quotations)
                self.logger.info(f"引用关系数据提取完成，获得 {len(valid_quotations)} 条有效引用关系")
            
            # 2.4 增强推文数据（添加 kol_id 和 entity_id）
            if is_predx_raw:
                enriched_tweets = tweets
                self.logger.info(f"PredX raw 模式跳过旧 KOL/项目增强，处理了 {len(enriched_tweets)} 条推文")
            else:
                self.logger.info("开始增强推文数据...")
                enriched_tweets = self.tweet_enricher.enrich_tweets(tweets, user_data_map)
                self.logger.info(f"推文增强完成，处理了 {len(enriched_tweets)} 条推文")
            
            # 3. 存储到数据库
            # 先保存用户数据（因为推文可能引用用户）
            if users and not is_predx_raw:
                user_saved_count = self._save_users_to_database(users)
                self.logger.info(f"成功保存 {user_saved_count} 条用户数据")
            
            # 然后保存增强后的推文数据
            tweet_saved_count = self._save_tweets_to_database(enriched_tweets)
            self.last_saved_tweet_count = tweet_saved_count
            storable_tweet_count = self.tweet_dao.count_storable_tweets(enriched_tweets)
            
            # 保存引用关系数据
            quotation_saved_count = 0
            if valid_quotations:
                quotation_saved_count = self._save_quotations_to_database(valid_quotations)
                self.logger.info(f"成功保存 {quotation_saved_count} 条引用关系数据")
            
            # 4. 数据保存完成
            if storable_tweet_count == 0:
                self.logger.info("本轮没有需要入库的有效推文，任务视为成功")
                self._commit_last_seen_candidates(process_code)
                self.success_count += 1
                if process_code:
                    safe_report(report_process_status, process_code, "success", execution_id=execution_id, start_time=_utc_text(started_at), end_time=_utc_text(), duration=_duration_seconds(started_at), total_records=0)
                return True

            if tweet_saved_count > 0:
                self.logger.info(f"成功保存 {tweet_saved_count} 条推文到数据库")
                
                # 话题分析已移除 - 在其他独立脚本中处理
                # try:
                #     self.logger.info("开始进行话题分析...")
                #     topic_success = self.topic_engine.analyze_recent_tweets(hours=1, max_tweets=50)
                #     
                #     if topic_success:
                #         self.logger.info("话题分析完成")
                #     else:
                #         self.logger.warning("话题分析失败，但不影响主流程")
                #         
                # except Exception as e:
                #     self.logger.error(f"话题分析异常: {e}")
                
                # KOL分析已禁用
                # try:
                #     self.logger.info("开始进行KOL分析...")
                #     kol_success = self.kol_engine.analyze_all_users_as_kols(min_followers=50000, max_users=10)
                #     
                #     if kol_success:
                #         self.logger.info("KOL分析完成")
                #     else:
                #         self.logger.warning("KOL分析失败，但不影响主流程")
                #         
                # except Exception as e:
                #     self.logger.error(f"KOL分析异常: {e}")
                
                # 爬虫阶段只负责抓取、去重、入库和基础字段清洗，不执行 AI 项目分析或活动结构化。
                self.logger.info("跳过项目分析和活动结构化：推文爬虫保持纯抓取链路")

                self._commit_last_seen_candidates(process_code)
                self.success_count += 1
                if process_code:
                    safe_report(report_process_status, process_code, "success", execution_id=execution_id, start_time=_utc_text(started_at), end_time=_utc_text(), duration=_duration_seconds(started_at), total_records=tweet_saved_count)
                return True
            else:
                self.logger.error("保存推文到数据库失败")
                self.error_count += 1
                if process_code:
                    safe_report(report_process_status, process_code, "failed", execution_id=execution_id, start_time=_utc_text(started_at), end_time=_utc_text(), duration=_duration_seconds(started_at), error_message="保存推文到数据库失败")
                return False
                
        except Exception as e:
            self.logger.error(f"爬取推文数据异常: {e}")
            self.error_count += 1
            if process_code:
                safe_report(report_process_status, process_code, "failed", execution_id=execution_id, start_time=_utc_text(started_at), end_time=_utc_text(), duration=_duration_seconds(started_at), error_message=str(e))
            return False
    

    def _tag_api_data_source(self, api_data: List[Dict[str, Any]], list_id: str) -> List[Dict[str, Any]]:
        """标记来源 List，供 predx_raw_tweets 保留原始来源。"""
        for item in api_data or []:
            if isinstance(item, dict):
                item['_source_list_id'] = list_id
        return api_data

    def _fetch_api_data(self, list_id: str = None, list_ids: List[str] = None, max_pages: int = None, 
                       page_size: int = None, hours_limit: int = 2, process_code: str = None,
                       execution_id: str = None) -> List[Dict[str, Any]]:
        """
        从API获取数据（支持并行获取多个list）
        
        Args:
            list_id: 单个列表ID（向后兼容）
            list_ids: 多个列表ID列表，优先级高于list_id
            max_pages: 最大页数（不超过15页）
            page_size: 每页大小
            hours_limit: 时间限制（小时），只拉取过去N小时的推文
            
        Returns:
            API数据列表
        """
        try:
            # 确定要使用的list_ids列表
            if list_ids is not None:
                # 优先使用传入的list_ids
                target_list_ids = list_ids
            elif list_id is not None:
                # 向后兼容：如果只传入了单个list_id，转换为列表
                target_list_ids = [list_id]
            else:
                # 使用配置文件中的默认值
                target_list_ids = config.get('api.default_params.list_ids', [config.get('api.default_params.list_id')])
            
            self.logger.info(f"正在并行获取 {len(target_list_ids)} 个list的数据: {target_list_ids}, 时间限制: {hours_limit}小时")
            
            # 如果只有一个list，使用原有的串行逻辑
            if len(target_list_ids) == 1:
                list_id_single = target_list_ids[0]
                self.logger.info(f"单个list模式，list_id: {list_id_single}")
                
                api_data_list = self._fetch_single_list_with_monitor(
                    list_id_single,
                    max_pages=max_pages,
                    page_size=page_size,
                    hours_limit=hours_limit,
                    process_code=process_code,
                    execution_id=execution_id,
                )
                
                api_data_list = self._tag_api_data_source(api_data_list, list_id_single)

                # 获取API请求统计
                stats = self.api_client.get_request_stats()
                self.logger.info(f"API请求统计: {stats}")
                
                return api_data_list
            
            # 多个list的并行处理逻辑
            import concurrent.futures
            
            all_api_data = []
            
            # 使用线程池并行获取多个list的数据
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(target_list_ids)) as executor:
                # 提交所有任务
                future_to_list_id = {
                    executor.submit(
                        self._fetch_single_list_with_monitor,
                        single_list_id,
                        max_pages=max_pages,
                        page_size=page_size,
                        hours_limit=hours_limit,
                        process_code=process_code,
                        execution_id=execution_id,
                    ): single_list_id for single_list_id in target_list_ids
                }
                
                # 收集结果
                for future in concurrent.futures.as_completed(future_to_list_id):
                    single_list_id = future_to_list_id[future]
                    try:
                        api_data = future.result()
                        if api_data:
                            self.logger.info(f"list_id {single_list_id} 获取到 {len(api_data)} 条数据")
                            all_api_data.extend(self._tag_api_data_source(api_data, single_list_id))
                        else:
                            self.logger.warning(f"list_id {single_list_id} 未获取到数据")
                    except Exception as e:
                        self.logger.error(f"获取 list_id {single_list_id} 数据失败: {e}")
            
            # 获取API请求统计
            stats = self.api_client.get_request_stats()
            self.logger.info(f"并行API请求统计: {stats}")
            self.logger.info(f"总共获取到 {len(all_api_data)} 条API数据")
            
            return all_api_data
            
        except Exception as e:
            self.logger.error(f"从API获取数据失败: {e}")
            return []
    
    def _fetch_single_list_with_monitor(self, list_id: str, max_pages: int = None, page_size: int = None,
                                        hours_limit: int = 2, process_code: str = None,
                                        execution_id: str = None) -> List[Dict[str, Any]]:
        started_at = datetime.utcnow()
        component_code = _component_code(process_code, list_id) if process_code else None
        component_name = _component_name(process_code, list_id) if process_code else None
        if process_code:
            safe_report(
                report_component_status,
                process_code,
                component_code,
                component_name,
                "running",
                execution_id=execution_id,
                start_time=_utc_text(started_at),
                last_heartbeat_at=_utc_text(started_at),
            )
        try:
            last_seen_tweet_id = self._get_last_seen_tweet_id(process_code, list_id)
            api_data = self.api_client.fetch_all_tweets(
                list_id=list_id,
                max_pages=max_pages,
                page_size=page_size,
                hours_limit=hours_limit,
                last_seen_tweet_id=last_seen_tweet_id,
            )
            self._remember_last_seen_candidate(process_code, list_id, api_data)
            if process_code:
                safe_report(
                    report_component_status,
                    process_code,
                    component_code,
                    component_name,
                    "success",
                    execution_id=execution_id,
                    start_time=_utc_text(started_at),
                    end_time=_utc_text(),
                    duration=_duration_seconds(started_at),
                    total_records=len(api_data or []),
                    success_count=1,
                    failed_count=0,
                    last_heartbeat_at=_utc_text(),
                )
            return api_data
        except Exception as exc:
            if process_code:
                safe_report(
                    report_component_status,
                    process_code,
                    component_code,
                    component_name,
                    "failed",
                    execution_id=execution_id,
                    start_time=_utc_text(started_at),
                    end_time=_utc_text(),
                    duration=_duration_seconds(started_at),
                    total_records=0,
                    success_count=0,
                    failed_count=1,
                    error_message=str(exc),
                    last_heartbeat_at=_utc_text(),
                )
            raise

    def _map_data_to_tweets(self, api_data_list: List[Dict[str, Any]]) -> List[Tweet]:
        """
        将API数据映射为Tweet对象
        
        Args:
            api_data_list: API数据列表
            
        Returns:
            Tweet对象列表
        """
        try:
            self.logger.info("开始映射API数据到Tweet对象...")
            
            tweets = self.data_mapper.map_api_data_list_to_tweets(api_data_list)
            source_by_id = {}
            for item in api_data_list:
                if not isinstance(item, dict):
                    continue
                item_id = item.get('id_str') or item.get('id') or item.get('tweet_id') or item.get('rest_id')
                if item_id:
                    source_by_id[str(item_id)] = item

            for tweet in tweets:
                item = source_by_id.get(str(tweet.id_str), {})
                user_data = item.get('user') or item.get('author') or {}
                if not isinstance(user_data, dict):
                    user_data = {}
                tweet.source_job_id = 'predx_raw_lists'
                tweet.raw_json = {
                    'source_list_id': item.get('_source_list_id'),
                    'tweet_id': tweet.id_str,
                    'author': user_data,
                }
                tweet.author_handle = (
                    user_data.get('screen_name') or user_data.get('username') or
                    user_data.get('userName') or tweet.user_name
                )
                tweet.author_name = user_data.get('name') or user_data.get('display_name') or tweet.user_name
                tweet.followers_count = (
                    user_data.get('followers_count') or user_data.get('followers') or
                    user_data.get('followersCount') or 0
                )
                tweet.lang = item.get('lang') or item.get('language')

            # 数据去重（基于id_str）
            unique_tweets = self._deduplicate_tweets(tweets)
            
            if len(unique_tweets) != len(tweets):
                self.logger.info(f"去重后剩余 {len(unique_tweets)} 条推文（原 {len(tweets)} 条）")
            
            return unique_tweets
            
        except Exception as e:
            self.logger.error(f"数据映射失败: {e}")
            return []
    
    def _deduplicate_tweets(self, tweets: List[Tweet]) -> List[Tweet]:
        """
        推文去重
        
        Args:
            tweets: 推文列表
            
        Returns:
            去重后的推文列表
        """
        seen_ids = set()
        unique_tweets = []
        
        for tweet in tweets:
            if tweet.id_str not in seen_ids:
                seen_ids.add(tweet.id_str)
                unique_tweets.append(tweet)
        
        return unique_tweets
    
    def _extract_users_from_api_data(self, api_data_list: List[Dict[str, Any]]) -> List[TwitterUser]:
        """
        从API数据中提取用户信息
        
        Args:
            api_data_list: API数据列表
            
        Returns:
            用户对象列表
        """
        try:
            self.logger.info("开始从API数据中提取用户信息...")
            
            users = self.data_mapper.extract_users_from_tweets(api_data_list)
            
            # 用户去重（基于id_str）
            unique_users = self._deduplicate_users(users)
            
            if len(unique_users) != len(users):
                self.logger.info(f"用户去重后剩余 {len(unique_users)} 个用户（原 {len(users)} 个）")
            
            return unique_users
            
        except Exception as e:
            self.logger.error(f"提取用户数据失败: {e}")
            return []
    
    def _deduplicate_users(self, users: List[TwitterUser]) -> List[TwitterUser]:
        """
        用户去重
        
        Args:
            users: 用户列表
            
        Returns:
            去重后的用户列表
        """
        seen_ids = set()
        unique_users = []
        
        for user in users:
            if user.id_str not in seen_ids:
                seen_ids.add(user.id_str)
                unique_users.append(user)
        
        return unique_users
    
    def _extract_user_data_from_api(self, api_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        从API数据中提取原始用户数据（用于推文增强）
        
        Args:
            api_data_list: API原始数据列表
            
        Returns:
            原始用户数据列表
        """
        user_data_list = []
        
        for api_data in api_data_list:
            try:
                # 从tweet数据中提取user信息
                user_data = api_data.get('user')
                if user_data and isinstance(user_data, dict):
                    user_data_list.append(user_data)
                    
            except Exception as e:
                self.logger.warning(f"提取用户数据失败: {e}")
                continue
        
        return user_data_list
    
    def _save_users_to_database(self, users: List[TwitterUser]) -> int:
        """
        保存用户到数据库

        Args:
            users: 用户列表

        Returns:
            成功保存的数量
        """
        try:
            self.logger.info(f"开始保存 {len(users)} 条用户到数据库...")

            # 语言检测已禁用，直接批量保存用户数据
            # enhanced_users = self._enhance_users_with_language(users)
            saved_count = self.user_dao.batch_upsert_users(users)

            return saved_count

        except Exception as e:
            self.logger.error(f"保存用户到数据库失败: {e}")
            return 0
    
    # def _enhance_users_with_language(self, users: List[TwitterUser]) -> List[TwitterUser]:
    #     """
    #     为用户列表添加语言检测信息（已禁用）
    #
    #     Args:
    #         users: 用户列表
    #
    #     Returns:
    #         增强后的用户列表
    #     """
    #     try:
    #         self.logger.info(f"开始为 {len(users)} 个用户进行语言检测...")
    #
    #         # 使用批量语言检测
    #         enhanced_users = self.user_language_integration.enhance_users_batch(
    #             users=users,
    #             use_ai_fallback=False  # 暂时不使用AI辅助，避免过多API调用
    #         )
    #
    #         self.logger.info(f"完成语言检测，增强了 {len(enhanced_users)} 个用户")
    #         return enhanced_users
    #
    #     except Exception as e:
    #         self.logger.error(f"用户语言检测失败: {e}")
    #         # 如果语言检测失败，设置默认语言并返回原用户列表
    #         for user in users:
    #             if not hasattr(user, 'language') or user.language is None:
    #                 user.language = "English"  # 默认设为English
    #         return users
    
    def _save_tweets_to_database(self, tweets: List[Tweet]) -> int:
        """
        保存推文到数据库
        
        Args:
            tweets: 推文列表
            
        Returns:
            成功保存的数量
        """
        try:
            self.logger.info(f"开始保存 {len(tweets)} 条推文到数据库...")
            
            # 批量保存
            saved_count = self.tweet_dao.batch_upsert_tweets(tweets)
            
            return saved_count
            
        except Exception as e:
            self.logger.error(f"保存推文到数据库失败: {e}")
            return 0
    
    def _save_quotations_to_database(self, quotations: List[Dict[str, Any]]) -> int:
        """
        保存引用关系到数据库
        
        Args:
            quotations: 引用关系数据列表
            
        Returns:
            成功保存的数量
        """
        try:
            if not quotations:
                self.logger.info("没有引用关系数据需要保存")
                return 0
            
            # 确保数据表存在
            if not self.quotation_dao.create_table_if_not_exists():
                self.logger.error("创建引用关系表失败")
                return 0
            
            self.logger.info(f"开始保存 {len(quotations)} 条引用关系到数据库...")
            
            # 批量保存
            saved_count = self.quotation_dao.batch_insert_quotations(quotations)
            
            return saved_count
            
        except Exception as e:
            self.logger.error(f"保存引用关系到数据库失败: {e}")
            return 0

    def _detect_and_structure_activities(self, tweets: List[Tweet]) -> bool:
        """活动结构化依赖 AI，推文爬虫链路已禁用。"""
        raise RuntimeError("活动结构化已禁用：推文爬虫只做抓取、去重、入库、基础字段清洗")

    def _update_tweet_activity_status(self, tweet_id: str, is_activity: int,
                                     activity_detail: str) -> bool:
        """
        更新推文的活动状态

        Args:
            tweet_id: 推文ID
            is_activity: 是否为活动推文（0或1）
            activity_detail: 活动详情（JSON字符串）

        Returns:
            是否成功
        """
        try:
            table_name = self.tweet_dao.db_manager.db_config.get('tables', {}).get('tweet', 'twitter_tweet')

            sql = f"""
            UPDATE {table_name}
            SET is_activity = %s, activity_detail = %s
            WHERE id_str = %s
            """

            affected_rows = self.tweet_dao.db_manager.execute_update(
                sql, (is_activity, activity_detail, tweet_id)
            )

            return affected_rows > 0

        except Exception as e:
            self.logger.error(f"更新推文活动状态失败 {tweet_id}: {e}")
            return False

    def test_connection(self) -> bool:
        """
        测试数据库连接
        
        Returns:
            连接是否成功
        """
        try:
            self.logger.info("测试数据库连接...")
            success = self.tweet_dao.db_manager.test_connection()
            
            if success:
                self.logger.info("数据库连接测试成功")
            else:
                self.logger.error("数据库连接测试失败")
            
            return success
            
        except Exception as e:
            self.logger.error(f"数据库连接测试异常: {e}")
            return False
    
    def test_api_connection(self) -> bool:
        """
        测试API连接
        
        Returns:
            API连接是否成功
        """
        try:
            self.logger.info("测试API连接...")
            
            # 尝试获取少量数据
            test_data, _ = self.api_client.fetch_tweets(count=1)
            
            success = test_data is not None and len(test_data) >= 0
            
            if success:
                self.logger.info("API连接测试成功")
            else:
                self.logger.error("API连接测试失败")
            
            return success
            
        except Exception as e:
            self.logger.error(f"API连接测试异常: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取爬虫统计信息
        
        Returns:
            统计信息字典
        """
        return {
            'crawl_count': self.crawl_count,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'success_rate': (self.success_count / max(self.crawl_count, 1)) * 100,
            'last_crawl_time': self.last_crawl_time.isoformat() if self.last_crawl_time else None,
            'api_stats': self.api_client.get_request_stats(),
            'database_tweet_count': self.tweet_dao.get_tweet_count(),
            'database_user_count': self.user_dao.get_user_count(),
            'database_quotation_count': self.quotation_dao.get_quotation_count(),
            # 'topic_stats': self.topic_engine.get_topic_statistics(),  # 话题分析已移除
            # 'kol_stats': self.kol_engine.get_kol_statistics(),  # KOL分析已禁用
            # 项目分析已从推文爬虫链路移除。
        }
    
    def reset_statistics(self) -> None:
        """重置统计信息"""
        self.crawl_count = 0
        self.success_count = 0
        self.error_count = 0
        self.last_crawl_time = None
        self.api_client.reset_stats()
        self.logger.info("爬虫统计信息已重置")
    
    def crawl_project_tweets(self, max_pages: int = None, page_size: int = None, hours_limit: int = 2) -> bool:
        """
        爬取项目推文数据（简化流程版本）

        Args:
            max_pages: 最大页数（不超过15页）
            page_size: 每页大小
            hours_limit: 时间限制（小时），只拉取过去N小时的推文，默认2小时

        Returns:
            是否成功
        """
        self.crawl_count += 1
        self.last_crawl_time = datetime.now()

        try:
            # 重置API统计信息（避免累计）
            self.api_client.reset_stats()

            self.logger.info(f"开始爬取项目推文数据 (第 {self.crawl_count} 次，时间限制: {hours_limit}小时)")
            
            # 1. 从API获取数据（使用配置中的list_ids_project）
            list_ids_project = config.get('api.default_params.list_ids_project', [])
            if not list_ids_project:
                self.logger.error("配置文件中未找到list_ids_project")
                self.error_count += 1
                return False
            
            self.logger.info(f"使用项目列表IDs: {list_ids_project}")
            
            api_data_list = self._fetch_api_data(None, list_ids_project, max_pages, page_size, hours_limit)
            
            if not api_data_list:
                self.logger.warning("未获取到任何项目推文API数据")
                self.error_count += 1
                return False
            
            self.logger.info(f"从API获取到 {len(api_data_list)} 条项目推文原始数据")
            
            # 2. 数据映射和转换（简化版本）
            tweets = self._map_data_to_tweets(api_data_list)
            
            if not tweets:
                self.logger.warning("项目推文数据映射后没有有效的数据")
                self.error_count += 1
                return False
            
            self.logger.info(f"成功映射 {len(tweets)} 条项目推文数据")
            
            # 2.1 提取用户数据
            users = self._extract_users_from_api_data(api_data_list)
            self.logger.info(f"成功提取 {len(users)} 条用户数据")
            
            # 2.2 构建用户数据映射（用于推文增强）
            user_data_map = {}
            for api_data in api_data_list:
                try:
                    tweet_id = api_data.get('id_str')
                    # 兼容 'user' 和 'author' 两种字段（新接口使用 author）
                    user_data = api_data.get('user') or api_data.get('author')
                    if tweet_id and user_data and isinstance(user_data, dict):
                        user_data_map[tweet_id] = user_data
                except Exception as e:
                    self.logger.warning(f"构建用户数据映射失败: {e}")
                    continue
            
            # 2.3 项目推文极简化增强（使用专门的简化增强器，无复杂数据加载）
            self.logger.info("开始项目推文极简化增强...")
            enriched_tweets = simple_tweet_enricher.enrich_project_tweets_simple(tweets, user_data_map)
            self.logger.info(f"项目推文极简化增强完成，处理了 {len(enriched_tweets)} 条推文")
            
            # 3. 简化存储到数据库（跳过复杂的处理逻辑）
            # 先保存用户数据
            if users:
                user_saved_count = self._save_users_to_database(users)
                self.logger.info(f"成功保存 {user_saved_count} 条用户数据")
            
            # 保存项目推文数据（使用普通推文的入库逻辑）
            tweet_saved_count = self._save_tweets_to_database(enriched_tweets)

            # 4. 数据保存完成
            if tweet_saved_count > 0:
                self.logger.info(f"成功保存 {tweet_saved_count} 条项目推文到数据库")
                self.success_count += 1

                # 打印 API 调用统计
                api_stats = self.api_client.get_request_stats()
                self.logger.info("=" * 50)
                self.logger.info("📊 API 调用统计")
                self.logger.info("=" * 50)
                self.logger.info(f"总请求次数: {api_stats.get('total_requests', 0)}")
                self.logger.info(f"获取推文数: {api_stats.get('tweets_fetched', 0)}")
                self.logger.info(f"错误次数: {api_stats.get('error_count', 0)}")
                self.logger.info(f"成功率: {api_stats.get('success_rate', 0):.2f}%")
                if api_stats.get('total_requests', 0) > 0:
                    self.logger.info(f"平均每次请求获取推文数: {api_stats.get('avg_tweets_per_request', 0):.1f}")
                # 显示成本信息
                self.logger.info("=" * 50)
                self.logger.info("💰 API 成本统计")
                self.logger.info("=" * 50)
                self.logger.info(f"本次总成本: ${api_stats.get('total_cost_usd', 0):.6f} USD")
                self.logger.info(f"平均每次请求成本: ${api_stats.get('avg_cost_per_request', 0):.6f} USD")
                self.logger.info(f"每条推文平均成本: ${api_stats.get('total_cost_usd', 0) / max(api_stats.get('tweets_fetched', 1), 1):.6f} USD")
                self.logger.info("=" * 50)

                # 自动记录成本数据到数据库
                try:
                    import subprocess
                    import os

                    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
                    # 获取项目根目录（当前文件在 src/crawler.py）
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    project_root = os.path.dirname(current_dir)
                    logger_path = os.path.join(project_root, 'src', 'utils', 'cost_db_logger.py')
                    venv_python = os.path.join(project_root, 'venv', 'bin', 'python')

                    if os.path.exists(logger_path) and os.path.exists(venv_python):
                        cmd = [
                            venv_python, logger_path,
                            '--task-name', 'project_tweet',
                            '--run-id', run_id,
                            '--total-requests', str(api_stats.get('total_requests', 0)),
                            '--total-cost', f"{api_stats.get('total_cost_usd', 0):.6f}",
                            '--tweets-fetched', str(api_stats.get('tweets_fetched', 0)),
                            '--error-count', str(api_stats.get('error_count', 0))
                        ]

                        self.logger.info(f"正在执行成本记录命令: {' '.join(cmd)}")
                        result = subprocess.run(cmd, cwd=project_root, capture_output=True, text=True)

                        if result.returncode == 0:
                            self.logger.info("✓ 成本数据已自动记录到数据库")
                        else:
                            self.logger.error(f"❌ 记录成本数据脚本执行失败 (Exit Code: {result.returncode})")
                            self.logger.error(f"标准错误输出: {result.stderr}")
                            self.logger.error(f"标准输出: {result.stdout}")

                except Exception as e:
                    self.logger.error(f"🚨 记录成本数据过程发生异常: {e}")

                return True
            else:
                self.logger.error("保存项目推文到数据库失败")
                self.error_count += 1
                return False
                
        except Exception as e:
            self.logger.error(f"爬取项目推文数据异常: {e}")
            self.error_count += 1
            return False

    def update_historical_metrics(self, days: float = 7) -> bool:
        """
        更新历史推文的互动数据（点赞、转发、评论、浏览量）

        Args:
            days: 更新过去多少天的推文

        Returns:
            是否成功
        """
        self.crawl_count += 1
        self.last_crawl_time = datetime.now()
        started_at = datetime.utcnow()
        if process_code:
            safe_report(
                report_process_status,
                process_code,
                "running",
                execution_id=execution_id,
                start_time=_utc_text(started_at),
            )

        try:
            self.api_client.reset_stats()
            self.logger.info(f"开始更新历史推文互动数据 (范围: 过去 {days} 天)")

            # 1. 从数据库获取需要更新的推文ID
            # 注意：这里直接操作 tweet_dao 的 db_manager，稍微破坏了封装但更高效
            table_name = self.tweet_dao.table_name
            # 计算起始时间
            start_time = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

            sql = f"SELECT id_str FROM {table_name} WHERE created_at_datetime >= %s ORDER BY created_at_datetime DESC"

            self.logger.info(f"查询数据库中 {start_time} 之后的推文...")
            results = self.tweet_dao.db_manager.execute_query(sql, (start_time,))

            if not results:
                self.logger.info("没有找到需要更新的推文")
                return True

            tweet_ids = [row['id_str'] for row in results]
            self.logger.info(f"共找到 {len(tweet_ids)} 条需要更新的推文")

            # 2. 调用 API 批量获取最新数据
            # 注意：tweet_ids 可能会非常多，api_client.fetch_tweets_by_ids 内部已经做了分批处理
            updated_tweets_data = self.api_client.fetch_tweets_by_ids(tweet_ids)

            if not updated_tweets_data:
                self.logger.warning("未从 API 获取到任何更新数据")
                return False

            self.logger.info(f"从 API 成功获取到 {len(updated_tweets_data)} 条推文的最新数据")

            # 3. 构造 SQL 进行批量更新
            # 我们只需要更新互动指标

            batch_size = 500

            sql_update = f"""
                UPDATE {table_name}
                SET favorite_count = %s,
                    retweet_count = %s,
                    reply_count = %s,
                    quote_count = %s,
                    view_count = %s,
                    bookmark_count = %s
                WHERE id_str = %s
            """

            update_params = []
            for t_data in updated_tweets_data:
                update_params.append((
                    t_data.get('favorite_count', 0),
                    t_data.get('retweet_count', 0),
                    t_data.get('reply_count', 0),
                    t_data.get('quote_count', 0),
                    t_data.get('view_count', 0),
                    t_data.get('bookmark_count', 0),
                    t_data.get('id_str')
                ))

            if update_params:
                self.logger.info(f"开始批量写入数据库 (共 {len(update_params)} 条)...")
                total_affected = 0

                # 分批执行 UPDATE
                for i in range(0, len(update_params), batch_size):
                    batch = update_params[i:i+batch_size]
                    try:
                        with self.tweet_dao.db_manager.get_cursor() as (conn, cursor):
                            cursor.executemany(sql_update, batch)
                            conn.commit()
                            total_affected += len(batch)
                    except Exception as e:
                        self.logger.error(f"批量更新数据库失败 (批次 {i}): {e}")

                self.logger.info(f"数据库更新完成，预计处理了 {total_affected} 条记录")
                self.success_count += 1

                # 记录成本（复用 run_project_once 里的逻辑）
                api_stats = self.api_client.get_request_stats()
                self.logger.info("=" * 50)
                self.logger.info("💰 更新操作成本统计")
                self.logger.info(f"API请求数: {api_stats.get('total_requests', 0)}")
                self.logger.info(f"总成本: ${api_stats.get('total_cost_usd', 0):.6f} USD")
                self.logger.info("=" * 50)

                # 自动记录成本数据到数据库
                try:
                    import subprocess
                    import os

                    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    project_root = os.path.dirname(current_dir)
                    logger_path = os.path.join(project_root, 'src', 'utils', 'cost_db_logger.py')
                    venv_python = os.path.join(project_root, 'venv', 'bin', 'python')

                    if os.path.exists(logger_path) and os.path.exists(venv_python):
                        cmd = [
                            venv_python, logger_path,
                            '--task-name', 'update_metrics',
                            '--run-id', run_id,
                            '--total-requests', str(api_stats.get('total_requests', 0)),
                            '--total-cost', f"{api_stats.get('total_cost_usd', 0):.6f}",
                            '--tweets-fetched', str(api_stats.get('tweets_fetched', 0)),
                            '--error-count', str(api_stats.get('error_count', 0))
                        ]
                        subprocess.run(cmd, cwd=project_root, capture_output=True, text=True)
                        self.logger.info("✓ 成本数据已自动记录到数据库")
                except Exception as e:
                    self.logger.warning(f"记录成本数据失败: {e}")

                return True
            else:
                self.logger.warning("没有需要更新的数据")
                return False

        except Exception as e:
            self.logger.error(f"更新历史推文数据异常: {e}")
            self.error_count += 1
            return False
    
    def close(self) -> None:
        """关闭爬虫，清理资源"""
        try:
            self.api_client.close()
            self.tweet_dao.db_manager.close()
            self.user_dao.db_manager.close()
            self.quotation_dao.db_manager.close()
            self.logger.info("爬虫资源已清理")
        except Exception as e:
            self.logger.error(f"清理爬虫资源时出错: {e}")


# 全局爬虫实例
crawler = TwitterCrawler() 