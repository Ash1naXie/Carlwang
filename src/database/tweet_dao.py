"""
推文数据访问对象 (Data Access Object)
"""
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime
import json
from pathlib import Path

from .connection import db_manager
from ..models.tweet import Tweet


class TweetDAO:
    """推文数据访问对象"""
    
    def __init__(self):
        """初始化DAO"""
        self.db_manager = db_manager
        self.table_name = self.db_manager.db_config.get('tables', {}).get('tweet', 'twitter_tweet')
        self.logger = logging.getLogger(__name__)

    def _get_failed_queue_file(self) -> Path:
        """获取失败推文重放队列文件路径"""
        project_root = Path(__file__).resolve().parents[2]

        if self.table_name == 'predx_raw_tweets':
            queue_dir = project_root / 'daily_predx_raw_tweet_crawler'
        elif self.table_name == 'twitter_tweet':
            queue_dir = project_root / 'daily_kol_tweet_crawler'
        elif self.table_name == 'twitter_tweet_back_test_cmc300':
            queue_dir = project_root / 'daily_tweet_crawler'
        else:
            queue_dir = project_root / 'logs'

        queue_dir.mkdir(parents=True, exist_ok=True)
        return queue_dir / f'failed_tweet_replay_{self.table_name}.jsonl'

    def _json_safe(self, value: Any) -> Any:
        """将对象转换为可JSON序列化格式"""
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(value, dict):
            return {k: self._json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._json_safe(v) for v in value]
        return value

    def _queue_failed_tweet(self, tweet_data: Dict[str, Any], error: Exception) -> None:
        """将失败推文写入本地重放队列"""
        queue_file = self._get_failed_queue_file()
        record = {
            'id_str': tweet_data.get('id_str'),
            'table_name': self.table_name,
            'failed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'error': str(error),
            'retry_count': 0,
            'tweet_data': self._json_safe(tweet_data),
        }

        with queue_file.open('a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')

    def _load_failed_queue_records(self, queue_file: Path) -> List[Dict[str, Any]]:
        """加载失败推文重放队列"""
        records: List[Dict[str, Any]] = []

        if not queue_file.exists():
            return records

        with queue_file.open('r', encoding='utf-8') as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    self.logger.warning(f"重放队列第{line_no}行JSON解析失败，已跳过: {exc}")

        return records

    def _write_failed_queue_records(self, queue_file: Path, records: List[Dict[str, Any]]) -> None:
        """覆盖写回失败推文重放队列"""
        temp_file = queue_file.with_suffix('.tmp')
        with temp_file.open('w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        temp_file.replace(queue_file)

    def _is_storable_tweet(self, tweet: Tweet) -> bool:
        """判断推文是否需要写入主表。"""
        if self.table_name == 'predx_raw_tweets':
            return tweet.validate()
        return tweet.validate() and tweet.is_valid != 0

    def count_storable_tweets(self, tweets: List[Tweet]) -> int:
        """统计本轮真正需要写入主表的推文数量。"""
        return sum(1 for tweet in tweets if self._is_storable_tweet(tweet))

    def _build_batch_upsert_sql(self) -> str:
        """构建当前表的批量upsert SQL"""
        if self.table_name == 'predx_raw_tweets':
            return f"""
                INSERT INTO {self.table_name} (
                    tweet_id, tweet_url, author_handle, author_name, followers_count,
                    full_text, summary, lang, sentiment,
                    engagement_total, like_count, repost_count, reply_count, quote_count, view_count,
                    published_at, fetched_at, source_job_id, parse_status, analysis_status, raw_json
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
            """

        if self.table_name == 'twitter_tweet_back_test_cmc300':
            return f"""
                INSERT INTO {self.table_name} (
                    id_str, conversation_id_str, in_reply_to_status_id_str,
                    full_text, created_at, created_at_datetime,
                    bookmark_count, favorite_count, quote_count, reply_count,
                    retweet_count, view_count, engagement_total, update_time,
                    user_id, user_name, tweet_url, sentiment, link_url, isAnnounce, summary, is_activity, is_retweet
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

        return f"""
            INSERT INTO {self.table_name} (
                id_str, conversation_id_str, in_reply_to_status_id_str,
                full_text, created_at, created_at_datetime,
                bookmark_count, favorite_count, quote_count, reply_count,
                retweet_count, view_count, engagement_total, update_time,
                kol_id, entity_id, project_id, topic_id, narrative_id, is_valid, sentiment, tweet_url, link_url, token_tag, project_tag, isAnnounce, is_announce, summary, is_activity, activity_detail, is_real_project_tweet, etl_flag
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

    def _build_batch_upsert_params(self, tweet_data: Dict[str, Any]) -> tuple:
        """构建当前表的批量upsert参数"""
        if self.table_name == 'predx_raw_tweets':
            raw_json = tweet_data.get('raw_json')
            if raw_json is not None and not isinstance(raw_json, str):
                raw_json = json.dumps(raw_json, ensure_ascii=False, default=str)

            return (
                tweet_data.get('id_str'),
                tweet_data.get('tweet_url'),
                tweet_data.get('author_handle') or tweet_data.get('user_name'),
                tweet_data.get('author_name') or tweet_data.get('user_name'),
                tweet_data.get('followers_count', 0) or 0,
                tweet_data.get('full_text'),
                tweet_data.get('summary'),
                tweet_data.get('lang'),
                tweet_data.get('sentiment'),
                tweet_data.get('engagement_total', 0) or 0,
                tweet_data.get('favorite_count', 0) or 0,
                tweet_data.get('retweet_count', 0) or 0,
                tweet_data.get('reply_count', 0) or 0,
                tweet_data.get('quote_count', 0) or 0,
                tweet_data.get('view_count', 0) or 0,
                tweet_data.get('created_at_datetime'),
                tweet_data.get('fetched_at') or datetime.now(),
                tweet_data.get('source_job_id') or 'predx_raw_lists',
                tweet_data.get('parse_status') or 'ready',
                tweet_data.get('analysis_status') or 'pending',
                raw_json,
            )

        if self.table_name == 'twitter_tweet_back_test_cmc300':
            return (
                tweet_data.get('id_str'),
                tweet_data.get('conversation_id_str'),
                tweet_data.get('in_reply_to_status_id_str'),
                tweet_data.get('full_text'),
                tweet_data.get('created_at'),
                tweet_data.get('created_at_datetime'),
                tweet_data.get('bookmark_count', 0),
                tweet_data.get('favorite_count', 0),
                tweet_data.get('quote_count', 0),
                tweet_data.get('reply_count', 0),
                tweet_data.get('retweet_count', 0),
                tweet_data.get('view_count', 0),
                tweet_data.get('engagement_total', 0),
                tweet_data.get('update_time'),
                tweet_data.get('kol_id'),
                tweet_data.get('user_name'),
                tweet_data.get('tweet_url'),
                tweet_data.get('sentiment'),
                tweet_data.get('link_url'),
                tweet_data.get('is_announce', 0),
                tweet_data.get('summary'),
                tweet_data.get('is_activity', 0),
                1 if tweet_data.get('is_retweet') else 0,
            )

        activity_detail = tweet_data.get('activity_detail')
        if isinstance(activity_detail, (dict, list)):
            activity_detail = json.dumps(activity_detail, ensure_ascii=False)

        return (
            tweet_data.get('id_str'),
            tweet_data.get('conversation_id_str'),
            tweet_data.get('in_reply_to_status_id_str'),
            tweet_data.get('full_text'),
            tweet_data.get('created_at'),
            tweet_data.get('created_at_datetime'),
            tweet_data.get('bookmark_count', 0),
            tweet_data.get('favorite_count', 0),
            tweet_data.get('quote_count', 0),
            tweet_data.get('reply_count', 0),
            tweet_data.get('retweet_count', 0),
            tweet_data.get('view_count', 0),
            tweet_data.get('engagement_total', 0),
            tweet_data.get('update_time'),
            tweet_data.get('kol_id'),
            tweet_data.get('entity_id'),
            tweet_data.get('project_id'),
            tweet_data.get('topic_id'),
            tweet_data.get('narrative_id'),
            tweet_data.get('is_valid'),
            tweet_data.get('sentiment'),
            tweet_data.get('tweet_url'),
            tweet_data.get('link_url'),
            tweet_data.get('token_tag'),
            tweet_data.get('project_tag'),
            tweet_data.get('is_announce', 0),
            tweet_data.get('is_announce', 0),
            tweet_data.get('summary'),
            tweet_data.get('is_activity', 0),
            activity_detail,
            tweet_data.get('is_real_project_tweet', 0),
            tweet_data.get('etl_flag', 0),
        )

    def _replay_failed_tweets(self, max_replay: int = 300) -> None:
        """自动重放历史失败推文（每轮限制重放条数）"""
        queue_file = self._get_failed_queue_file()
        records = self._load_failed_queue_records(queue_file)
        if not records:
            return

        sql = self._build_batch_upsert_sql()
        remaining: List[Dict[str, Any]] = []
        replay_attempted = 0
        replay_success = 0
        replay_failed = 0

        for record in records:
            record_table = record.get('table_name')
            if record_table != self.table_name:
                remaining.append(record)
                continue

            if replay_attempted >= max_replay:
                remaining.append(record)
                continue

            tweet_data = record.get('tweet_data') or {}
            tweet_id = tweet_data.get('id_str') or record.get('id_str')
            if not tweet_id:
                continue

            replay_attempted += 1

            try:
                params = self._build_batch_upsert_params(tweet_data)
                self.db_manager.execute_update(sql, params)
                replay_success += 1
            except Exception as exc:
                replay_failed += 1
                record['retry_count'] = int(record.get('retry_count', 0)) + 1
                record['last_error'] = str(exc)
                record['last_retry_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                remaining.append(record)

        self._write_failed_queue_records(queue_file, remaining)

        if replay_attempted > 0:
            self.logger.info(
                f"失败推文自动重放: 尝试={replay_attempted}, 成功={replay_success}, 失败={replay_failed}, 剩余队列={len(remaining)}"
            )
    
    def insert_tweet(self, tweet: Tweet) -> bool:
        """
        插入单条推文数据

        Args:
            tweet: 推文对象

        Returns:
            是否插入成功
        """
        if not tweet.validate():
            self.logger.error(f"推文数据验证失败: {tweet}")
            return False

        # 跳过 is_valid=0 的推文
        if tweet.is_valid == 0:
            self.logger.info(f"跳过存储 is_valid=0 的推文: {tweet.id_str}")
            return False

        try:
            # 包含所有字段（包括新增的project_id、topic_id、is_retweet等）
            sql = f"""
            INSERT INTO {self.table_name} (
                id_str, conversation_id_str, in_reply_to_status_id_str,
                full_text, is_quote_status, is_retweet, created_at, created_at_datetime,
                bookmark_count, favorite_count, quote_count, reply_count,
                retweet_count, view_count, engagement_total, update_time,
                kol_id, entity_id, project_id, topic_id, narrative_id, is_valid, sentiment, tweet_url, link_url, token_tag, project_tag, isAnnounce, is_announce, summary, is_activity, activity_detail, is_real_project_tweet, etl_flag
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            tweet_data = tweet.to_dict()
            params = (
                tweet_data['id_str'],
                tweet_data['conversation_id_str'],
                tweet_data['in_reply_to_status_id_str'],
                tweet_data['full_text'],
                tweet_data.get('is_quote_status', False),
                tweet_data.get('is_retweet', False),
                tweet_data['created_at'],
                tweet_data['created_at_datetime'],
                tweet_data['bookmark_count'],
                tweet_data['favorite_count'],
                tweet_data['quote_count'],
                tweet_data['reply_count'],
                tweet_data['retweet_count'],
                tweet_data['view_count'],
                tweet_data['engagement_total'],
                tweet_data['update_time'],
                tweet_data['kol_id'],
                tweet_data['entity_id'],
                tweet_data['project_id'],
                tweet_data['topic_id'],
                tweet_data.get('narrative_id'),
                tweet_data['is_valid'],
                tweet_data['sentiment'],
                tweet_data['tweet_url'],
                tweet_data.get('link_url'),  # 使用get方法以防字段不存在
                tweet_data.get('token_tag'),
                tweet_data.get('project_tag'),
                tweet_data.get('is_announce', 0),  # isAnnounce字段
                tweet_data.get('is_announce', 0),  # is_announce字段
                tweet_data.get('summary'),  # 公告总结
                tweet_data.get('is_activity', 0),  # 是否为活动
                tweet_data.get('activity_detail'),  # 活动详情
                tweet_data.get('is_real_project_tweet', 0),  # 是否为项目官方推文
                tweet_data.get('etl_flag', 0)  # ETL标记
            )
            
            affected_rows = self.db_manager.execute_update(sql, params)
            success = affected_rows > 0
            
            if success:
                self.logger.info(f"推文插入成功: {tweet.id_str}")
            else:
                self.logger.warning(f"推文插入失败: {tweet.id_str}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"插入推文数据失败: {tweet.id_str}, 错误: {e}")
            return False
    
    def upsert_tweet(self, tweet: Tweet) -> bool:
        """
        插入或更新推文数据（Doris Unique Key模型自动处理重复数据）

        Args:
            tweet: 推文对象

        Returns:
            是否操作成功
        """
        if not tweet.validate():
            self.logger.error(f"推文数据验证失败: {tweet}")
            return False

        # 跳过 is_valid=0 的推文
        if tweet.is_valid == 0:
            self.logger.info(f"跳过存储 is_valid=0 的推文: {tweet.id_str}")
            return False

        try:
            # 包含所有字段（包括新增的project_id、topic_id、is_retweet等）
            sql = f"""
            INSERT INTO {self.table_name} (
                id_str, conversation_id_str, in_reply_to_status_id_str,
                full_text, is_quote_status, is_retweet, created_at, created_at_datetime,
                bookmark_count, favorite_count, quote_count, reply_count,
                retweet_count, view_count, engagement_total, update_time,
                kol_id, entity_id, project_id, topic_id, narrative_id, is_valid, sentiment, tweet_url, link_url, token_tag, project_tag, isAnnounce, is_announce, summary, is_activity, activity_detail, is_real_project_tweet, etl_flag
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            tweet_data = tweet.to_dict()
            params = (
                tweet_data['id_str'],
                tweet_data['conversation_id_str'],
                tweet_data['in_reply_to_status_id_str'],
                tweet_data['full_text'],
                tweet_data.get('is_quote_status', False),
                tweet_data.get('is_retweet', False),
                tweet_data['created_at'],
                tweet_data['created_at_datetime'],
                tweet_data['bookmark_count'],
                tweet_data['favorite_count'],
                tweet_data['quote_count'],
                tweet_data['reply_count'],
                tweet_data['retweet_count'],
                tweet_data['view_count'],
                tweet_data['engagement_total'],
                tweet_data['update_time'],
                tweet_data['kol_id'],
                tweet_data['entity_id'],
                tweet_data['project_id'],
                tweet_data['topic_id'],
                tweet_data.get('narrative_id'),
                tweet_data['is_valid'],
                tweet_data['sentiment'],
                tweet_data['tweet_url'],
                tweet_data.get('link_url'),  # 使用get方法以防字段不存在
                tweet_data.get('token_tag'),
                tweet_data.get('project_tag'),
                tweet_data.get('is_announce', 0),  # isAnnounce字段
                tweet_data.get('is_announce', 0),  # is_announce字段
                tweet_data.get('summary'),  # 公告总结
                tweet_data.get('is_activity', 0),  # 是否为活动
                tweet_data.get('activity_detail'),  # 活动详情
                tweet_data.get('is_real_project_tweet', 0),  # 是否为项目官方推文
                tweet_data.get('etl_flag', 0)  # ETL标记
            )
            
            affected_rows = self.db_manager.execute_update(sql, params)
            success = affected_rows > 0
            
            if success:
                self.logger.info(f"推文upsert成功: {tweet.id_str}")
            else:
                self.logger.warning(f"推文upsert失败: {tweet.id_str}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"推文upsert失败: {tweet.id_str}, 错误: {e}")
            return False
    
    def batch_upsert_tweets(self, tweets: List[Tweet]) -> int:
        """
        批量插入或更新推文数据
        
        Args:
            tweets: 推文对象列表
            
        Returns:
            成功操作的数量
        """
        if not tweets:
            try:
                self._replay_failed_tweets()
            except Exception as replay_error:
                self.logger.warning(f"执行失败推文自动重放时出错: {replay_error}")
            return 0
        
        # 过滤有效的推文（排除 is_valid=0 的推文）
        valid_tweets = [tweet for tweet in tweets if self._is_storable_tweet(tweet)]
        filtered_count = len(tweets) - len(valid_tweets)
        if filtered_count > 0:
            invalid_by_validation = len([t for t in tweets if not t.validate()])
            invalid_by_is_valid = len([t for t in tweets if t.validate() and t.is_valid == 0])
            self.logger.info(f"过滤推文: 数据验证失败={invalid_by_validation}, is_valid=0={invalid_by_is_valid}, 总共过滤={filtered_count}")

        if not valid_tweets:
            return 0

        try:
            self._replay_failed_tweets()
        except Exception as replay_error:
            self.logger.warning(f"执行失败推文自动重放时出错: {replay_error}")
        
        try:
            sql = self._build_batch_upsert_sql()

            success_count = 0
            failed_count = 0
            queued_count = 0
            for tweet in valid_tweets:
                tweet_data = {'id_str': tweet.id_str}
                try:
                    tweet_data = tweet.to_dict()
                    params = self._build_batch_upsert_params(tweet_data)
                    
                    affected_rows = self.db_manager.execute_update(sql, params)
                    if affected_rows > 0:
                        success_count += 1
                        
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"插入推文失败: {tweet.id_str}, 错误: {e}")
                    try:
                        self._queue_failed_tweet(tweet_data, e)
                        queued_count += 1
                    except Exception as queue_error:
                        self.logger.error(f"失败推文落盘失败: {tweet.id_str}, 错误: {queue_error}")
                    continue
            
            self.logger.info(f"批量upsert推文成功: {success_count}/{len(valid_tweets)} 条数据")
            unchanged_count = len(valid_tweets) - success_count - failed_count
            self.logger.info(f"批量upsert推文明细: 成功={success_count}, 未变更={unchanged_count}, 失败={failed_count}, 失败落盘={queued_count}")
            return success_count
            
        except Exception as e:
            self.logger.error(f"批量upsert推文数据失败: {e}")
            return 0
    
    def get_tweet_by_id(self, id_str: str) -> Optional[Tweet]:
        """
        根据ID获取推文
        
        Args:
            id_str: 推文ID
            
        Returns:
            推文对象或None
        """
        try:
            sql = f"SELECT * FROM {self.table_name} WHERE id_str = %s"
            results = self.db_manager.execute_query(sql, (id_str,))
            
            if results:
                return self._dict_to_tweet(results[0])
            return None
            
        except Exception as e:
            self.logger.error(f"查询推文失败: {id_str}, 错误: {e}")
            return None
    
    def get_tweets_by_date_range(self, start_date: datetime, end_date: datetime, 
                                limit: int = 1000) -> List[Tweet]:
        """
        根据日期范围获取推文
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            limit: 限制数量
            
        Returns:
            推文对象列表
        """
        try:
            sql = f"""
            SELECT * FROM {self.table_name} 
            WHERE created_at_datetime BETWEEN %s AND %s 
            ORDER BY created_at_datetime DESC 
            LIMIT %s
            """
            results = self.db_manager.execute_query(sql, (start_date, end_date, limit))
            
            return [self._dict_to_tweet(row) for row in results]
            
        except Exception as e:
            self.logger.error(f"按日期范围查询推文失败: {e}")
            return []
    
    def get_tweet_count(self) -> int:
        """
        获取推文总数
        
        Returns:
            推文总数
        """
        try:
            sql = f"SELECT COUNT(*) as count FROM {self.table_name}"
            results = self.db_manager.execute_query(sql)
            
            if results:
                return results[0]['count']
            return 0
            
        except Exception as e:
            self.logger.error(f"查询推文总数失败: {e}")
            return 0
    
    def delete_tweet(self, id_str: str) -> bool:
        """
        删除推文
        
        Args:
            id_str: 推文ID
            
        Returns:
            是否删除成功
        """
        try:
            sql = f"DELETE FROM {self.table_name} WHERE id_str = %s"
            affected_rows = self.db_manager.execute_update(sql, (id_str,))
            
            success = affected_rows > 0
            if success:
                self.logger.info(f"推文删除成功: {id_str}")
            else:
                self.logger.warning(f"推文删除失败，可能不存在: {id_str}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"删除推文失败: {id_str}, 错误: {e}")
            return False
    
    def _dict_to_tweet(self, data: Dict[str, Any]) -> Tweet:
        """
        将数据库查询结果转换为Tweet对象
        
        Args:
            data: 数据库查询结果字典
            
        Returns:
            Tweet对象
        """
        return Tweet(
            id_str=data['id_str'],
            conversation_id_str=data.get('conversation_id_str'),
            in_reply_to_status_id_str=data.get('in_reply_to_status_id_str'),
            full_text=data.get('full_text'),
            is_quote_status=False,  # 数据库表中没有此字段，设为默认值
            created_at=data.get('created_at'),
            created_at_datetime=data.get('created_at_datetime'),
            bookmark_count=data.get('bookmark_count', 0),
            favorite_count=data.get('favorite_count', 0),
            quote_count=data.get('quote_count', 0),
            reply_count=data.get('reply_count', 0),
            retweet_count=data.get('retweet_count', 0),
            view_count=data.get('view_count', 0),
            engagement_total=data.get('engagement_total'),
            update_time=data.get('update_time')
        )
    
    def get_recent_tweets(self, since_time: datetime = None, limit: int = 100) -> List[Tweet]:
        """
        获取最近的推文

        Args:
            since_time: 起始时间
            limit: 限制数量

        Returns:
            推文列表
        """
        try:
            if since_time:
                sql = f"""
                SELECT * FROM {self.table_name}
                WHERE created_at_datetime >= %s
                ORDER BY created_at_datetime DESC
                LIMIT %s
                """
                results = self.db_manager.execute_query(sql, (since_time, limit))
            else:
                sql = f"""
                SELECT * FROM {self.table_name}
                ORDER BY created_at_datetime DESC
                LIMIT %s
                """
                results = self.db_manager.execute_query(sql, (limit,))

            return [self._dict_to_tweet(row) for row in results]

        except Exception as e:
            self.logger.error(f"查询最近推文失败: {e}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取推文统计信息
        
        Returns:
            统计信息字典
        """
        try:
            sql = f"""
            SELECT 
                COUNT(*) as total_count,
                MIN(created_at_datetime) as earliest_date,
                MAX(created_at_datetime) as latest_date,
                AVG(engagement_total) as avg_engagement
            FROM {self.table_name}
            WHERE created_at_datetime IS NOT NULL
            """
            
            results = self.db_manager.execute_query(sql)
            
            if results:
                stats = results[0]
                return {
                    'total_tweets': stats['total_count'] or 0,
                    'earliest_date': stats['earliest_date'],
                    'latest_date': stats['latest_date'],
                    'average_engagement': round(stats['avg_engagement'] or 0, 2)
                }
            
            return {}
            
        except Exception as e:
            self.logger.error(f"获取推文统计信息失败: {e}")
            return {}
    
    def get_tweets_by_users_and_time_range(self, user_ids: List[str], 
                                         start_time: datetime, 
                                         end_time: datetime,
                                         limit: int = 1000) -> List[Dict[str, Any]]:
        """
        根据用户ID列表和时间范围查询推文
        
        Args:
            user_ids: 用户ID列表
            start_time: 开始时间
            end_time: 结束时间
            limit: 限制数量
            
        Returns:
            推文数据列表
        """
        try:
            if not user_ids:
                return []
            
            # 构建IN子句的占位符
            placeholders = ','.join(['%s'] * len(user_ids))
            
            sql = f"""
            SELECT id_str, full_text, created_at_datetime, engagement_total, user_id
            FROM {self.table_name}
            WHERE user_id IN ({placeholders})
            AND created_at_datetime >= %s 
            AND created_at_datetime <= %s
            AND full_text IS NOT NULL
            AND LENGTH(full_text) > 10
            ORDER BY created_at_datetime DESC
            LIMIT %s
            """
            
            params = user_ids + [start_time, end_time, limit]
            results = self.db_manager.execute_query(sql, params)
            
            self.logger.info(f"查询到{len(results)}条推文数据")
            return results
            
        except Exception as e:
            self.logger.error(f"按用户和时间范围查询推文失败: {e}")
            return []


# 全局推文DAO实例
tweet_dao = TweetDAO()
