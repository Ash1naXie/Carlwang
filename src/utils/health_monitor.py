#!/usr/bin/env python3
"""
爬虫健康监控模块
用于检测异常情况（如连续无效推文）并记录成本
"""
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from src.utils.logger import get_logger


class HealthMonitor:
    """爬虫健康监控器"""

    def __init__(self, stats_file: str = None):
        self.logger = get_logger(__name__)

        # 统计文件路径
        if stats_file:
            self.stats_file = Path(stats_file)
        else:
            project_root = Path(__file__).parent.parent.parent
            self.stats_file = project_root / 'logs' / 'crawler_health_stats.json'

        self.stats_file.parent.mkdir(parents=True, exist_ok=True)

        # 异常检测配置
        self.config = {
            'max_consecutive_zero_valid': 6,  # 连续6次0条有效推文触发报警
            'min_valid_rate': 0.05,  # 最低有效率5%（低于此值触发警告）
            'cost_alert_threshold_hourly': 1.0,  # 每小时成本超过$1触发警告
            'cost_alert_threshold_daily': 10.0,  # 每天成本超过$10触发警告
        }

    def record_crawl_stats(self, stats: Dict) -> None:
        """
        记录一次爬取的统计信息

        Args:
            stats: 包含以下字段的字典
                - timestamp: 时间戳
                - total_tweets: 总推文数
                - valid_tweets: 有效推文数
                - invalid_tweets: 无效推文数
                - twitter_api_cost: Twitter API成本
                - ai_api_cost: AI验证成本
                - total_cost: 总成本
        """
        try:
            # 加载现有统计
            history = self._load_history()

            # 添加新记录
            record = {
                'timestamp': stats.get('timestamp', datetime.now().isoformat()),
                'process_code': stats.get('process_code'),
                'list_ids': stats.get('list_ids'),
                'total_tweets': stats.get('total_tweets', 0),
                'valid_tweets': stats.get('valid_tweets', 0),
                'invalid_tweets': stats.get('invalid_tweets', 0),
                'valid_rate': stats.get('valid_tweets', 0) / max(stats.get('total_tweets', 1), 1),
                'twitter_api_cost': stats.get('twitter_api_cost', 0),
                'ai_api_cost': stats.get('ai_api_cost', 0),
                'total_cost': stats.get('total_cost', 0),
                'total_requests': stats.get('total_requests', 0),
                'error_count': stats.get('error_count', 0),
            }

            history.append(record)

            # 只保留最近30天的数据
            cutoff_time = datetime.now() - timedelta(days=30)
            history = [r for r in history if datetime.fromisoformat(r['timestamp']) > cutoff_time]

            # 保存
            self._save_history(history)

            self.logger.info(f"记录爬取统计: 总={record['total_tweets']}, 有效={record['valid_tweets']}, 成本=${record['total_cost']:.4f}")

        except Exception as e:
            self.logger.error(f"记录统计失败: {e}")

    def check_health(self) -> Dict:
        """
        检查爬虫健康状态

        Returns:
            健康状态字典，包含:
                - is_healthy: 是否健康
                - alerts: 报警列表
                - warnings: 警告列表
                - should_stop: 是否应该停止服务
        """
        try:
            history = self._load_history()

            result = {
                'is_healthy': True,
                'alerts': [],
                'warnings': [],
                'should_stop': False,
            }

            if not history:
                return result

            # 1. 检查连续零有效推文
            consecutive_zero = self._count_consecutive_zero_valid(history)
            if consecutive_zero >= self.config['max_consecutive_zero_valid']:
                result['is_healthy'] = False
                result['should_stop'] = True
                result['alerts'].append(
                    f"🚨 严重: 连续{consecutive_zero}次爬取0条有效推文！可能是验证逻辑出错。"
                )
            elif consecutive_zero >= self.config['max_consecutive_zero_valid'] // 2:
                result['warnings'].append(
                    f"⚠️  警告: 连续{consecutive_zero}次爬取0条有效推文，请注意。"
                )

            # 2. 检查最近的有效率
            recent_valid_rate = self._calculate_recent_valid_rate(history, hours=6)
            if recent_valid_rate < self.config['min_valid_rate']:
                result['warnings'].append(
                    f"⚠️  警告: 最近6小时有效率仅{recent_valid_rate*100:.1f}%（低于{self.config['min_valid_rate']*100}%阈值）"
                )

            # 3. 检查成本
            hourly_cost = self._calculate_cost(history, hours=1)
            daily_cost = self._calculate_cost(history, hours=24)

            if hourly_cost > self.config['cost_alert_threshold_hourly']:
                result['warnings'].append(
                    f"⚠️  成本警告: 最近1小时成本${hourly_cost:.2f}（超过阈值${self.config['cost_alert_threshold_hourly']:.2f}）"
                )

            if daily_cost > self.config['cost_alert_threshold_daily']:
                result['alerts'].append(
                    f"🚨 成本超限: 最近24小时成本${daily_cost:.2f}（超过阈值${self.config['cost_alert_threshold_daily']:.2f}）"
                )

            return result

        except Exception as e:
            self.logger.error(f"健康检查失败: {e}")
            return {
                'is_healthy': False,
                'alerts': [f"健康检查异常: {e}"],
                'warnings': [],
                'should_stop': False,
            }

    def get_cost_summary(self, hours: int = 24) -> Dict:
        """
        获取成本摘要

        Args:
            hours: 统计最近N小时

        Returns:
            成本摘要字典
        """
        try:
            history = self._load_history()

            if not history:
                return {
                    'period_hours': hours,
                    'twitter_api_cost': 0,
                    'ai_api_cost': 0,
                    'total_cost': 0,
                    'request_count': 0,
                    'avg_cost_per_request': 0,
                }

            # 过滤最近N小时的数据
            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent = [r for r in history if datetime.fromisoformat(r['timestamp']) > cutoff_time]

            twitter_cost = sum(r.get('twitter_api_cost', 0) for r in recent)
            ai_cost = sum(r.get('ai_api_cost', 0) for r in recent)
            total_cost = sum(r.get('total_cost', 0) for r in recent)

            return {
                'period_hours': hours,
                'twitter_api_cost': twitter_cost,
                'ai_api_cost': ai_cost,
                'total_cost': total_cost,
                'request_count': len(recent),
                'avg_cost_per_request': total_cost / len(recent) if recent else 0,
            }

        except Exception as e:
            self.logger.error(f"获取成本摘要失败: {e}")
            return {}

    def _load_history(self) -> List[Dict]:
        """加载历史统计"""
        try:
            if self.stats_file.exists():
                with open(self.stats_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except Exception as e:
            self.logger.error(f"加载历史统计失败: {e}")
            return []

    def _save_history(self, history: List[Dict]) -> None:
        """保存历史统计"""
        try:
            with open(self.stats_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"保存历史统计失败: {e}")

    def _count_consecutive_zero_valid(self, history: List[Dict]) -> int:
        """计算连续零有效推文的次数"""
        if not history:
            return 0

        # 从最新的记录开始往前数
        count = 0
        for record in reversed(history):
            if record.get('valid_tweets', 0) == 0:
                count += 1
            else:
                break

        return count

    def _calculate_recent_valid_rate(self, history: List[Dict], hours: int) -> float:
        """计算最近N小时的有效率"""
        if not history:
            return 0.0

        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent = [r for r in history if datetime.fromisoformat(r['timestamp']) > cutoff_time]

        if not recent:
            return 0.0

        total = sum(r.get('total_tweets', 0) for r in recent)
        valid = sum(r.get('valid_tweets', 0) for r in recent)

        return valid / total if total > 0 else 0.0

    def _calculate_cost(self, history: List[Dict], hours: int) -> float:
        """计算最近N小时的总成本"""
        if not history:
            return 0.0

        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent = [r for r in history if datetime.fromisoformat(r['timestamp']) > cutoff_time]

        return sum(r.get('total_cost', 0) for r in recent)
