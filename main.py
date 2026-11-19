#!/usr/bin/env python3
"""
Twitter数据爬虫主程序
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.crawler import crawler
from src.utils.scheduler import scheduler
from src.utils.logger import get_logger
from src.utils.config_manager import config
from src.utils.health_monitor import HealthMonitor
from src.topic_engine import topic_engine
# from src.kol_engine import kol_engine  # KOL分析已禁用
# Project/AI analysis is lazy-imported only in explicit project modes.

# 初始化健康监控器
health_monitor = HealthMonitor()

# 监控页面使用的稳定流程编码
RETAIL_CORE_LIST_IDS = [
    "2076918265339920691",
    "2076918496106352730",
    "2076917145783660587",
]
ALPHA_ADDRESS_LIST_ID = "2045070679889055752"
PREDX_RAW_LIST_IDS = RETAIL_CORE_LIST_IDS


def parse_list_ids(value):
    if not value:
        return None
    return [item.strip() for item in value.split(',') if item.strip()]


def resolve_process_code(args):
    if getattr(args, 'process_code', None):
        return args.process_code
    if getattr(args, 'mode', None) in ('predx-raw-once', 'predx-raw-schedule'):
        return 'db002_predx_raw_lists'
    list_ids = getattr(args, 'list_ids_list', None) or []
    if list_ids and set(list_ids) == set(RETAIL_CORE_LIST_IDS):
        return 'db002_retail_core_lists'
    if getattr(args, 'list_id', None) == ALPHA_ADDRESS_LIST_ID:
        return 'db002_alpha_address'
    if not getattr(args, 'list_id', None) and not list_ids and getattr(args, 'mode', None) in ('once', 'schedule'):
        return 'db002_kol_tweet_general'
    return None


def build_execution_id(process_code):
    if not process_code:
        return None
    return f"{process_code}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"



def run_post_crawl_hook(logger) -> bool:
    """执行抓取后的附加命令。"""
    hook_cmd = os.getenv("POST_CRAWL_CMD", "").strip()
    if not hook_cmd:
        return True

    logger.info(f"执行抓取后钩子: {hook_cmd}")
    completed = subprocess.run(
        hook_cmd,
        shell=True,
        cwd=str(project_root),
        capture_output=True,
        text=True,
    )

    if completed.stdout and completed.stdout.strip():
        logger.info(f"抓取后钩子输出:\n{completed.stdout.strip()}")
    if completed.stderr and completed.stderr.strip():
        logger.warning(f"抓取后钩子错误输出:\n{completed.stderr.strip()}")

    if completed.returncode != 0:
        logger.error(f"抓取后钩子执行失败，退出码: {completed.returncode}")
        return False

    logger.info("抓取后钩子执行完成")
    return True


def main():
    """主函数"""
    # 设置日志
    logger = get_logger(__name__)
    
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='Twitter数据爬虫')
    parser.add_argument('--mode', choices=['once', 'schedule', 'test', 'topic', 'project', 'project-once', 'project-schedule', 'predx-raw-once', 'predx-raw-schedule', 'health', 'cost', 'update-metrics', 'update-metrics-daemon'], default='once',
                       help='运行模式: once=单次执行, schedule=定时调度, test=测试连接, health=健康检查, cost=成本统计, update-metrics=更新历史指标, update-metrics-daemon=守护进程模式更新历史指标')
    parser.add_argument('--max-pages', type=int, help='最大页数')
    parser.add_argument('--page-size', type=int, help='每页大小')
    parser.add_argument('--interval', type=int, help='调度间隔(分钟)')
    parser.add_argument('--hours-limit', type=float, default=3, help='时间限制(小时，可为小数)，只拉取过去N小时的推文，默认3小时')
    parser.add_argument('--days', type=float, default=7, help='天数限制，用于 update-metrics 模式，默认7天')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--cost-period', type=int, default=24, help='成本统计周期(小时)，默认24小时')
    parser.add_argument('--list-id', type=str, help='指定Twitter List ID')
    parser.add_argument('--list-ids', type=str, help='逗号分隔的多个 Twitter List ID')
    parser.add_argument('--process-code', type=str, help='监控页面使用的流程编码')
    
    args = parser.parse_args()
    args.list_ids_list = parse_list_ids(args.list_ids)
    args.process_code = resolve_process_code(args)
    
    logger.info("=" * 50)
    logger.info("Twitter数据爬虫启动")
    logger.info("=" * 50)
    
    try:
        # 如果指定了配置文件，重新加载配置
        if args.config:
            config.config_file = Path(args.config)
            config.reload_config()
            logger.info(f"使用配置文件: {args.config}")
        
        # 根据模式执行相应操作
        if args.mode == 'test':
            run_tests()
        elif args.mode == 'once':
            run_once(args)
        elif args.mode == 'schedule':
            run_scheduled(args)
        elif args.mode == 'topic':
            run_topic_analysis(args)
        # elif args.mode == 'kol':  # KOL分析已禁用
        #     run_kol_analysis(args)
        elif args.mode == 'project':
            run_project_analysis(args)
        elif args.mode == 'project-once':
            run_project_once(args)
        elif args.mode == 'project-schedule':
            run_project_scheduled(args)
        elif args.mode == 'predx-raw-once':
            run_predx_raw_once(args)
        elif args.mode == 'predx-raw-schedule':
            run_predx_raw_scheduled(args)
        elif args.mode == 'health':
            run_health_check()
        elif args.mode == 'cost':
            run_cost_stats(args)
        elif args.mode == 'update-metrics':
            run_update_metrics(args)
        elif args.mode == 'update-metrics-daemon':
            run_update_metrics_daemon(args)
        
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在退出...")
    except Exception as e:
        logger.error(f"程序异常: {e}")
        sys.exit(1)
    finally:
        # 清理资源
        cleanup()


def run_tests():
    """运行连接测试"""
    logger = get_logger(__name__)
    
    logger.info("开始运行连接测试...")
    
    # 测试数据库连接
    db_success = crawler.test_connection()
    
    # 测试API连接
    api_success = crawler.test_api_connection()
    
    # 显示测试结果
    logger.info("=" * 30)
    logger.info("测试结果:")
    logger.info(f"数据库连接: {'✓ 成功' if db_success else '✗ 失败'}")
    logger.info(f"API连接: {'✓ 成功' if api_success else '✗ 失败'}")
    logger.info("=" * 30)
    
    if db_success and api_success:
        logger.info("所有连接测试通过")
        sys.exit(0)
    else:
        logger.error("连接测试失败")
        sys.exit(1)


def run_once(args):
    """单次执行爬取"""
    logger = get_logger(__name__)

    logger.info("开始单次数据爬取...")

    # 先进行健康检查
    health_status = health_monitor.check_health()
    if health_status['should_stop']:
        logger.error("健康检查失败，服务应该停止！")
        for alert in health_status['alerts']:
            logger.error(alert)
        sys.exit(1)

    # 显示警告（如果有）
    for warning in health_status['warnings']:
        logger.warning(warning)

    # 执行爬取（直接使用配置文件中的list_ids进行并行获取，使用智能时间检测）
    success = crawler.crawl_tweets(
        list_id=args.list_id,
        list_ids=args.list_ids_list,
        process_code=args.process_code,
        execution_id=build_execution_id(args.process_code),
        max_pages=args.max_pages,
        page_size=args.page_size,
        hours_limit=args.hours_limit
    )

    # 获取爬取统计信息
    stats = crawler.get_statistics()
    logger.info("=" * 30)
    logger.info("爬取统计:")
    for key, value in stats.items():
        logger.info(f"{key}: {value}")
    logger.info("=" * 30)

    # 记录统计到健康监控（需要从crawler获取详细统计）
    try:
        from datetime import datetime
        # 计算有效/无效推文数（从数据库最近记录中统计）
        # 这里简化处理，实际应该从crawler返回的详细统计中获取
        health_monitor.record_crawl_stats({
            'timestamp': datetime.now().isoformat(),
            'total_tweets': stats.get('database_tweet_count', 0),  # 简化：使用总数
            'valid_tweets': int(stats.get('database_tweet_count', 0) * 0.95),  # 估算，应从crawler获取
            'invalid_tweets': int(stats.get('database_tweet_count', 0) * 0.05),
            'twitter_api_cost': stats.get('api_stats', {}).get('total_cost_usd', 0),
            'ai_api_cost': 0,  # TODO: 从AI调用中获取
            'total_cost': stats.get('api_stats', {}).get('total_cost_usd', 0),
        })
    except Exception as e:
        logger.warning(f"记录健康统计失败: {e}")

    if success:
        if not run_post_crawl_hook(logger):
            logger.error("抓取后钩子失败")
            sys.exit(1)
        logger.info("数据爬取完成")
        logger.info("单次执行完成")
        sys.exit(0)
    else:
        logger.error("数据爬取失败")
        sys.exit(1)


def run_scheduled(args):
    """定时调度执行"""
    logger = get_logger(__name__)
    
    logger.info("开始定时调度模式...")
    
    # 设置调度间隔
    if args.interval:
        scheduler.update_interval(args.interval)
    
    # 创建爬取任务函数
    def crawl_task():
        """定时爬取任务（包含项目分析）"""
        logger.info("执行定时爬取任务...")

        # 重置API统计信息（避免累计）
        crawler.api_client.reset_stats()

        # 执行爬取（直接使用配置文件中的list_ids进行并行获取，使用智能时间检测）
        crawl_success = crawler.crawl_tweets(
            list_id=args.list_id,
            list_ids=args.list_ids_list,
            process_code=args.process_code,
            execution_id=build_execution_id(args.process_code),
            max_pages=args.max_pages,
            page_size=args.page_size,
            hours_limit=args.hours_limit
        )

        # 记录本轮 TwitterAPI 成本。先写本地健康统计，再尝试写成本库；失败必须打明原因。
        try:
            import subprocess
            from datetime import datetime
            import os

            api_stats = crawler.api_client.get_request_stats()
            total_cost = float(api_stats.get('total_cost_usd', 0) or 0)
            tweets_fetched = int(api_stats.get('tweets_fetched', 0) or 0)
            total_requests = int(api_stats.get('total_requests', 0) or 0)
            error_count = int(api_stats.get('error_count', 0) or 0)
            task_name = args.process_code or 'tweet_crawler'
            list_ids_text = args.list_ids or args.list_id or ''
            run_id = build_execution_id(task_name)

            health_monitor.record_crawl_stats({
                'timestamp': datetime.now().isoformat(),
                'process_code': task_name,
                'list_ids': list_ids_text,
                'total_tweets': tweets_fetched,
                'valid_tweets': tweets_fetched,
                'invalid_tweets': 0,
                'twitter_api_cost': total_cost,
                'ai_api_cost': 0,
                'total_cost': total_cost,
                'total_requests': total_requests,
                'error_count': error_count,
            })
            logger.info(
                f"本轮成本记录: process_code={task_name}, requests={total_requests}, "
                f"tweets={tweets_fetched}, twitter_api_cost=${total_cost:.6f}, ai_api_cost=$0.000000"
            )

            script_dir = os.path.dirname(os.path.abspath(__file__))
            logger_path = os.path.join(script_dir, 'src', 'utils', 'cost_db_logger.py')
            venv_python = os.path.join(script_dir, 'venv', 'bin', 'python')

            if os.path.exists(logger_path) and os.path.exists(venv_python):
                cmd = [
                    venv_python, logger_path,
                    '--task-name', task_name,
                    '--run-id', run_id,
                    '--total-requests', str(total_requests),
                    '--total-cost', f"{total_cost:.6f}",
                    '--tweets-fetched', str(tweets_fetched),
                    '--error-count', str(error_count),
                ]
                if list_ids_text:
                    cmd.extend(['--list-ids', list_ids_text])

                result = subprocess.run(cmd, cwd=script_dir, capture_output=True, text=True)
                if result.returncode == 0:
                    logger.info("✓ 成本数据已自动记录到数据库")
                else:
                    logger.warning(
                        f"成本数据库记录失败 exit={result.returncode}: "
                        f"{(result.stderr or result.stdout).strip()}"
                    )
            else:
                logger.warning("成本数据库记录脚本或 venv Python 不存在，已仅写入本地健康统计")
        except Exception as e:
            logger.warning(f"记录成本数据失败（不影响主流程）: {e}")

        if crawl_success:
            if getattr(crawler, 'last_saved_tweet_count', 0) <= 0:
                logger.info("本轮没有新增有效推文，跳过抓取后钩子和项目分析")
                return True

            logger.info("爬取完成，跳过抓取后钩子和项目分析：推文爬虫保持纯抓取链路")

        return crawl_success
    
    # 设置调度器
    scheduler.set_crawler(crawl_task)
    
    # 先执行一次测试
    logger.info("执行连接测试...")
    if not (crawler.test_connection() and crawler.test_api_connection()):
        logger.error("连接测试失败，无法启动定时调度")
        sys.exit(1)
    
    # 启动定时调度
    scheduler.start_crawling()
    
    logger.info("定时调度已启动，按 Ctrl+C 停止")
    
    try:
        # 主循环，定期显示状态
        while True:
            time.sleep(300)  # 每5分钟显示一次状态
            
            # 显示调度器状态
            scheduler_status = scheduler.get_status()
            logger.info(f"调度器状态: 运行中={scheduler_status['is_running']}, "
                       f"任务数={scheduler_status['task_count']}, "
                       f"成功率={scheduler_status['success_rate']:.1f}%")
            
            # 显示爬虫统计
            crawler_stats = crawler.get_statistics()
            logger.info(f"爬虫统计: 成功={crawler_stats['success_count']}, "
                       f"失败={crawler_stats['error_count']}, "
                       f"数据库推文数={crawler_stats['database_tweet_count']}")
    
    except KeyboardInterrupt:
        logger.info("接收到停止信号...")
        scheduler.stop()


def run_topic_analysis(args):
    """运行话题分析 - 已移除，请使用独立的话题分析脚本"""
    logger = get_logger(__name__)
    
    max_tweets = args.max_pages * (args.page_size or 10) if args.max_pages and args.page_size else 20
    if not topic_engine.analyze_recent_tweets(hours=24, max_tweets=max_tweets):
        raise RuntimeError("topic analysis failed")
    logger.info(topic_engine.get_topic_statistics())
    return
    logger.warning("话题分析功能已从主服务中移除")
    logger.info("请使用以下独立服务:")
    logger.info("- ./start_topic_service.sh start  # 独立的话题分析服务")
    logger.info("- python main.py --mode topic     # 仅在需要时手动运行")
    
    # 功能已移除
    # logger.info("开始话题分析模式...")
    # 
    # # 分析现有推文数据
    # max_tweets = args.max_pages * (args.page_size or 10) if args.max_pages and args.page_size else 20
    # 
    # success = topic_engine.analyze_recent_tweets(hours=24, max_tweets=max_tweets)
    # 
    # # 显示分析结果
    # stats = topic_engine.get_topic_statistics()
    # logger.info("=" * 30)
    # logger.info("话题分析统计:")
    # for key, value in stats.items():
    #     if key != 'hot_topics_sample':  # 跳过复杂的嵌套数据
    #         logger.info(f"{key}: {value}")
    # 
    # # 显示生成的话题
    # from src.database.topic_dao import topic_dao
    # topic_count = topic_dao.get_topic_count()
    # if topic_count > 0:
    #     hot_topics = topic_dao.get_hot_topics(limit=3)
    #     logger.info("\n最新热门话题:")
    #     for i, topic in enumerate(hot_topics, 1):
    #         logger.info(f"{i}. {topic.topic_name} (热度: {topic.popularity})")
    # 
    # logger.info("=" * 30)
    # 
    # if success:
    #     logger.info("话题分析完成")
    #     sys.exit(0)
    # else:
    #     logger.error("话题分析失败")
    #     sys.exit(1)
    
    sys.exit(0)


# KOL分析功能已禁用
# def run_kol_analysis(args):
#     """运行KOL分析"""
#     logger = get_logger(__name__)
#     logger.warning("KOL分析功能已禁用")
#     sys.exit(0)


def run_project_analysis(args):
    """运行项目分析"""
    from src.project_engine import project_engine
    logger = get_logger(__name__)
    
    logger.info("开始项目分析模式...")
    
    # 分析参数
    hours = 24  # 分析最近24小时
    max_tweets = args.max_pages * (args.page_size or 10) if args.max_pages and args.page_size else 50
    
    success = project_engine.analyze_recent_tweets(hours=hours, max_tweets=max_tweets)
    
    # 显示分析结果
    stats = project_engine.get_project_statistics()
    logger.info("=" * 30)
    logger.info("项目分析统计:")
    for key, value in stats.items():
        if key not in ['category_distribution', 'sentiment_distribution', 'hot_projects', 'chatgpt_stats']:
            logger.info(f"{key}: {value}")
    
    # 显示热门项目
    hot_projects = stats.get('hot_projects', [])
    if hot_projects:
        logger.info("\n热门项目:")
        for i, project in enumerate(hot_projects, 1):
            logger.info(f"{i}. {project['name']} ({project['symbol']}) - 热度: {project['popularity']}, 情感: {project['sentiment']}")
    
    # 显示分类分布
    category_dist = stats.get('category_distribution', {})
    if category_dist:
        logger.info("\n项目分类分布:")
        for category, count in category_dist.items():
            if count > 0:
                logger.info(f"  {category}: {count} 个项目")
    
    logger.info("=" * 30)
    
    if success:
        logger.info("项目分析完成")
        sys.exit(0)
    else:
        logger.error("项目分析失败")
        sys.exit(1)


def run_project_once(args):
    """单次执行项目推文爬取"""
    logger = get_logger(__name__)

    logger.info("开始单次项目推文数据爬取...")

    # 设置使用项目推文专用表
    from src.database.tweet_dao import tweet_dao
    tweet_dao.table_name = 'twitter_tweet_back_test_cmc300'
    logger.info(f"使用数据表: {tweet_dao.table_name}")

    # 执行爬取（使用配置文件中的list_ids_project进行并行获取，使用智能时间检测）
    success = crawler.crawl_project_tweets(
        max_pages=args.max_pages,
        page_size=args.page_size,
        hours_limit=args.hours_limit
    )

    # 显示爬取统计信息
    stats = crawler.get_statistics()
    logger.info("=" * 30)
    logger.info("项目推文爬取统计:")
    for key, value in stats.items():
        logger.info(f"{key}: {value}")
    logger.info("=" * 30)

    if success:
        logger.info("项目推文数据爬取完成")
        logger.info("单次执行完成")
        sys.exit(0)
    else:
        logger.error("项目推文数据爬取失败")
        sys.exit(1)


def run_project_scheduled(args):
    """定时调度执行项目推文爬取"""
    logger = get_logger(__name__)

    logger.info("开始项目推文定时调度模式...")

    # 设置使用项目推文专用表
    from src.database.tweet_dao import tweet_dao
    tweet_dao.table_name = 'twitter_tweet_back_test_cmc300'
    logger.info(f"使用数据表: {tweet_dao.table_name}")

    # 设置调度间隔
    if args.interval:
        scheduler.update_interval(args.interval)
    
    # 创建爬取任务函数
    def crawl_project_task():
        """定时项目推文爬取任务"""
        logger.info("执行定时项目推文爬取任务...")
        
        # 执行爬取（使用配置文件中的list_ids_project进行并行获取，使用智能时间检测）
        crawl_success = crawler.crawl_project_tweets(
            max_pages=args.max_pages,
            page_size=args.page_size,
            hours_limit=args.hours_limit
        )
        
        if crawl_success:
            logger.info("项目推文爬取完成")
        else:
            logger.warning("项目推文爬取失败")
        
        return crawl_success
    
    # 设置调度器
    scheduler.set_crawler(crawl_project_task)
    
    # 先执行一次测试
    logger.info("执行连接测试...")
    if not (crawler.test_connection() and crawler.test_api_connection()):
        logger.error("连接测试失败，无法启动定时调度")
        sys.exit(1)
    
    # 启动定时调度
    scheduler.start_crawling()
    
    logger.info("项目推文定时调度已启动，按 Ctrl+C 停止")
    
    try:
        # 主循环，定期显示状态
        while True:
            time.sleep(300)  # 每5分钟显示一次状态
            
            # 显示调度器状态
            scheduler_status = scheduler.get_status()
            logger.info(f"调度器状态: 运行中={scheduler_status['is_running']}, "
                       f"任务数={scheduler_status['task_count']}, "
                       f"成功率={scheduler_status['success_rate']:.1f}%")
            
            # 显示爬虫统计
            crawler_stats = crawler.get_statistics()
            logger.info(f"爬虫统计: 成功={crawler_stats['success_count']}, "
                       f"失败={crawler_stats['error_count']}, "
                       f"数据库推文数={crawler_stats['database_tweet_count']}")
    
    except KeyboardInterrupt:
        logger.info("接收到停止信号...")
        scheduler.stop()


def run_update_metrics(args):
    """运行历史推文指标更新"""
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info(f"🔄 开始更新历史推文指标 (范围: 过去 {args.days} 天)")
    logger.info("=" * 60)

    # 设置使用项目推文专用表
    from src.database.tweet_dao import tweet_dao
    tweet_dao.table_name = 'twitter_tweet_back_test_cmc300'
    logger.info(f"操作数据表: {tweet_dao.table_name}")

    # 执行更新
    success = crawler.update_historical_metrics(days=args.days)

    if success:
        logger.info("✅ 历史推文指标更新成功")
        sys.exit(0)
    else:
        logger.error("❌ 历史推文指标更新失败")
        sys.exit(1)


def run_update_metrics_daemon(args):
    """守护进程模式运行历史推文指标更新（每天服务器4点，即北京时间12点）"""
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info(f"🔄 启动历史推文指标更新守护进程")
    logger.info(f"📅 调度时间: 每天服务器 04:00 (北京 12:00)")
    logger.info(f"📊 更新范围: 过去 {args.days} 天")
    logger.info("=" * 60)

    # 设置使用项目推文专用表
    from src.database.tweet_dao import tweet_dao
    tweet_dao.table_name = 'twitter_tweet_back_test_cmc300'
    logger.info(f"操作数据表: {tweet_dao.table_name}")

    from datetime import datetime, timedelta
    import time

    try:
        while True:
            now = datetime.now()
            # 目标时间：今天的 04:00:00
            target_time = now.replace(hour=4, minute=0, second=0, microsecond=0)

            # 如果现在已经过了 04点，目标设为明天 04点
            if now >= target_time:
                target_time += timedelta(days=1)

            wait_seconds = (target_time - now).total_seconds()
            hours_wait = wait_seconds / 3600

            logger.info(f"⏳ 下次更新时间: {target_time.strftime('%Y-%m-%d %H:%M:%S')} (等待约 {hours_wait:.2f} 小时)")

            # 休眠直到目标时间
            # 为了能够响应中断，我们分段休眠
            while wait_seconds > 0:
                sleep_chunk = min(wait_seconds, 60) # 每次最多睡60秒
                time.sleep(sleep_chunk)
                wait_seconds -= sleep_chunk

                # 可选：每隔1小时打印一次心跳
                # if int(wait_seconds) % 3600 == 0 and wait_seconds > 60:
                #    logger.info(f"心跳: 距离更新还有 {wait_seconds/3600:.1f} 小时")

            # 时间到，执行任务
            logger.info("⏰ 到达预定时间，开始执行更新任务...")
            success = crawler.update_historical_metrics(days=args.days)

            if success:
                logger.info("✅ 本次定时更新完成")
            else:
                logger.error("❌ 本次定时更新失败")

            # 循环继续，计算明天的目标时间

    except KeyboardInterrupt:
        logger.info("守护进程接收到停止信号，正在退出...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"守护进程发生异常: {e}")
        sys.exit(1)


def run_health_check():
    """运行健康检查"""
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info("🏥 爬虫健康检查")
    logger.info("=" * 60)

    # 执行健康检查
    health_status = health_monitor.check_health()

    # 显示健康状态
    if health_status['is_healthy']:
        logger.info("✅ 状态: 健康")
    else:
        logger.warning("⚠️  状态: 异常")

    # 显示报警
    if health_status['alerts']:
        logger.info("\n🚨 报警:")
        for alert in health_status['alerts']:
            logger.error(f"  {alert}")

    # 显示警告
    if health_status['warnings']:
        logger.info("\n⚠️  警告:")
        for warning in health_status['warnings']:
            logger.warning(f"  {warning}")

    # 显示成本摘要（最近24小时）
    cost_summary = health_monitor.get_cost_summary(hours=24)
    logger.info("\n💰 成本摘要（最近24小时）:")
    logger.info(f"  Twitter API: ${cost_summary.get('twitter_api_cost', 0):.4f}")
    logger.info(f"  AI API: ${cost_summary.get('ai_api_cost', 0):.4f}")
    logger.info(f"  总计: ${cost_summary.get('total_cost', 0):.4f}")
    logger.info(f"  请求次数: {cost_summary.get('request_count', 0)}")

    logger.info("\n" + "=" * 60)

    # 如果需要停止服务，退出码为1
    sys.exit(1 if health_status['should_stop'] else 0)


def run_cost_stats(args):
    """运行成本统计"""
    logger = get_logger(__name__)

    period_hours = args.cost_period

    logger.info("=" * 60)
    logger.info(f"💰 API成本统计（最近{period_hours}小时）")
    logger.info("=" * 60)

    # 获取成本摘要
    cost_summary = health_monitor.get_cost_summary(hours=period_hours)

    if cost_summary.get('request_count', 0) == 0:
        logger.info("\n⚠️  暂无统计数据")
    else:
        logger.info(f"\n📊 统计周期: 最近 {period_hours} 小时")
        logger.info(f"📈 请求次数: {cost_summary.get('request_count', 0)} 次")
        logger.info(f"\n💵 成本明细:")
        logger.info(f"  • Twitter API: ${cost_summary.get('twitter_api_cost', 0):.4f}")
        logger.info(f"  • AI API: ${cost_summary.get('ai_api_cost', 0):.4f}")
        logger.info(f"  • 总计: ${cost_summary.get('total_cost', 0):.4f}")
        logger.info(f"\n📉 平均成本:")
        logger.info(f"  • 每次请求: ${cost_summary.get('avg_cost_per_request', 0):.4f}")

        # 预估每日/每月成本
        if period_hours > 0:
            daily_estimate = cost_summary.get('total_cost', 0) * (24 / period_hours)
            monthly_estimate = daily_estimate * 30
            logger.info(f"\n📅 成本预估:")
            logger.info(f"  • 每日: ${daily_estimate:.2f}")
            logger.info(f"  • 每月: ${monthly_estimate:.2f}")

    logger.info("\n" + "=" * 60)

    sys.exit(0)


def _parse_predx_raw_list_ids(args):
    if getattr(args, 'list_ids_list', None):
        return args.list_ids_list
    raw = os.getenv("PREDX_RAW_LIST_IDS", "")
    if raw:
        ids = [item.strip() for item in raw.split(",") if item.strip()]
        if ids:
            return ids
    return PREDX_RAW_LIST_IDS


def _use_predx_raw_tweets_table(logger) -> None:
    from src.database.tweet_dao import tweet_dao
    tweet_dao.table_name = 'predx_raw_tweets'
    crawler.tweet_dao.table_name = 'predx_raw_tweets'
    logger.info("PredX raw 推文目标表: predx_raw_tweets")


def run_predx_raw_once(args):
    """单次抓取 PredX 三个 List，写入 predx_raw_tweets。"""
    logger = get_logger(__name__)
    _use_predx_raw_tweets_table(logger)
    list_ids = _parse_predx_raw_list_ids(args)
    logger.info(f"开始 PredX raw 推文单次抓取: list_count={len(list_ids)}, hours_limit={args.hours_limit}")

    success = crawler.crawl_tweets(
        list_ids=list_ids,
        process_code=args.process_code,
        execution_id=build_execution_id(args.process_code),
        max_pages=args.max_pages,
        page_size=args.page_size,
        hours_limit=args.hours_limit,
    )

    if success:
        logger.info("PredX raw 推文单次抓取完成")
        sys.exit(0)
    logger.error("PredX raw 推文单次抓取失败")
    sys.exit(1)


def run_predx_raw_scheduled(args):
    """定时抓取 PredX 三个 List，写入 predx_raw_tweets。"""
    logger = get_logger(__name__)
    _use_predx_raw_tweets_table(logger)
    list_ids = _parse_predx_raw_list_ids(args)
    logger.info(f"开始 PredX raw 推文定时抓取: list_count={len(list_ids)}, interval={args.interval}m, hours_limit={args.hours_limit}")

    if args.interval:
        scheduler.update_interval(args.interval)

    def crawl_predx_raw_task():
        logger.info("执行 PredX raw 推文抓取任务...")
        crawler.api_client.reset_stats()
        return crawler.crawl_tweets(
            list_ids=list_ids,
            process_code=args.process_code,
            execution_id=build_execution_id(args.process_code),
            max_pages=args.max_pages,
            page_size=args.page_size,
            hours_limit=args.hours_limit,
        )

    scheduler.set_crawler(crawl_predx_raw_task)

    logger.info("执行连接测试...")
    if not (crawler.test_connection() and crawler.test_api_connection()):
        logger.error("连接测试失败，无法启动 PredX raw 定时调度")
        sys.exit(1)

    scheduler.start_crawling()
    logger.info("PredX raw 定时调度已启动，按 Ctrl+C 停止")

    try:
        while True:
            time.sleep(300)
            scheduler_status = scheduler.get_status()
            logger.info(
                f"PredX raw 调度器状态: 运行中={scheduler_status['is_running']}, "
                f"任务数={scheduler_status['task_count']}, 成功率={scheduler_status['success_rate']:.1f}%"
            )
    except KeyboardInterrupt:
        logger.info("接收到停止信号...")
        scheduler.stop()


def cleanup():
    """清理资源"""
    logger = get_logger(__name__)
    
    try:
        # 停止调度器
        if scheduler.is_running:
            scheduler.stop()
        
        # 关闭爬虫
        crawler.close()
        
        logger.info("资源清理完成")
        
    except Exception as e:
        logger.error(f"清理资源时出错: {e}")


if __name__ == '__main__':
    main() 