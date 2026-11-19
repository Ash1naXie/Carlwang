# alpha_address 运行说明

固定配置：

- `list_id=2045070679889055752`
- `pool_id=alpha_address`
- 原始推文表：`twitter_tweet`
- 池成员表：`twitter_kol_pool_member`
- 池推文索引表：`twitter_kol_pool_tweet`

运行前先确认 Python：

- 如果机器上有 `Carlwang/venv/bin/python`，脚本会直接用它
- 如果没有，先手工指定：

```bash
export CARLWANG_PYTHON_BIN=/绝对路径/python
export BET_KOL_PYTHON_BIN=/绝对路径/python
```

`BET_KOL_PYTHON_BIN` 可以省略；省略时会优先复用 `CARLWANG_PYTHON_BIN`

## 初始化

```bash
cd /Users/carlos/Documents/twitter_pipeline/Carlwang/daily_kol_tweet_crawler
./start_service_alpha_address.sh bootstrap
```

这一步会做两件事：

1. 直接从 X List 拉成员，同步到 `twitter_kol_pool_member where pool_id='alpha_address'`
2. 跑一次单次抓取，并在抓完后自动回填 `twitter_kol_pool_tweet`

## 常驻抓取

```bash
cd /Users/carlos/Documents/twitter_pipeline/Carlwang/daily_kol_tweet_crawler
./start_service_alpha_address.sh start
```

默认参数：

- 间隔：20 分钟
- 最大页数：5
- 每页：20
- 时间窗口：0.5 小时
- 索引回填窗口：48 小时

服务每轮执行：

1. `main.py --mode schedule --list-id 2045070679889055752`
2. 抓取成功后自动执行 `backfill_kol_pool_tweets.py --pool-id alpha_address`

## 常用命令

```bash
./start_service_alpha_address.sh status
./start_service_alpha_address.sh logs 100
./start_service_alpha_address.sh sync-pool
./start_service_alpha_address.sh reindex 168
./start_service_alpha_address.sh stop
```

## 首次上线顺序

```bash
cd /Users/carlos/Documents/twitter_pipeline/Carlwang/daily_kol_tweet_crawler
./start_service_alpha_address.sh sync-pool
./start_service_alpha_address.sh bootstrap
./start_service_alpha_address.sh start
```

如果环境缺包，脚本会直接报错，不会偷偷换解释器继续跑。

## 提炼查询

```sql
SELECT t.*
FROM twitter_kol_pool_tweet pt
JOIN twitter_tweet t ON t.id_str = pt.tweet_id
WHERE pt.pool_id = 'alpha_address'
ORDER BY t.created_at_datetime DESC;
```
