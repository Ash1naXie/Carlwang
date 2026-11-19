# Production KOL tweet pipeline

The deployment on `linx-df-db002` uses `/home/centos/Project/Carlwang`.
Only the root `start_service.sh` crawler is enabled. It writes tweets to
`public_data.twitter_tweet`, with the existing user, quotation, and cost records.

## Configuration

`config/config.json` is private runtime configuration and must not be committed.
The tracked `config/config.json.template` contains placeholders only. Fill in the
TwitterAPI.io, Gemini, database, and cost database credentials before first use.
Do not overwrite an existing production configuration with the template.

Use the single `api` section with:

- `base_url`: `https://api.twitterapi.io`
- `endpoints.list_tweets`: `/twitter/list/tweets_timeline`
- `headers.X-API-Key`: the credential belonging to the funded TwitterAPI.io account
- `default_params.list_id`: `1896516371435122886`
- `default_params.list_ids`: `1896516371435122886`, `1995387419634622745`, `1993260268055417065`

The default TwitterAPI client falls back to `api` when `api_twitterapi` is absent.
The generic list selector also reads `api.default_params.list_ids`, so this avoids
duplicating API credentials and list configuration. Runtime JSON credentials are
read directly; the main TwitterAPI client does not automatically replace them
from a shell API-key variable. Legacy TweetScout helper scripts use the
`TWEETSCOUT_API_KEY` environment variable.

## Start and inspect

```bash
cd /home/centos/Project/Carlwang
bash start_service.sh start 60 100 100 3
bash start_service.sh status
bash start_service.sh logs 50
```

The service starts one cycle immediately, then runs every 60 minutes and keeps a
three-hour tweet window. The page arguments are request limits; the provider
controls the actual number of tweets returned per page.

The only enabled Carlwang cron entry is:

```cron
*/5 * * * * /home/centos/Project/Carlwang/service_monitor.sh >/dev/null 2>&1
```

Project collection and historical metrics updates, the duplicate KOL daemon,
global media, Alpha Address, topic analysis, and Marco are disabled on this host.
Their source scripts remain in the repository for reference; do not enable their
monitors when deploying this single-pipeline configuration. Runtime cron state
is managed on the server and is not applied automatically by a Git checkout.

An HTTP 402 response with `Credits is not enough` belongs to the API key actually
loaded by the client. Check the owning account and the active configuration key;
a different configured key can have a different balance.
