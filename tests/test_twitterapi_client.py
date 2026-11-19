"""Offline client contract tests: no database or external API calls."""
import importlib.util
import pathlib
import sys
import types
import unittest
from datetime import datetime, timedelta
from unittest import mock


class TwitterAPIClientTests(unittest.TestCase):
    def load_client(self, dedicated=False):
        api = {
            "base_url": "https://api.twitterapi.io",
            "endpoints": {"list_tweets": "/twitter/list/tweets_timeline"},
            "headers": {"X-API-Key": "unit-test-key"},
            "default_params": {"list_id": "list-one", "includeReplies": True},
            "retry_attempts": 3,
            "retry_delay": 0,
        }
        settings = {"api": api}
        if dedicated:
            settings["api_twitterapi"] = dict(api, base_url="https://dedicated.invalid")
        config = mock.Mock()
        config.get_api_config.return_value = api
        config.get.side_effect = lambda key, default=None: settings.get(key, default)
        fake = types.ModuleType("src.utils.config_manager")
        fake.config = config
        source = pathlib.Path(__file__).resolve().parents[1] / "src/api/twitter_api_twitterapi.py"
        spec = importlib.util.spec_from_file_location("src.api._isolated_twitterapi_client", source)
        module = importlib.util.module_from_spec(spec)
        with mock.patch.dict(sys.modules, {"src.utils.config_manager": fake}):
            spec.loader.exec_module(module)
        client = module.TwitterAPITwitterAPIClient()
        self.addCleanup(client.session.close)
        return module, client

    def test_single_api_configuration_uses_twitterapi_endpoint_and_key(self):
        _, client = self.load_client()
        self.assertEqual(client._build_url(), "https://api.twitterapi.io/twitter/list/tweets_timeline")
        self.assertEqual(client.session.headers["X-API-Key"], "unit-test-key")

    def test_dedicated_configuration_remains_supported(self):
        _, client = self.load_client(dedicated=True)
        self.assertEqual(client.base_url, "https://dedicated.invalid")

    def test_list_override_and_legacy_cursor_are_sent_without_page_size(self):
        _, client = self.load_client()
        client._make_request = mock.Mock(return_value={"tweets": []})
        client.fetch_tweets("list-two", pagination_token="cursor-two", max_results=100, count=20)
        _, params = client._make_request.call_args.args
        self.assertEqual(params, {"listId": "list-two", "cursor": "cursor-two"})

    def test_missing_list_does_not_send_a_request(self):
        _, client = self.load_client()
        client.default_params = {}
        client._make_request = mock.Mock()
        self.assertEqual(client.fetch_tweets(), ([], None))
        client._make_request.assert_not_called()

    def test_credit_failure_counts_error_without_retries(self):
        _, client = self.load_client()
        client.session.get = mock.Mock(return_value=mock.Mock(status_code=402, text="Credits is not enough"))
        self.assertIsNone(client._make_request("https://unit.invalid"))
        self.assertEqual(client.session.get.call_count, 1)
        self.assertEqual((client.request_count, client.error_count), (1, 1))

    def test_transient_server_failure_retries_then_succeeds(self):
        module, client = self.load_client()
        success = mock.Mock(status_code=200)
        success.json.return_value = {"tweets": []}
        client.session.get = mock.Mock(side_effect=[mock.Mock(status_code=503), success])
        with mock.patch.object(module.time, "sleep"):
            self.assertEqual(client._make_request("https://unit.invalid"), {"tweets": []})
        self.assertEqual((client.request_count, client.error_count), (2, 0))

    def test_last_seen_stops_before_known_tweet_and_next_page(self):
        _, client = self.load_client()
        client.fetch_tweets = mock.Mock(return_value=([
            {"id_str": "new"}, {"id": "seen"}, {"tweet_id": "older"}
        ], "next"))
        pages = list(client.fetch_tweets_with_pagination(last_seen_tweet_id="seen"))
        self.assertEqual(pages, [[{"id_str": "new"}]])
        client.fetch_tweets.assert_called_once()

    def test_old_tweet_stops_pagination_but_keeps_recent_tweet(self):
        _, client = self.load_client()
        now = datetime.now().astimezone()
        recent = {"id_str": "new", "created_at": now.isoformat()}
        old = {"id_str": "old", "created_at": (now - timedelta(hours=8)).isoformat()}
        client.fetch_tweets = mock.Mock(return_value=([recent, old], "next"))
        pages = list(client.fetch_tweets_with_pagination(hours_limit=3))
        self.assertEqual(pages, [[recent]])
        client.fetch_tweets.assert_called_once()


if __name__ == "__main__":
    unittest.main()
