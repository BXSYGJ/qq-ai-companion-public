import asyncio
import copy
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import qq_bot as q


def make_bot():
    cfg = copy.deepcopy(q.DEFAULTS)
    cfg["memory"] = {"persist": False}
    cfg["qq"]["allow_users"] = [42]
    cfg["behavior"].update(
        typing_indicator=False, followup_enabled=False, topic_push=False,
        regenerate_max=2, reply_delay_ms=0, segment_delay_ms=[0, 0],
        first_segment_delay_ms=[0, 0],
    )
    bot = q.QQBot(cfg, mock=True)
    bot.action = AsyncMock(return_value={"status": "ok", "retcode": 0})
    bot._wait_until_peer_ready = AsyncMock(return_value=True)
    return bot


class ConversationTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_limit_preserves_original_and_new_messages_in_order(self):
        bot = make_bot()
        calls = 0

        async def generate(key, text, sender, **kwargs):
            nonlocal calls
            calls += 1
            bot._chat_queue.setdefault(key, []).append(f"new-{calls}")
            return f"draft-{calls}"

        bot.generate = AsyncMock(side_effect=generate)
        await bot._respond("u:42", "original", object(), "send_private_msg", {"user_id": 42}, {})
        self.assertEqual(bot._chat_queue["u:42"], ["original", "new-1", "new-2"])
        self.assertEqual(bot._history.get("u:42", []), [])
        bot.action.assert_not_awaited()

        merged = "\n".join(bot._take_queued("u:42"))
        bot.generate = AsyncMock(return_value="all addressed")
        await bot._respond("u:42", merged, object(), "send_private_msg", {"user_id": 42}, {}, burst=3)
        self.assertEqual(bot._history["u:42"][0]["content"], "original\nnew-1\nnew-2")
        self.assertEqual(bot._history["u:42"][1]["content"], "all addressed")

    async def test_message_during_coverage_rewrite_regenerates_before_sending(self):
        bot = make_bot()
        calls = 0

        async def generate(key, text, sender, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                bot._chat_queue.setdefault(key, []).append("new question")
                return "stale rewrite"
            return "final answer" if calls == 3 else "initial answer"

        bot.generate = AsyncMock(side_effect=generate)
        bot._coverage_check = AsyncMock(side_effect=["second topic", ""])
        await bot._respond("u:42", "first\nsecond", object(), "send_private_msg", {"user_id": 42}, {}, burst=2)
        self.assertEqual(calls, 3)
        self.assertEqual(bot.generate.await_args_list[1].kwargs["coverage_hint"], "second topic")
        self.assertIn("new question", bot.generate.await_args_list[2].args[1])
        self.assertEqual(bot._history["u:42"][1]["content"], "final answer")
        self.assertEqual(bot.action.await_count, 1)

    async def test_new_message_immediately_before_first_segment_requeues_turn(self):
        bot = make_bot()
        calls = 0

        async def ready(key):
            nonlocal calls
            calls += 1
            if calls == 2:
                bot._chat_queue.setdefault(key, []).append("late")
            return True

        bot._wait_until_peer_ready = AsyncMock(side_effect=ready)
        bot.generate = AsyncMock(return_value="a complete response")
        await bot._respond("u:42", "original", object(), "send_private_msg", {"user_id": 42}, {})
        self.assertEqual(bot._chat_queue["u:42"], ["original", "late"])
        self.assertEqual(bot._history.get("u:42", []), [])
        bot.action.assert_not_awaited()

    async def test_new_message_between_segments_stops_and_saves_only_delivered_text(self):
        bot = make_bot()
        bot.generate = AsyncMock(return_value="first part\nsecond part")

        async def action(ws, action, params, **kwargs):
            bot._chat_queue.setdefault("u:42", []).append("interruption")
            return {"status": "ok", "retcode": 0}

        bot.action = AsyncMock(side_effect=action)
        await bot._respond("u:42", "original", object(), "send_private_msg", {"user_id": 42}, {})
        self.assertEqual(bot.action.await_count, 1)
        self.assertEqual(bot._history["u:42"][1]["content"], "first part")
        self.assertEqual(bot._chat_queue["u:42"], ["interruption"])
        self.assertIn("没有完整发出", bot._build_messages("u:42", "next")[0]["content"])

    async def test_typing_timeout_stops_next_segment(self):
        bot = make_bot()
        bot.generate = AsyncMock(return_value="first part\nsecond part")
        bot._wait_until_peer_ready = AsyncMock(side_effect=[True, True, False])
        await bot._respond("u:42", "original", object(), "send_private_msg", {"user_id": 42}, {})
        self.assertEqual(bot.action.await_count, 1)
        self.assertEqual(bot._history["u:42"][1]["content"], "first part")

    async def test_partial_delivery_failure_is_not_followed_up(self):
        bot = make_bot()
        bot.generate = AsyncMock(return_value="first part\nsecond part")
        bot.action = AsyncMock(side_effect=[{"status": "ok", "retcode": 0},
                                            {"status": "failed", "retcode": 100}])
        bot._arm_followup = unittest.mock.Mock()
        await bot._respond("u:42", "original", object(), "send_private_msg", {"user_id": 42}, {})
        self.assertEqual(bot._history["u:42"][1]["content"], "first part")
        bot._arm_followup.assert_not_called()

    async def test_coverage_review_returns_only_explicit_missing_items(self):
        bot = make_bot()
        bot.mock = False
        bot._simple_chat = AsyncMock(return_value=json.dumps({"complete": False, "missing": ["second question"]}))
        bot._history["u:42"] = [{"role": "user", "content": "earlier context"}]
        self.assertEqual(await bot._coverage_check("u:42", "first? second?", "first answer"), "second question")
        self.assertIn("候选回复", bot._simple_chat.await_args.args[0][0]["content"])
        self.assertIn("earlier context", bot._simple_chat.await_args.args[0][0]["content"])

    async def test_coverage_failure_falls_back_to_draft(self):
        bot = make_bot()
        bot.mock = False
        bot._simple_chat = AsyncMock(side_effect=["not json", RuntimeError("offline")])
        self.assertEqual(await bot._coverage_check("u:42", "a? b?", "draft"), "")
        self.assertEqual(await bot._coverage_check("u:42", "a? b?", "draft"), "")

    async def test_coverage_timeout_falls_back_to_draft(self):
        bot = make_bot()
        bot.mock = False
        bot.cfg["behavior"]["coverage_timeout_seconds"] = 0.01
        async def slow(*args, **kwargs):
            await asyncio.sleep(1)
        bot._simple_chat = AsyncMock(side_effect=slow)
        self.assertEqual(await bot._coverage_check("u:42", "a? b?", "draft"), "")

    async def test_worker_retries_requeued_turn_without_losing_new_messages(self):
        bot = make_bot()
        ws = object()
        calls = 0

        async def generate(key, text, sender, **kwargs):
            nonlocal calls
            calls += 1
            if calls <= 2:
                bot._enqueue_chat(key, f"extra-{calls}", ws, "send_private_msg", {"user_id": 42}, {})
            return "complete answer"

        bot.generate = AsyncMock(side_effect=generate)
        with patch.object(q, "sample_distribution", return_value=0.01):
            bot._enqueue_chat("u:42", "original", ws, "send_private_msg", {"user_id": 42}, {})
            await asyncio.wait_for(bot._chat_workers["u:42"], timeout=3)
        self.assertEqual(calls, 3)
        self.assertEqual(bot._history["u:42"][0]["content"], "original\nextra-1\nextra-2")
        self.assertFalse(bot._chat_queue.get("u:42"))
        self.assertEqual(bot.action.await_count, 1)

    def test_multi_question_detection_and_prompt(self):
        self.assertTrue(q.needs_coverage_check("今天怎么样？明天有空吗？"))
        self.assertTrue(q.needs_coverage_check("第一件事\n第二件事"))
        self.assertFalse(q.needs_coverage_check("今天怎么样？"))
        bot = make_bot()
        messages = bot._build_messages("u:42", "今天怎么样？明天有空吗？", "明天的安排")
        self.assertIn("不同的问题", messages[0]["content"])
        self.assertIn("明天的安排", messages[0]["content"])


if __name__ == "__main__":
    unittest.main()
