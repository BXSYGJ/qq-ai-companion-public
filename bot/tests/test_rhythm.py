import asyncio
import copy
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import qq_bot as q


def make_bot():
    cfg = copy.deepcopy(q.DEFAULTS)
    cfg["memory"] = {"persist": False}
    cfg["qq"]["allow_users"] = [42]
    cfg["proactive"]["quiet_hours"] = []
    cfg["behavior"].update(reply_delay_ms=0, typing_indicator=False,
                           followup_probability=1, followup_second_probability=1)
    bot = q.QQBot(cfg, mock=True)
    bot.action = AsyncMock(return_value={"status": "ok", "retcode": 0})
    return bot


class DistributionTests(unittest.TestCase):
    def test_bounds_and_mode(self):
        q.random.seed(271828)
        samples = [q.sample_distribution([3.5, 5, 8], (3.5, 5, 8)) for _ in range(30000)]
        self.assertTrue(all(3.5 <= x <= 8 for x in samples))
        counts = [sum(a <= x < a + 0.3 for x in samples) for a in (3.5, 4.85, 7.7)]
        self.assertGreater(counts[1], counts[0] * 2)
        self.assertGreater(counts[1], counts[2] * 2)

    def test_invalid_configuration_falls_back(self):
        for value in (None, [], [3, 2, 1], [0, 0, 1], ['x', 2, 3], [0, 1, float('inf')]):
            self.assertTrue(3.5 <= q.sample_distribution(value, (3.5, 5, 8)) <= 8)

    def test_daily_qzone_schedule_is_stable_and_bounded(self):
        bot = make_bot()
        now = datetime(2026, 11, 10, 9)
        first = bot._qzone_time_for(now, {"time_distribution": [20, 22, 24]})
        self.assertEqual(first, bot._qzone_time_for(now, {"time_distribution": [20, 22, 24]}))
        self.assertTrue(20 <= first.hour < 24)
        second = bot._qzone_time_for(datetime(2026, 11, 11, 9), {"time_distribution": [1, 2, 3]})
        self.assertEqual(second.day, 11)
        self.assertTrue(20 <= second.hour < 24)

    def test_schedule_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            cfg = copy.deepcopy(q.DEFAULTS)
            cfg['memory'] = {'persist': True, 'dir': directory}
            first_bot = q.QQBot(cfg, mock=True)
            now = datetime(2026, 11, 10, 9)
            scheduled = first_bot._qzone_time_for(now, {'time_distribution': [20, 22, 24]})
            second_bot = q.QQBot(cfg, mock=True)
            self.assertEqual(scheduled, second_bot._qzone_time_for(now, {}))


class RhythmTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_followups_retain_unanswered_context(self):
        bot = make_bot()
        ws = object()
        bot._clients.add(ws)
        bot._history['u:42'] = [{"role": "assistant", "content": "first"}]
        bot.generate = AsyncMock(side_effect=['follow one', 'follow two'])
        with patch.object(q, 'sample_distribution', return_value=0), patch.object(q.asyncio, 'sleep', new=AsyncMock()), patch.object(q.random, 'random', return_value=0):
            await bot._followup_after('u:42', ws, 'send_private_msg', {'user_id': 42})
        self.assertEqual(bot.action.await_count, 2)
        self.assertEqual([x['content'] for x in bot._history['u:42']], ['first', 'follow one', 'follow two'])
        self.assertIn('没有回复', bot.generate.await_args_list[1].args[1])
        self.assertIn('第 2 次', bot.generate.await_args_list[1].args[1])

    async def test_first_followup_skipped_means_no_second(self):
        bot = make_bot()
        bot.cfg['behavior']['followup_probability'] = 0
        ws = object()
        bot._clients.add(ws)
        bot._history['u:42'] = [{"role": "assistant", "content": "first"}]
        bot.generate = AsyncMock()
        with patch.object(q, 'sample_distribution', return_value=0), patch.object(q.asyncio, 'sleep', new=AsyncMock()):
            await bot._followup_after('u:42', ws, 'send_private_msg', {'user_id': 42})
        bot.generate.assert_not_awaited()
        bot.action.assert_not_awaited()

    async def test_inbound_during_generation_prevents_send(self):
        bot = make_bot()
        ws = object()
        bot._clients.add(ws)
        bot._history['u:42'] = [{"role": "assistant", "content": "first"}]
        async def generate(*args):
            bot._mark_activity('u:42')
            return 'stale reply'
        bot.generate = AsyncMock(side_effect=generate)
        with patch.object(q, 'sample_distribution', return_value=0), patch.object(q.asyncio, 'sleep', new=AsyncMock()), patch.object(q.random, 'random', return_value=0):
            await bot._followup_after('u:42', ws, 'send_private_msg', {'user_id': 42})
        bot.action.assert_not_awaited()
        self.assertEqual(len(bot._history['u:42']), 1)

    async def test_muted_and_typing_prevent_initiation(self):
        bot = make_bot()
        ws = object()
        bot._clients.add(ws)
        version = bot._activity_version['u:42']
        bot._muted_until = q.time.time() + 100
        self.assertFalse(bot._can_initiate('u:42', ws, version))
        bot._muted_until = 0
        bot._on_notice({'notice_type': 'notify', 'sub_type': 'input_status', 'user_id': 42, 'event_type': 1})
        self.assertFalse(bot._can_initiate('u:42', ws, version))
        bot._on_notice({'notice_type': 'notify', 'sub_type': 'input_status', 'user_id': 42, 'event_type': 2})
        self.assertTrue(3.5 <= bot._typing_grace['u:42'] <= 8)

    async def test_proactive_evaluation_is_once_per_schedule(self):
        bot = make_bot()
        ws = object()
        bot._clients.add(ws)
        bot._last_active['u:42'] = q.time.time() - 8000
        bot._proactive_due['u:42'] = q.time.time() - 1
        bot._proactive_factor = AsyncMock(return_value=0)
        await bot._proactive_once(bot.cfg['proactive'])
        await bot._proactive_once(bot.cfg['proactive'])
        self.assertEqual(bot._proactive_factor.await_count, 1)
        bot.action.assert_not_awaited()

    async def test_proactive_does_not_send_after_activity_during_evaluation(self):
        bot = make_bot()
        ws = object()
        bot._clients.add(ws)
        bot._last_active['u:42'] = q.time.time() - 8000
        bot._proactive_due['u:42'] = q.time.time() - 1
        async def evaluate(*args):
            bot._mark_activity('u:42')
            return 1
        bot._proactive_factor = AsyncMock(side_effect=evaluate)
        bot.generate = AsyncMock(return_value='hello')
        await bot._proactive_once(bot.cfg['proactive'])
        bot.generate.assert_not_awaited()
        bot.action.assert_not_awaited()

    async def test_model_factor_invalid_or_unavailable_is_zero(self):
        bot = make_bot()
        bot.mock = False
        bot._simple_chat = AsyncMock(side_effect=['{"factor": 5}', '{"factor": "nan"}', RuntimeError('offline')])
        self.assertEqual(await bot._proactive_factor('u:42', 120), 1.5)
        self.assertEqual(await bot._proactive_factor('u:42', 120), 0)
        self.assertEqual(await bot._proactive_factor('u:42', 120), 0)

    async def test_reply_failure_is_not_counted_as_sent(self):
        bot = make_bot()
        bot.action = AsyncMock(return_value={"status": "failed", "retcode": 100})
        sent = await bot._reply(object(), 'send_private_msg', {'user_id': 42}, 'hello')
        self.assertFalse(sent)
        self.assertEqual(bot._stats['replied'], 0)


if __name__ == '__main__':
    unittest.main()
