import asyncio
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import qq_bot as q


def make_bot(directory=None):
    cfg = copy.deepcopy(q.DEFAULTS)
    cfg['memory'] = {'persist': bool(directory), 'dir': directory or 'state'}
    cfg['qq']['allow_users'] = [42, 43]
    cfg['proactive']['quiet_hours'] = []
    cfg['behavior'].update(typing_indicator=False, followup_enabled=False, topic_push=False,
                           coverage_check=False, reply_delay_ms=0, segment_delay_ms=[0, 0],
                           first_segment_delay_ms=[0, 0])
    bot = q.QQBot(cfg, mock=True)
    bot.action = AsyncMock(return_value={'status': 'ok', 'retcode': 0})
    bot._wait_until_peer_ready = AsyncMock(return_value=True)
    return bot


def event(mid, text):
    return {'message_id': mid, 'user_id': 42, 'self_id': 99,
            'message_type': 'private', 'message': text}


class ContinuityTests(unittest.IsolatedAsyncioTestCase):
    async def test_relationship_requires_explicit_proposal_and_acceptance(self):
        for text in ('我喜欢你', '你说“我们在一起吧”是什么意思', '要是我们在一起吧', '我爱你'):
            self.assertFalse(q.relationship_proposal(text), text)
        for text in ('我还没想好', '我不愿意', '我愿意吗？', '我愿意做朋友'):
            self.assertFalse(q.relationship_acceptance(text), text)
        bot = make_bot()
        bot.generate = AsyncMock(return_value='我愿意，和你在一起')
        await bot._respond('u:42', '我喜欢你', object(), 'send_private_msg', {'user_id': 42}, {})
        self.assertEqual(bot._relationship_of('u:42'), 'crush')
        await bot._respond('u:42', '我们在一起吧', object(), 'send_private_msg', {'user_id': 42}, {})
        self.assertEqual(bot._relationship_of('u:42'), 'girlfriend')
        self.assertEqual(bot._relationship_of('u:43'), 'crush')

    async def test_partial_relationship_acceptance_does_not_commit(self):
        bot = make_bot()
        bot.generate = AsyncMock(return_value='我愿意\n和你在一起')
        bot.action = AsyncMock(side_effect=[{'status': 'ok', 'retcode': 0},
                                            {'status': 'failed', 'retcode': 100}])
        await bot._respond('u:42', '我们在一起吧', object(), 'send_private_msg', {'user_id': 42}, {})
        self.assertEqual(bot._relationship_of('u:42'), 'crush')

    async def test_public_prompt_excludes_all_private_context(self):
        bot = make_bot()
        bot.persona = 'PRIVATE_PERSONA_SECRET'
        bot._history['qzone:auto'] = [{'role': 'user', 'content': 'PRIVATE_HISTORY_SECRET'}]
        bot._summary['qzone:auto'] = 'PRIVATE_SUMMARY_SECRET'
        bot._events['qzone:auto'] = [{'text': 'PRIVATE_EVENT_SECRET'}]
        bot._proactive_seed = 'PRIVATE_SEED_SECRET'
        contents = json.dumps(bot._build_messages('qzone:auto', '写说说'), ensure_ascii=False)
        self.assertNotIn('PRIVATE_', contents)

    async def test_simultaneous_sessions_do_not_share_reply_flags(self):
        bot = make_bot()
        ready, release = asyncio.Event(), asyncio.Event()
        async def first():
            bot._burst_count = 4
            bot._topic_hint = True
            ready.set()
            await release.wait()
            self.assertEqual(bot._burst_count, 4)
        async def second():
            await ready.wait()
            self.assertEqual(bot._burst_count, 1)
            self.assertFalse(bot._topic_hint)
            self.assertNotIn('他刚才连着发了 4 条', bot._persona_block('u:43'))
            release.set()
        await asyncio.gather(first(), second())

    async def test_event_extraction_validation_persistence_and_update(self):
        with tempfile.TemporaryDirectory() as directory:
            bot = make_bot(directory)
            bot.mock = False
            bot._simple_chat = AsyncMock(return_value=json.dumps([
                {'id': '', 'text': '准备考试', 'status': 'planned', 'evidence': '下周要考试'},
                {'id': '', 'text': '虚构', 'status': 'planned', 'evidence': '没有说过的内容'},
            ], ensure_ascii=False))
            await bot._extract_events('u:42', '我下周要考试，现在有点紧张', 0)
            self.assertEqual(len(bot._events['u:42']), 1)
            ident = bot._events['u:42'][0]['id']
            bot._simple_chat.return_value = json.dumps([
                {'id': ident, 'text': '考试结束', 'status': 'resolved', 'evidence': '考试结束了'}
            ], ensure_ascii=False)
            await bot._extract_events('u:42', '今天考试结束了，感觉还好', 0)
            restored = make_bot(directory)
            self.assertEqual(restored._events['u:42'][0]['status'], 'resolved')
            self.assertEqual(restored._events['u:42'][0]['id'], ident)
            self.assertNotIn('u:43', restored._events)

    async def test_delete_and_reset_invalidate_pending_extraction(self):
        bot = make_bot()
        bot.mock = False
        entered, release = asyncio.Event(), asyncio.Event()
        async def extract(*args, **kwargs):
            entered.set()
            await release.wait()
            return '[{"text":"考试", "status":"planned", "evidence":"下周要考试"}]'
        bot._simple_chat = AsyncMock(side_effect=extract)
        bot._events['u:42'] = [{'id': 'old', 'text': '旧记忆'}]
        task = asyncio.create_task(bot._extract_events('u:42', '我下周要考试，现在有点紧张', 0))
        await entered.wait()
        await bot._handle_message(object(), event(1, '/忘记 1'))
        release.set()
        await task
        self.assertEqual(bot._events['u:42'], [])
        bot._history['u:42'] = [{'role': 'user', 'content': 'old'}]
        bot._summary['u:42'] = 'old summary'
        bot._events['u:42'] = [{'id': 'old'}]
        bot._chat_queue['u:42'] = ['old queued']
        bot._set_relationship('u:42', 'girlfriend')
        await bot._handle_message(object(), event(2, '/new'))
        self.assertEqual(bot._history['u:42'], [])
        self.assertNotIn('u:42', bot._summary)
        self.assertNotIn('u:42', bot._events)
        self.assertNotIn('u:42', bot._chat_queue)
        self.assertEqual(bot._relationship_of('u:42'), 'girlfriend')

    async def test_reset_during_generation_prevents_stale_reply(self):
        bot = make_bot()
        async def generate(*args, **kwargs):
            await bot._handle_message(object(), event(1, '/new'))
            return 'stale'
        bot.generate = AsyncMock(side_effect=generate)
        await bot._respond('u:42', 'old message', object(), 'send_private_msg', {'user_id': 42}, {})
        self.assertEqual(bot._history.get('u:42', []), [])
        self.assertEqual(bot.action.await_count, 1)  # reset acknowledgement only

    async def test_followup_respects_question_and_request_for_space(self):
        bot = make_bot()
        ws = object()
        bot._clients.add(ws)
        bot.cfg['behavior']['followup_probability'] = 1
        bot.generate = AsyncMock(return_value='followup')
        for user, assistant in [('明天见', '明天几点？'), ('我先忙了，晚点聊', '好')]:
            bot._history['u:42'] = [{'role': 'user', 'content': user},
                                    {'role': 'assistant', 'content': assistant}]
            with patch.object(q, 'sample_distribution', return_value=0), patch.object(q.asyncio, 'sleep', new=AsyncMock()):
                await bot._followup_after('u:42', ws, 'send_private_msg', {'user_id': 42})
        bot.generate.assert_not_awaited()

    async def test_compression_keeps_unprocessed_history(self):
        bot = make_bot()
        bot.cfg['llm'].update(context_max_tokens=10, compress_keep_turns=10)
        bot._history['u:42'] = [{'role': 'user', 'content': str(i) + ':' + 'x' * 1000} for i in range(80)]
        bot._simple_chat = AsyncMock(return_value='summary')
        await bot._maybe_compress('u:42')
        self.assertGreater(len(bot._history['u:42']), 20)
        self.assertEqual(bot._history['u:42'][-1]['content'][:3], '79:')
        self.assertIn('summary', bot._summary['u:42'])

    async def test_relationship_survives_restart_and_is_isolated(self):
        with tempfile.TemporaryDirectory() as directory:
            bot = make_bot(directory)
            bot._set_relationship('u:42', 'girlfriend')
            restored = make_bot(directory)
            self.assertEqual(restored._relationship_of('u:42'), 'girlfriend')
            self.assertEqual(restored._relationship_of('u:43'), 'crush')
