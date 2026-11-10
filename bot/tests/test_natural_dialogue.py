import asyncio
import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import qq_bot as q


def make_bot():
    cfg = copy.deepcopy(q.DEFAULTS)
    cfg['memory'] = {'persist': False}
    cfg['behavior'].update(typing_indicator=False, followup_enabled=False, topic_push=True,
                           coverage_check=False, reply_delay_ms=0, segment_delay_ms=[0, 0],
                           first_segment_delay_ms=[0, 0], regenerate_max=2)
    bot = q.QQBot(cfg, mock=True)
    bot.action = AsyncMock(return_value={'status': 'ok', 'retcode': 0})
    bot._wait_until_peer_ready = AsyncMock(return_value=True)
    return bot


class NaturalDialogueTests(unittest.IsolatedAsyncioTestCase):
    def test_closing_detection_does_not_suppress_a_question(self):
        for text in ('谢谢你理解', '先去洗澡，等会儿聊', '晚安'):
            self.assertTrue(q.is_conversation_closing(text), text)
        for text in ('谢谢，你觉得下一步怎么办？', '我去图书馆看到一本书', '你要去吃饭吗？'):
            self.assertFalse(q.is_conversation_closing(text), text)

    def test_acknowledgement_detection_is_narrow(self):
        self.assertTrue(q.is_acknowledgement('哈哈哈'))
        self.assertTrue(q.is_acknowledgement('嗯，好呀'))
        self.assertFalse(q.is_acknowledgement('哈哈你说得对'))
        self.assertFalse(q.is_acknowledgement(''))
        self.assertFalse(q.is_acknowledgement('嗯？'))

    def test_event_json_tolerates_prose_and_multiple_arrays(self):
        self.assertEqual(q.first_json_array('```json\n[{"text":"比赛"}]\n```\n[]'), [{'text': '比赛'}])
        self.assertEqual(q.first_json_array('[]\n[]'), [])
        with self.assertRaises(ValueError):
            q.first_json_array('没有记录')

    def test_quiet_turn_rewrite_is_narrow(self):
        self.assertFalse(q.is_question('好呀'))
        self.assertTrue(q.quiet_turn_needs_rewrite('哈哈哈', '笑什么呀'))
        self.assertTrue(q.quiet_turn_needs_rewrite('谢谢你', '回来跟我说说'))
        self.assertTrue(q.quiet_turn_needs_rewrite('只想吐槽一下', '我今天也是这样的'))
        self.assertFalse(q.quiet_turn_needs_rewrite('只想吐槽一下', '嗯，你说，我听着'))
        self.assertFalse(q.quiet_turn_needs_rewrite('你今天过得怎么样？', '你呢？'))
        self.assertFalse(q.is_conversation_closing('谢谢你今天有空吗'))

    def test_ambiguous_support_is_not_a_concrete_task(self):
        for text in ('我好累，不知道怎么办了', '帮帮我，我现在特别无助', '怎么办？'):
            self.assertTrue(q.ambiguous_emotional_help(text), text)
        for text in ('帮我排一下汇报大纲', '电脑打不开怎么办', '我压力很大，给我具体建议'):
            self.assertFalse(q.ambiguous_emotional_help(text), text)
        self.assertTrue(q.quiet_turn_needs_rewrite('好累，不知道怎么办', '首先，你应该列一个计划。'))
        self.assertFalse(q.quiet_turn_needs_rewrite('好累，不知道怎么办', '听起来很难受。想先聊聊还是一起想办法？'))

    def test_prompt_respects_closing_and_question_streak(self):
        bot = make_bot()
        bot._history['u:42'] = [{'role': 'assistant', 'content': '什么比赛呀'},
                                {'role': 'user', 'content': '学校的'},
                                {'role': 'assistant', 'content': '你准备好了吗'}]
        prompt = bot._build_messages('u:42', '先去洗澡，等会儿聊')[0]['content']
        self.assertIn('不要追问、索取汇报', prompt)
        self.assertIn('最近连续两次都在提问', prompt)
        self.assertIn('不编造之前聊过的细节', prompt)
        public = bot._build_messages('qzone:auto', '先去洗澡，等会儿聊')[0]['content']
        self.assertNotIn('本轮节奏', public)

    async def test_single_dry_reply_is_not_automatically_regenerated(self):
        bot = make_bot()
        bot.generate = AsyncMock(return_value='嗯')
        await bot._respond('u:42', '我把刚刚提到的事情做完了', object(),
                           'send_private_msg', {'user_id': 42}, {})
        self.assertEqual(bot.generate.await_count, 1)
        self.assertEqual(bot._history['u:42'][-1]['content'], '嗯')

    async def test_closing_does_not_trigger_topic_rewrite(self):
        bot = make_bot()
        bot._dry_streak['u:42'] = 3
        bot.generate = AsyncMock(return_value='好')
        await bot._respond('u:42', '先去洗澡，等会儿聊', object(),
                           'send_private_msg', {'user_id': 42}, {})
        self.assertEqual(bot.generate.await_count, 1)

    async def test_acknowledgement_rewrites_an_unwanted_followup(self):
        bot = make_bot()
        bot.generate = AsyncMock(side_effect=['笑什么呀', '哈哈哈'])
        await bot._respond('u:42', '哈哈哈', object(),
                           'send_private_msg', {'user_id': 42}, {})
        self.assertEqual(bot.generate.await_count, 2)
        self.assertEqual(bot._history['u:42'][-1]['content'], '哈哈哈')

    async def test_ambiguous_help_rewrites_unsolicited_instructions(self):
        bot = make_bot()
        bot.generate = AsyncMock(side_effect=['首先，你应该列一个计划。', '听着挺累的，我在听。'])
        await bot._respond('u:42', '我好累，不知道怎么办', object(),
                           'send_private_msg', {'user_id': 42}, {})
        self.assertEqual(bot.generate.await_count, 2)
        self.assertEqual(bot._history['u:42'][-1]['content'], '听着挺累的，我在听。')
