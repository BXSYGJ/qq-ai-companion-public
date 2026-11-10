import copy
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import qq_bot as q


def make_bot():
    cfg = copy.deepcopy(q.DEFAULTS)
    cfg['memory'] = {'persist': False}
    cfg['qq']['allow_users'] = [42]
    cfg['behavior'].update(typing_indicator=False, followup_enabled=False, topic_push=False,
                           coverage_check=False, reply_delay_ms=0, segment_delay_ms=[0, 0],
                           first_segment_delay_ms=[0, 0])
    bot = q.QQBot(cfg, mock=True)
    bot.action = AsyncMock(return_value={'status': 'ok', 'retcode': 0})
    bot._wait_until_peer_ready = AsyncMock(return_value=True)
    return bot


class MediaTests(unittest.IsolatedAsyncioTestCase):
    async def test_voice_uses_qq_message_id(self):
        bot = make_bot()
        bot.action = AsyncMock(return_value={'status': 'ok', 'data': {'text': '你好呀'}})
        result = await bot._describe_media(object(), {'type': 'record', 'data': {}, 'message_id': 123})
        self.assertIn('你好呀', result)
        self.assertEqual(bot.action.await_args.args[1:], ('fetch_ptt_text', {'message_id': 123}))

    async def test_voice_failure_is_not_invented(self):
        bot = make_bot()
        bot.action = AsyncMock(return_value={'status': 'failed', 'data': {}})
        self.assertIn('失败', await bot._describe_media(object(), {'type': 'record', 'data': {}, 'message_id': 2}))

    async def test_file_and_oversize(self):
        bot = make_bot()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'a.txt'
            path.write_text('测试文件内容', encoding='utf-8')
            bot.action = AsyncMock(return_value={'data': {'file': str(path), 'file_name': 'a.txt'}})
            result = await bot._describe_media(object(), {'type': 'file', 'data': {'file_id': 'id'}})
            self.assertIn('测试文件内容', result)
            path.write_bytes(b'x' * 2048)
            bot.cfg['media']['max_bytes'] = 1024
            self.assertIn('过大', await bot._describe_media(object(), {'type': 'file', 'data': {'file_id': 'id'}}))

    async def test_rejects_untrusted_url_before_action(self):
        bot = make_bot()
        result = await bot._describe_media(object(), {'type': 'image', 'data': {'file': 'http://127.0.0.1/private'}})
        self.assertIn('标识', result)
        bot.action.assert_not_awaited()

    async def test_image_sends_actual_bytes_to_vision_model(self):
        bot = make_bot()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'x.png'
            path.write_bytes(b'\x89PNG\r\n\x1a\nexample')
            bot.action = AsyncMock(return_value={'data': {'file': str(path)}})
            bot._post_chat = AsyncMock(return_value={'choices': [{'message': {'content': '一只猫'}}]})
            result = await bot._describe_media(object(), {'type': 'image', 'data': {'file': 'id'}}, '图里是什么')
            self.assertIn('一只猫', result)
            body = bot._post_chat.await_args.args[0]
            self.assertIn('data:image/png;base64,', body['messages'][0]['content'][1]['image_url']['url'])
            self.assertIn('图里是什么', body['messages'][0]['content'][0]['text'])

    async def test_media_is_resolved_before_generation_and_preserves_order(self):
        bot = make_bot()
        event = {'message_id': 22, 'message': [{'type': 'text', 'data': {'text': '看看'}},
                                                 {'type': 'record', 'data': {'file': 'voice'}},
                                                 {'type': 'text', 'data': {'text': '然后说说'}}]}
        text = bot._queue_media_segments('u:42', event, '')
        bot._describe_media = AsyncMock(return_value='[语音转写：测试]')
        bot.generate = AsyncMock(return_value='收到')
        await bot._respond('u:42', text, object(), 'send_private_msg', {'user_id': 42}, {})
        self.assertEqual(bot.generate.await_args.args[1], '看看[语音转写：测试]然后说说')
        self.assertFalse(bot._media_items)

    def test_pdf_extracts_text(self):
        import fitz
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((50, 50), 'hello pdf')
        content = doc.tobytes()
        doc.close()
        self.assertIn('hello pdf', q.extract_document_text(content, '.pdf', 1000))

    def test_docx_extracts_text(self):
        from docx import Document
        doc = Document()
        doc.add_paragraph('document text')
        doc.add_table(rows=1, cols=1).cell(0, 0).text = 'table text'
        buf = io.BytesIO()
        doc.save(buf)
        self.assertIn('document text', q.extract_document_text(buf.getvalue(), '.docx', 1000))
        self.assertIn('table text', q.extract_document_text(buf.getvalue(), '.docx', 1000))
