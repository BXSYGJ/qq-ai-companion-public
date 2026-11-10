import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import qq_bot as q


class PackagingTests(unittest.TestCase):
    def test_example_config_is_safe_and_persona_exists(self):
        bot_dir = Path(__file__).resolve().parents[1]
        cfg = q.load_config(bot_dir / 'config.example.yaml')
        self.assertEqual(cfg['qq']['allow_users'], [])
        self.assertEqual(cfg['llm']['api_key'], '')
        self.assertTrue(cfg['proactive']['enabled'])
        self.assertTrue(cfg['behavior']['followup_enabled'])
        self.assertFalse(cfg['qzone']['auto_post']['enabled'])
        self.assertFalse(cfg['history_import']['on_connect'])
        self.assertTrue(q.load_persona(cfg))

    def test_initiation_defaults_and_explicit_disable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.yaml'
            path.write_text('{}', encoding='utf-8')
            cfg = q.load_config(path)
            self.assertTrue(cfg['proactive']['enabled'])
            self.assertTrue(cfg['behavior']['followup_enabled'])
            path.write_text('proactive:\n  enabled: false\nbehavior:\n  followup_enabled: false\n', encoding='utf-8')
            cfg = q.load_config(path)
            self.assertFalse(cfg['proactive']['enabled'])
            self.assertFalse(cfg['behavior']['followup_enabled'])

    def test_public_qzone_prompt_does_not_impose_a_private_identity(self):
        cfg = q.load_config(Path(__file__).resolve().parents[1] / 'config.example.yaml')
        cfg['memory']['persist'] = False
        bot = q.QQBot(cfg, mock=True)
        bot.persona = 'PRIVATE_PERSONA_SECRET'
        messages = bot._build_messages('qzone:auto', '写一条公开动态')
        self.assertNotIn('PRIVATE_PERSONA_SECRET', str(messages))
