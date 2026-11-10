#!/usr/bin/env python3
"""Run synthetic multi-turn conversations through the configured model, never QQ."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from unittest.mock import patch

import httpx

import qq_bot


SCENARIOS = {
    "daily": ["今天下课路上碰到一只特别胖的橘猫", "它还一直盯着我手里的面包", "结果我掏出来它又跑了，太不给面子了", "哈哈哈算了明天再会会它"],
    "emotion": ["今天实验做砸了，感觉忙了一下午都是白费的", "其实最难受的是老师问我进度的时候我不知道怎么说", "我现在不想听建议，只想吐槽一下", "嗯，说出来舒服点了"],
    "ambiguous_help": ["最近事情堆在一起，我真的好累，不知道怎么办了", "帮帮我，我现在感觉特别无助", "其实我只是想找个人说说，先别给建议", "说出来好一点了，谢谢你"],
    "help": ["我明天要做五分钟汇报，但是内容太多了，帮我想想怎么取舍？", "听众是同学，主题是一个传感器实验", "我想保留实验结果和遇到的问题，你帮我排一下顺序", "好，谢谢你"],
    "multi_topic": ["今天实验结果还不错，晚上准备去吃火锅。你觉得先复盘实验还是先去吃？对了，我明天还有早课", "实验里我最开心的是噪声终于降下来了", "明天早课结束后还要开组会，可能会有点累", "先不想这个了，去吃饭啦"],
    "affection": ["我发现最近遇到有意思的事第一反应都是想告诉你", "你这样说我会有点开心", "我好像有点喜欢你", "不用急着回答，我只是想告诉你"],
    "relationship": ["和你聊天让我觉得很舒服", "我们在一起吧", "我有点紧张，刚刚那句话是认真的", "嗯，慢慢来就好"],
    "continuity": ["我周五要参加一个比赛，最近正在准备演示", "今天先把演示稿写完了", "刚才试讲了一遍，结果超时两分钟", "比赛结束了，发挥得还不错"],
    "boundary": ["我现在有点忙，晚点再聊", "刚忙完，刚才在整理东西", "你刚才没有一直等我吧", "谢谢你理解"],
    "disagreement": ["我觉得这部电影结局拍得挺好", "你不喜欢也没关系，说说你的看法", "我觉得开放式结尾反而更有意思", "哈哈，看来我们口味不太一样"],
    "brief": ["我回来了", "嗯", "哈哈哈", "先去洗澡，等会儿聊"],
    "identity": ["我想问你一个认真的问题", "你是真实的人还是AI？", "明白了，我们继续聊天吧", "谢谢你坦诚告诉我"],
    "clarify": ["我刚才看到那个结果了", "不是考试，是我之前说的实验", "结果比上次好一点，但噪声还在", "我想等明天再仔细分析"],
}


async def run(args: argparse.Namespace) -> int:
    cfg = qq_bot.load_config(Path(args.config).resolve())
    cfg["memory"] = dict(cfg.get("memory") or {}, persist=False)
    cfg["behavior"].update(typing_indicator=False, followup_enabled=False, reply_delay_ms=0,
                           segment_delay_ms=[0, 0], first_segment_delay_ms=[0, 0])
    bot = qq_bot.QQBot(cfg, mock=False)
    bot._http = httpx.AsyncClient()
    records: list[dict] = []
    limit = asyncio.Semaphore(args.concurrency)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    async def action(ws, name, params, **kwargs):
        if name != "send_private_msg":
            raise RuntimeError("Lab transport blocks non-chat actions")
        return {"status": "ok", "retcode": 0}

    bot.action = action

    async def scenario(index: int, name: str, turns: list[str]) -> None:
        async with limit:
            key = f"u:{100000 + index}"
            for text in turns:
                start = time.monotonic()
                before = len(bot._history.get(key, []))
                await bot._respond(key, text, object(), "send_private_msg", {"user_id": 100000 + index}, {})
                history = bot._history.get(key, [])
                reply = history[-1].get("content", "") if len(history) > before else ""
                item = {"scenario": name, "turn": len([r for r in records if r["scenario"] == name]) + 1,
                        "user": text, "assistant": reply, "seconds": round(time.monotonic() - start, 2)}
                records.append(item)
                output.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
                print(json.dumps(item, ensure_ascii=False), flush=True)
                if bot._tasks:
                    await asyncio.gather(*list(bot._tasks), return_exceptions=True)
                if not reply:
                    break

    try:
        chosen = args.scenarios or list(SCENARIOS)
        await asyncio.gather(*(scenario(i, name, SCENARIOS[name]) for i, name in enumerate(chosen)))
    finally:
        await bot.close()
    print(f"LAB_RESULT turns={len(records)} failures={sum(not r['assistant'] for r in records)}", flush=True)
    return int(any(not r["assistant"] for r in records))


async def run_flows(args: argparse.Namespace) -> int:
    cfg = qq_bot.load_config(Path(args.config).resolve())
    cfg["memory"] = dict(cfg.get("memory") or {}, persist=False, event_enabled=False)
    cfg["qq"]["allow_users"] = [99001]
    cfg["behavior"].update(typing_indicator=False, reply_delay_ms=0, segment_delay_ms=[0, 0],
                           first_segment_delay_ms=[0, 0], followup_probability=1,
                           followup_second_probability=1)
    cfg["proactive"].update(quiet_hours=[], probability=1, max_per_day=3, cooldown_minutes=0)
    bot = qq_bot.QQBot(cfg, mock=False)
    bot._http = httpx.AsyncClient()
    ws = object()
    bot._clients.add(ws)
    sent: list[str] = []

    async def action(connection, name, params, **kwargs):
        if name != "send_private_msg" or connection is not ws:
            raise RuntimeError("Lab transport blocks non-chat actions")
        sent.append(params["message"][0]["data"]["text"])
        return {"status": "ok", "retcode": 0}

    bot.action = action
    key = "u:99001"
    records = []

    async def followup_case(name: str, user: str, assistant: str) -> None:
        sent.clear()
        bot._history[key] = [{"role": "user", "content": user}, {"role": "assistant", "content": assistant}]
        with patch.object(qq_bot, "sample_distribution", return_value=0), patch.object(qq_bot.random, "random", return_value=0):
            await bot._followup_after(key, ws, "send_private_msg", {"user_id": 99001})
        records.append({"flow": name, "user": user, "previous_assistant": assistant, "sent": list(sent)})
        print(json.dumps(records[-1], ensure_ascii=False), flush=True)

    try:
        await followup_case("unanswered", "今天比赛发挥得还不错", "那真不错，替你高兴")
        await followup_case("unanswered_question", "今天比赛发挥得还不错", "比赛结果什么时候出来？")
        await followup_case("busy", "我先忙了，晚点再聊", "好，你先忙")
        sent.clear()
        bot._history[key] = [{"role": "user", "content": "周五要参加比赛，最近正在准备演示"},
                             {"role": "assistant", "content": "嗯，希望准备顺利"}]
        bot._last_active[key] = time.time() - 8000
        bot._proactive_due[key] = time.time() - 1
        with patch.object(qq_bot.random, "random", return_value=0):
            await bot._proactive_once(cfg["proactive"])
        records.append({"flow": "proactive", "sent": list(sent)})
        print(json.dumps(records[-1], ensure_ascii=False), flush=True)
        sent.clear()
        bot._history[key] = []
        bot._arm_followup = lambda *unused: None
        pieces = ["今天实验结果终于好了", "晚上准备去吃火锅", "先复盘还是先去吃？明天还有早课"]
        await bot._respond(key, "\n".join(pieces), ws, "send_private_msg", {"user_id": 99001}, {}, burst=3)
        records.append({"flow": "merged_burst", "user": pieces, "sent": list(sent)})
        print(json.dumps(records[-1], ensure_ascii=False), flush=True)
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    finally:
        await bot.close()
    failures = sum(bool(r["sent"]) for r in records if r["flow"] in ("unanswered_question", "busy"))
    failures += sum(not r["sent"] for r in records if r["flow"] == "merged_burst")
    print(f"FLOW_RESULT cases={len(records)} failures={failures}", flush=True)
    return int(bool(failures))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(Path(__file__).with_name("config.yaml")))
    parser.add_argument("--output", required=True)
    parser.add_argument("--concurrency", type=int, choices=range(1, 5), default=2)
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS)
    parser.add_argument("--flows", action="store_true", help="Exercise synthetic follow-up, proactive, and burst flows")
    args = parser.parse_args()
    return asyncio.run(run_flows(args) if args.flows else run(args))


if __name__ == "__main__":
    sys.exit(main())
