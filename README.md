# QQ AI 聊天伙伴

在自己的电脑上运行的 QQ 聊天机器人：NapCatQQ 负责连接 QQ，本项目通过 OneBot v11 接收消息，再调用兼容 Chat Completions 的模型接口生成回复。默认只回复白名单中的联系人。

**适合 Windows 新手。** 你需要一台运行时保持开机的 Windows 电脑、一个用于机器人的 QQ 账号、Python、PowerShell 7、NapCatQQ，以及一个可用的模型 API Key。模型接口一般按用量收费，QQ 与第三方组件的可用性也可能发生变化。

## 第一次使用

### 1. 安装工具

1. 安装 [Python 3.10 或更高版本](https://www.python.org/downloads/windows/)，安装时勾选加入 PATH。
2. 安装 [PowerShell 7](https://learn.microsoft.com/powershell/scripting/install/installing-powershell-on-windows)。下面的命令在 PowerShell 7 中运行。
3. 按 [NapCatQQ Windows 安装说明](https://napneko.github.io/guide/boot/Shell)安装、启动，并登录准备用作机器人的 QQ 账号。本项目不包含 QQ 客户端或 NapCat 安装包。
4. 申请模型 API Key。示例配置使用 [DeepSeek 官方 API](https://platform.deepseek.com/)，也可改用兼容 OpenAI Chat Completions 的服务。

### 2. 下载项目并进行离线演示

在 GitHub 页面选择 **Code → Download ZIP**，解压到一个固定目录。在解压后的项目文件夹中打开 PowerShell 7，运行：

```powershell
Set-Location -LiteralPath bot
pwsh -NoProfile -File ./start.ps1 -Demo
```

首次运行会在 `bot/.venv` 创建虚拟环境并安装依赖，需要网络；之后的演示不需要 QQ 或 API Key。看到“演示成功”表示本地模拟收发链路可用。若端口 `16700` 被占用，请先关闭占用该端口的程序，再运行演示。

### 3. 填写配置

仍在 `bot` 文件夹中运行：

```powershell
pwsh -NoProfile -File ./start.ps1
```

首次运行会生成 `bot/config.yaml` 并退出。用记事本打开该文件，至少修改：

```yaml
qq:
  allow_users: [你的个人QQ号] # 用数字替换，不加方括号以外的文字

llm:
  base_url: https://api.deepseek.com/v1
  api_key: "填入你的API密钥"
  model: deepseek-flash
```

这里的 `allow_users` 是**允许与机器人聊天的联系人 QQ 号**，不是机器人的 QQ 号。若使用其他模型服务，请同时按服务商文档修改 `base_url` 和 `model`；它必须支持兼容 Chat Completions 的接口。示例 Key 只是占位文字，不可直接使用。

保存后运行：

```powershell
pwsh -NoProfile -File ./start.ps1 -Check
```

自检会实际请求一次模型接口，可能产生费用。白名单、API Key 和接口都通过后继续。

### 4. 配置 NapCat 并启动

在 NapCat WebUI 的网络配置中新建并启用 **WebSocket 客户端（反向 WebSocket）**，地址填写：

```text
ws://127.0.0.1:6700/
```

保持 NapCat 运行；然后在 `bot` 文件夹运行：

```powershell
pwsh -NoProfile -File ./start.ps1
```

看到服务启动及 OneBot 连接日志后，用白名单中填写的个人 QQ 号给机器人 QQ 发消息。关闭此窗口或按 `Ctrl+C` 会停止机器人；下次要再次启动 NapCat 和机器人。请勿把监听地址改为 `0.0.0.0`，也不要公开 NapCat WebUI 或令牌。

## 更换角色

私聊支持图片、文本文件、PDF、DOCX，以及 NapCat `fetch_ptt_text` 提供的 QQ 语音转文字。图片识别需要支持视觉的模型；`media.vision_model` 留空时使用当前聊天模型。文件默认限制为 5 MiB，PDF 最多读取前 30 页，扫描件不自动 OCR；群上传通知不属于普通消息段，暂未支持。图片和提取文本会发送给模型服务，可能产生费用；语音使用 QQ 转写接口，结果可能有误。识别失败会提示无法读取。

默认角色为 `persona.example.md`。建议先把它复制为项目根目录的 `persona.local.md`，编辑副本，然后在 `bot/config.yaml` 中修改：

```yaml
llm:
  system_prompt_file: "../persona.local.md"
```

角色文件修改后需要重启。`persona.local.md` 已加入 `.gitignore`；不要把真实姓名、住址或其他人的隐私写入公开仓库。运行时的聊天节奏和消息安全规则也会影响回复。

## 常用命令

从白名单 QQ 向机器人发送：

| 命令 | 作用 |
| --- | --- |
| `/help` | 查看简要帮助 |
| `/status` | 查看当前状态 |
| `/记忆` | 查看模型提取的事件与原话依据，提取可能有误 |
| `/忘记 1` | 删除第 1 条事件；原聊天和摘要仍保留 |
| `/new` | 清除本会话聊天、摘要和事件，保留关系设置 |
| `/关系 暗恋`、`/关系 恋人` | 手动设定本会话关系 |
| `/静默 2h`、`/静默 off` | 暂停或恢复主动联系 |

默认关闭主动搭话、未回复补话、自动发说说和自动导入历史。如需使用，分别查看 `bot/config.yaml` 的 `proactive`、`behavior.followup_enabled`、`qzone`、`history_import`；修改后重启。事件记忆和多话题检查可能额外调用模型并产生费用。公开说说使用独立上下文，但生成内容仍应在发布前自行判断。

## 隐私与排障

- API Key 在 `bot/config.yaml`，聊天记录、摘要及事件在 `bot/state/`，日志在 `bot/qq-bot.log`。这些文件可能含私人信息，不要发给他人或上传；`.gitignore` 只防止通常情况下误提交，不能保护已经上传过的密钥。
- 模型服务会收到角色提示词及生成回复所需的聊天上下文。使用第三方接口前，请确认其数据处理和费用政策。
- 不回复：检查机器人和 NapCat 是否均在运行、WebSocket 是否连接、个人 QQ 是否在 `allow_users` 中。
- `401`、`model_not_found`：检查 Key、余额、模型名称及接口地址；不同服务商的模型名称不一定相同。
- 端口占用：关闭冲突的实例，或同步修改 `config.yaml` 的端口及 NapCat 的连接地址。
- 想先验证 QQ 而不调用模型：配置好白名单后运行 `pwsh -NoProfile -File ./start.ps1 -Mock`，它会用固定回复，不会调用模型接口。

## 开发与测试

```powershell
python -m pip install -r bot/requirements.txt
python -B -m unittest discover -s bot/tests -v
```

`bot/conversation_lab.py` 可用合成对话调用已配置的真实模型评测语气；会产生模型费用，不会向真实 QQ 发送消息。测试生成的记录默认被 `.gitignore` 忽略。
