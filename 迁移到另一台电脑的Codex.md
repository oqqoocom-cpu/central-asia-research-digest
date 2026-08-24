# 迁移到另一台电脑的 Codex

## 1. 安装并登录

在新电脑安装 ChatGPT desktop app 或 Codex CLI，并使用同一个 ChatGPT/OpenAI 账号登录。Codex 在桌面应用中用于代码、仓库、终端和开发工具；IDE 扩展也可以直接使用当前打开的项目上下文。

## 2. 获取项目

```powershell
git clone https://github.com/oqqoocom-cpu/central-asia-research-digest.git
cd central-asia-research-digest
```

如果新电脑使用 macOS/Linux，把 PowerShell 命令换成对应 shell 命令即可。

## 3. 安装依赖

```text
python -m venv .venv
python -m pip install -r requirements.txt
```

在 Windows PowerShell 中如需激活环境：

```powershell
.\.venv\Scripts\Activate.ps1
```

## 4. 打开 Codex

在 Codex 中打开仓库根目录。仓库里的 `AGENTS.md` 会提供项目规则；如果本地版本没有该文件，可在 Codex 中使用 `/init` 生成项目指令骨架，再以本仓库版本为准。

## 5. 恢复运行状态

程序代码和规则已经在 GitHub。只有需要保持历史去重、历史日报或某天离线重放时，才从旧电脑复制 `CODEX_CONTINUATION.md` 中列出的状态文件。不要把这些运行态文件提交到公共仓库。

## 6. 验证

```text
python -m py_compile digest_generator.py _test_source_diversity.py digest_core/*.py
python _test_source_diversity.py
```

之后再执行指定日期的正式生成。第一次迁移不要直接使用 `--full`，先用默认稳定模式观察来源健康日志。

## 7. 继续开发

先让 Codex 阅读 `AGENTS.md`、`CODEX_CONTINUATION.md`、`README.md`、规则和架构文档，再提出下一项改进。每完成一项改动，都应测试、提交并推送：

```text
git add .
git commit -m "Describe the change"
git push
```
