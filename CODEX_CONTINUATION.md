# Codex Continuation Brief

这份文件用于在另一台电脑的 Codex 中快速恢复项目上下文。它不是运行缓存，也不包含个人凭据。

## 项目

- 仓库：`https://github.com/oqqoocom-cpu/central-asia-research-digest`
- 默认分支：`main`
- 项目版本：`2.0.0`
- 维护者：薛定谔与海森堡（`@oqqoocom-cpu`）
- 许可证：Apache License 2.0

## 当前系统能力

- RSS、网页、Google News 深度发现、机构出版物、PDF/报告、国家评估和学术 API 多通道抓取。
- `digest_core/` 提供运行配置、礼貌 HTTP、原子状态写入、Trafilatura HTML 正文抽取和 PyPDF 文本抽取。
- 公开版通过日期、中亚强相关、研究体裁、来源信誉、正文证据、访问状态和跨日去重门禁。
- 同日已选条目有稳定锚点；`.daily_render_YYYY-MM-DD.md` 支持 `--replay` 离线精确重放。
- `selection_audit_YYYY-MM-DD.json` 记录每条入选材料的来源、评分、文献形态和质量证据。
- 学术通道使用 OpenAlex 主题与白名单检索，Crossref 负责核心 ISSN 定向校验。

## 在新电脑上的第一条指令

把仓库打开后，将下面这段话发给 Codex：

```text
请先阅读 AGENTS.md、CODEX_CONTINUATION.md、README.md、 中亚研究每日简报规则.md 和 机制架构与升级说明.md。
不要先重写架构。先检查当前 Git 状态、运行时依赖和 33 项回归测试，确认项目能在本机复现。
然后告诉我：当前版本有哪些已完成能力、哪些状态文件需要从旧电脑迁移、下一步最值得改进的三个问题。
所有来源适配必须遵守公开访问、礼貌限流、正文证据和深度门禁要求。
```

## 建议从旧电脑单独迁移的运行状态

这些文件被 `.gitignore` 排除，不应上传 GitHub；如果希望保持历史去重和同日状态，可通过加密 U 盘或可信私有云复制到新电脑的项目目录：

- `seen_item_history.json`
- `candidate_source_history.json`
- 历史 `CentralAsia_Research_*.md`
- 历史 `CentralAsia_Internal_Review_*.md`
- 需要继续重放的 `.daily_selection_YYYY-MM-DD.json` 和 `.daily_render_YYYY-MM-DD.md`

翻译缓存和 Google News 解析缓存可以不迁移；缺失时程序会重新建立。不要迁移或公开 API key、Cookie、代理凭据和个人配置。
