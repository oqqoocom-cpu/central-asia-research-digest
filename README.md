# 中亚研究每日简报

当前流水线版本：`2.0.0`

维护者：**薛定谔与海森堡**（GitHub：`@oqqoocom-cpu`）  
许可证：Apache License 2.0（代码与项目文档）

面向专业中亚区域国别研究者的每日深度阅读清单。目标不是“抓到多少算多少”，而是在约 1 小时阅读时间内，筛出最新、最深度、最有研究价值的报道、分析、政策材料和高质量论文入口。

## 当前规则

- 公开版标题：`中亚研究每日简报`
- 阅读量目标：优先 8–12 条，最多 15 条；**高质量不足时宁缺毋滥**
- 普通深度报道/分析：近 14 天
- 顶级深度源 / 重要报告：近 30 天
- 白名单学术论文：近 120 天（按期刊定向拉取，不用泛搜充数）
- 必须能识别发布时间，并通过跨日去重
- Telegram 碎片化短消息默认关闭
- 学术论文仅白名单期刊 + DOI/作者/摘要 + 标题或摘要中亚强相关；无合格论文时学术栏可为 0
- Google News 中转链在落盘前解析为出版方原文
- 入选条目会抓取原文摘要，改善内容线索

## 数据源结构（约）

- RSS：约 130 个配置，稳定模式启用约 60+ 
- 网页源：约 44 个，稳定模式启用少量高价值源
- 候选网页源：约 55 个（试探/引文衍生）
- 全球深度发现查询：约 68 组
- PDF/报告源：约 21 个 + World Bank API
- 会议/多边机制：4 个
- 学术查询：OpenAlex 8 个主题任务 + 1 个期刊白名单任务 + Crossref 核心 ISSN 定向任务；不再用 Crossref 重复宽泛主题检索

## 稳定性与可复核性

- 默认启用 TLS 校验、按域名礼貌间隔、`Retry-After`、429/5xx 退避和供应商级冷却。
- Trafilatura 用于 HTML 正文抽取，PyPDF 用于受限页数和大小的 PDF 文本抽取。
- 每次生成保存 `.daily_render_YYYY-MM-DD.md` 和 `selection_audit_YYYY-MM-DD.json`；使用 `--replay` 可离线逐字重放同日成品。
- 同日已经入选且仍通过完整门禁的条目会作为稳定锚点，减少网络瞬时波动造成的随机替换。
- 已知失效 RSS 不再因 S 级覆盖规则被无限重复请求；出版方仍通过网页、站点地图、定向发现或专用出版物适配器覆盖。

注意：健康日志中的 RSS URL、Google News 查询和学术 API 查询属于“发现任务”。“返回非空”只表示技术上抓到链接，不等于当天有新的、带日期且达到公开门槛的独立信息源。请以日志末尾的“筛选漏斗”和“规范化出版方”统计判断真实覆盖。

## 输出文件

| 文件 | 用途 |
|---|---|
| `CentralAsia_Research_YYYY-MM-DD.md` | 公众号公开版（研究者链接清单） |
| `CentralAsia_Internal_Review_YYYY-MM-DD.md` | 内部备查 |
| `source_health_YYYY-MM-DD.log` | 源健康 + 近失诊断 |
| `selection_audit_YYYY-MM-DD.json` | 每条入选材料的来源、门禁、评分和质量证据 |
| `.daily_render_YYYY-MM-DD.md` | 同日离线精确重放快照 |
| `seen_item_history.json` | 跨日去重 |
| `.google_news_resolve_cache.json` | 原文链接解析缓存 |

## 使用方法

1. 双击 `run_digest.bat`，或运行 `python digest_generator.py --date YYYY-MM-DD`
2. 打开 `CentralAsia_Research_YYYY-MM-DD.md` 复制到公众号
3. 需要完整线索时查看内部备查
4. 公开版偏少时，先看健康日志“近失样本”，再判断是否源失效
5. 需要复现同一天的最终成品时运行 `python digest_generator.py --date YYYY-MM-DD --replay`

## 配置

复制 `config.example.json` 为本地配置，或使用环境变量 `OPENALEX_API_KEY`、`CROSSREF_MAILTO`、`DIGEST_OUTPUT_DIR`、`DIGEST_VERIFY_TLS`。不要把真实凭据提交到仓库。完整参数见：

```text
python digest_generator.py --help
```

## 协作身份

代码贡献默认按照 Apache License 2.0 提交。贡献者通过 Fork、Issue 和 Pull Request 参与；合并前必须通过自动测试和来源质量检查。文章原文、报告正文、图片和出版社标识不因本项目开源而获得再许可。

## 规则与架构文档

- `中亚研究每日简报规则.md`：业务硬规则
- `机制架构与升级说明.md`：流水线架构与升级记录
- `学术论文准入白名单.md`
- `引文衍生信息源.md`
- `AGENTS.md`：Codex 项目指令
