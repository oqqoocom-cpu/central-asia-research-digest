# 中亚研究每日简报

当前流水线版本：`2.0.0`

维护者：**薛定谔与海森堡**（GitHub：`@oqqoocom-cpu`）  
许可证：Apache License 2.0（代码与项目文档）

项目仓库：[Central Asia Research Digest](https://github.com/oqqoocom-cpu/central-asia-research-digest)

面向专业中亚区域国别研究者的每日深度阅读清单。目标不是“抓到多少算多少”，而是在约 1 小时阅读时间内，筛出最新、最深度、最有研究价值的报道、分析、政策材料和高质量论文入口。

**项目边界**：本仓库只维护“每日研究简报”和“中亚涉华争议监测”两个持续运行的材料入口，不承载具体论文、咨政报告或田野调查项目。具体研究题目应在 Codex 中另开独立项目；如需引用本仓库材料，由研究者按题目手动选择，不自动导入或共享长上下文。

## 当前规则

- 公开版标题：`中亚研究每日简报`
- 阅读量目标：优先 8–12 条，最多 15 条；**高质量不足时宁缺毋滥**
- 普通深度报道/分析：近 14 天
- 顶级深度源 / 重要报告：近 30 天
- 长期有效研究：仅限通过权威性、正文深度、可访问性和耐久性门禁的正式报告/研究材料，最长回溯约 10 年，并单列“长期有效研究（首次收录）”栏目；普通新闻和一般分析不适用
- 白名单学术论文：近 120 天（按期刊定向拉取，不用泛搜充数）
- 必须能识别发布时间，并通过跨日去重
- Telegram 碎片化短消息默认关闭
- 学术论文仅白名单期刊 + DOI/作者/摘要 + 标题或摘要中亚强相关；无合格论文时学术栏可为 0
- Google News 中转链在落盘前解析为出版方原文
- 入选条目会抓取原文摘要，改善内容线索

## 2026-09-04 入口核验与涉华源头补强

- 报告入口修正：`IWEP Kazakhstan Publications` 改用研究院首页并允许 `/en/`、`/ru/` 出版物路径；`ISRS Analytical Materials` 增加乌兹别克语 `/oz` 入口；`FAO Europe Central Asia Publications` 增加欧洲与中亚区域资源页；`UNICEF Europe Central Asia Reports` 改用 `research-and-reports` 页面；`Central Asia Forum Publications` 增加 `/articles/`、`/issues/` 入口。
- 涉华旁路新增五国语言发现查询：哈萨克语、乌兹别克语、吉尔吉斯语、塔吉克语、土库曼语各 5 组查询；查询结果仍需通过来源、日期、中亚关联、争议信号、深度和跨日去重门槛。
- 涉华旁路新增 22 个本土源头入口：9 个 RSS、13 个网页入口；覆盖五国本土媒体、官方站点和相对稳定的本国语言页面，不包含 Telegram、Facebook 等社交平台碎片。
- 塔吉克语 Asia-Plus 已切换到 `asiaplus.news` RSS；旧的 Khovar 旁路入口因站点返回 `410` 不再纳入涉华源头扫描。
- 涉华旁路新增国际来源复用层：把主流程已抓取的国际权威媒体、国际智库/调查机构、国际组织/多边机构、周边国家权威来源和白名单学术材料复制到独立监测池，不重复联网请求。
- 国际层覆盖 Reuters、AP、BBC、FT、Economist、NYT、ICG、Carnegie、Brookings、CSIS、Chatham House、RUSI、SIPRI、IISS、World Bank、IMF、ADB、EBRD、OECD、联合国系统及俄土伊阿、南亚和南高加索研究来源；最终仍按涉华争议、深度、时效和去重门槛筛选。
- 本土源只进入 `Sino_CentralAsia_Watch_YYYY-MM-DD.md` 旁路，不会降低主简报深度门槛或把一般舆情倒灌进公众号版。

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
- 健康日志末尾记录来源绩效漏斗，用于区分真实独立贡献、重复和低产来源；该统计不改变当轮筛选结果。
- 入口维护采用低频队列：稳定模式跳过源按周小批量复测；已知死链和 `discovery-only` 源不盲目重试旧地址，必须先登记公开官方替代入口。
- 维护成功须绑定同一入口、在不同日期连续通过至少两次才建议适配器复核，不自动启用；已配置替代入口不等于稳定或有独立贡献。维护状态或日志写入异常不会中断主采集，离线回放和重筛不执行维护。
- 机构页面的 meta 摘要和正文摘录分别接受标题一致性校验；明显来自相关推荐卡片或其他文献的摘要不会进入公开版。
- 经出版方页面核验的跨语言版本使用窄范围文献家族表去重；不对普通多语种标题做高风险模糊合并。
- 规则调整后可用 `--rescreen` 对当日候选缓存离线重筛；该模式不重新联网，不覆盖当日候选缓存、历史去重和正常运行审计。
- 同日已经入选且仍通过完整门禁的条目会作为稳定锚点，减少网络瞬时波动造成的随机替换。
- 已知失效 RSS 不再因 S 级覆盖规则被无限重复请求；出版方仍通过网页、站点地图、定向发现或专用出版物适配器覆盖。

注意：健康日志中的 RSS URL、Google News 查询和学术 API 查询属于“发现任务”。“返回非空”只表示技术上抓到链接，不等于当天有新的、带日期且达到公开门槛的独立信息源。请以日志末尾的“筛选漏斗”和“规范化出版方”统计判断真实覆盖。

## 输出文件

| 文件 | 用途 |
|---|---|
| `CentralAsia_Research_YYYY-MM-DD.md` | 公众号公开版（研究者链接清单） |
| `CentralAsia_Internal_Review_YYYY-MM-DD.md` | 内部备查 |
| `Sino_CentralAsia_Watch_YYYY-MM-DD.md` | 严格涉华争议监测档案 |
| `Sino_CentralAsia_LeadPool_YYYY-MM-DD.md` | 宽口径待核验线索池 |
| `source_health_YYYY-MM-DD.log` | 源健康 + 近失诊断 |
| `source_maintenance_YYYY-MM-DD.log` | 暂停源入口诊断、低频重测与待修复队列 |
| `source_maintenance_state.json` | 入口维护队列的跨日状态（本地运行文件，不提交 Git） |
| `selection_audit_YYYY-MM-DD.json` | 每条入选材料的来源、门禁、评分和质量证据 |
| `sino_central_asia_watch_YYYY-MM-DD.json` | 严格涉华监测结构化记录 |
| `sino_central_asia_lead_pool_YYYY-MM-DD.json` | 宽口径线索结构化记录 |
| `sino_central_asia_lead_history.json` | 宽口径线索独立跨日去重状态 |
| `rescreen_audit_YYYY-MM-DD.json` | 规则调整后离线重筛的结果记录 |
| `.daily_render_YYYY-MM-DD.md` | 同日离线精确重放快照 |
| `seen_item_history.json` | 跨日去重 |
| `.google_news_resolve_cache.json` | 原文链接解析缓存 |
| `archive/YYYY/MM/` | 超过 7 天的成品、审计、快照和日志 |

## 使用方法

1. 日常只需双击 `run_digest.bat`；生成完成后，根目录当天的 `CentralAsia_Research_YYYY-MM-DD.md` 就是可直接复制到 Mdnice 的公众号底稿
2. 需要完整线索时，直接打开根目录当天的 `CentralAsia_Internal_Review_YYYY-MM-DD.md`、`Sino_CentralAsia_Watch_YYYY-MM-DD.md` 或 `Sino_CentralAsia_LeadPool_YYYY-MM-DD.md`
3. 程序每次生成后自动把超过 7 天的成品、技术日志和审计文件移动到 `archive/YYYY/MM/`；根目录只保留最近 7 天，便于日常选择
4. 归档不影响跨日去重、`--replay` 或 `--rescreen`；历史输入会从根目录和 `archive/` 一并查找
5. 主简报末尾统一显示内部备查条数、严格涉华旁路统计和宽口径线索池统计；三类材料仍保持独立文件，不混入公开正文
6. 公开版偏少时，先看健康日志“近失样本”，再判断是否源失效
7. 需要复现同一天的最终成品时运行 `python digest_generator.py --date YYYY-MM-DD --replay`
8. 仅按最新规则重筛同日缓存时运行 `python digest_generator.py --date YYYY-MM-DD --rescreen`

## 文件归档与日常使用

这个项目不再生成固定入口文件，也不要求通过入口跳转。每天只需在根目录选择当天的 Markdown 文件：通常先把 `CentralAsia_Research_YYYY-MM-DD.md` 放入 Mdnice 排版；内部备查、严格涉华监测和宽口径线索池按需打开。

归档目录按 `archive/YYYY/MM/` 组织。根目录默认保留今天及前 6 天的成品，超过 7 天的公开版、内部备查、涉华监测、线索池、同日快照、选择审计、重筛审计和源健康日志会自动归档。永久状态文件（如 `seen_item_history.json`、翻译缓存和涉华历史）继续留在根目录，不参与移动。

## 涉华议题监测

程序每天自动执行一条旁路任务：从公开 RSS、网页、机构报告、国际组织材料、白名单学术来源和多语种 Google News 发现入口筛选中亚涉华争议性议题。来源同时覆盖中亚五国本土材料、国际权威媒体、国际智库/调查机构、多边组织及周边国家权威来源；国际层优先复用主流程已抓取条目，避免重复请求。它要求同时出现中国因素、中亚实质关联、争议或利益冲突信号、可识别来源和最低深度证据，并自动排除 Facebook、Telegram 等社交平台碎片、普通快讯、活动稿和聚合摘要。

监测结果写入：

- `Sino_CentralAsia_Watch_YYYY-MM-DD.md`：便于阅读的中文档案；
- `sino_central_asia_watch_YYYY-MM-DD.json`：结构化记录；
- `sino_central_asia_watch_history.json`：跨日去重状态。

宽口径线索池另行写入：

- `Sino_CentralAsia_LeadPool_YYYY-MM-DD.md`：扩大观察面用的待核验清单；
- `sino_central_asia_lead_pool_YYYY-MM-DD.json`：结构化线索和严格淘汰原因；
- `sino_central_asia_lead_history.json`：与严格档案分离的 90 日跨日去重状态。

线索池仍要求中国因素、中亚关联和负面/争议信号同时出现，但暂时允许来源信誉不足、原文深度不足或普通新闻体裁。它只用于后续寻找第二独立来源或一手文件，不能替代严格档案，也不能直接作为事实依据。

该旁路只提供“初步排查后的研究线索”，不把单一来源的指控当作事实，也不会改变主简报的深度报告定位。

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
- 跨项目使用边界见 `机制架构与升级说明.md` 的“研究项目与持续监测解耦”章节；私人研究设想和讨论稿保留本地，不作为本仓库实施计划。
- `学术论文准入白名单.md`
- `引文衍生信息源.md`
- `AGENTS.md`：Codex 项目指令
