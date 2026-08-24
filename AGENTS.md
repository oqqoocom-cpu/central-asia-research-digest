# Codex Project Instructions

本仓库用于维护《中亚研究每日简报》。开始任何改动前，先阅读：

- `中亚研究每日简报规则.md`
- `机制架构与升级说明.md`
- `README.md`

## 项目目标

- 面向资深中亚区域国别研究者，不做泛新闻聚合。
- 公开版优先 8–12 条、最多 15 条；质量不足时宁缺毋滥。
- 只收录与中亚有实质关系、时间可核验、原文可访问且具备深度证据的研究材料。
- 普通深度分析原则上近 14 日；顶级媒体、权威机构报告可放宽至近 30 日；白名单论文近 120 日；长期研究须通过耐久性门禁。
- 不用课程、奖学金、会议广告、普通快讯、书评、营销稿、无原文摘要或低质量论文凑数。

## 工程规则

- 优先使用现有适配器、`digest_core/` 基础设施和本地测试，不重复发明同类机制。
- 不绕过登录、付费墙、验证码、robots 限制或访问控制；遵守域名礼貌间隔与 `Retry-After`。
- 不把 API key、邮箱凭据、Cookie、缓存、历史日报、内部日志或文章原文提交到 Git。
- 修改来源适配器时必须记录公开入口、日期、访问状态和失败类型，并补充回归测试。
- 任何筛选规则变化都要检查跨日去重、同日锚点、选择审计和 `--replay`。

## 验证命令

```text
python -m py_compile digest_generator.py _test_source_diversity.py digest_core/*.py
python _test_source_diversity.py
python digest_generator.py --date YYYY-MM-DD --replay
```

如果是在 Windows PowerShell 中运行，可将 `digest_core/*.py` 换成 `digest_core\*.py`。

## 当前维护者

- 公开署名：薛定谔与海森堡
- GitHub：`oqqoocom-cpu`
- 许可证：Apache License 2.0
