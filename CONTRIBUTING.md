# 贡献指南

感谢参与《中亚研究每日简报》的改进。这个项目首先服务于专业中亚区域国别研究者，贡献应优先提高深度、权威性、可复核性和长期可维护性，而不是单纯增加抓取数量。

## 来源适配器

- 一个出版方或机构使用一个独立适配器，避免把站点特例散落在主流程中。
- 适配器必须记录原始 URL、出版日期、文献形态、访问状态和失败原因。
- 优先使用 RSS、站点地图、官方 API、出版物索引和公开全文入口；不要绕过登录、付费墙、验证码或访问控制。
- 对 403、429、5xx 和 `Retry-After` 做礼貌处理；不得通过并发轰炸提高返回量。
- HTML 正文应尽量经过正文抽取；PDF 应限制大小和页数，并保留无法抽取的诊断信息。

## 内容门禁

- 不以会议新闻、课程广告、奖学金、书评、普通快讯或仅有摘要的页面凑数。
- 每条公开材料必须有可核验时间、强中亚关联、足够深度和可访问原文。
- 学术论文必须遵守期刊白名单、DOI、作者、摘要和中亚锚点门禁。
- 新增测试覆盖来源身份、日期解析、去重、限流、抽取或输出稳定性。

## 提交前检查

```powershell
python -m py_compile digest_generator.py _test_source_diversity.py digest_core\*.py
python _test_source_diversity.py
python digest_generator.py --date YYYY-MM-DD --no-translation
python digest_generator.py --date YYYY-MM-DD --replay
```

不要提交 API key、个人邮箱、缓存、历史日报、健康日志或内部备查文件。
