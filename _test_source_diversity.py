import digest_generator as dg
from digest_core.http_client import retry_after_seconds
from digest_core.extraction import extract_main_text, extract_pdf_text
from digest_core.runtime import RuntimeSettings
from digest_core.state import atomic_write_json

import datetime as dt
import json
import tempfile
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch


RECENT_TEST_DATE = str(dg.TODAY - dt.timedelta(days=2))


def make_item(source, index, venue='', published=None, **extra):
    published = published or RECENT_TEST_DATE
    item = {
        'source': source,
        'title': f'Central Asia research item {source} {index}',
        'link': f'https://example.com/{source.lower().replace(" ", "-")}/{index}',
        'summary': 'Central Asia policy analysis with enough context for deterministic selection tests.',
        'published': published,
        'priority_score': 100 - index,
        'depth_score': 20,
        'kw_score': 10,
    }
    if venue:
        item['academic_quality'] = True
        item['academic_venue'] = venue
    item.update(extra)
    return item


def test_canonical_source_keys():
    direct = make_item('The Times of Central Asia', 1)
    discovery = make_item('Deep Discovery: Google News｜The Times Of Central Asia', 2)
    cpc_rss = make_item('Caspian Policy Center RSS', 3)
    cpc_web = make_item('Caspian Policy Center', 4)
    assert dg.public_source_key(direct) == dg.public_source_key(discovery)
    assert dg.public_source_key(cpc_rss) == dg.public_source_key(cpc_web)


def test_source_performance_snapshot_deduplicates_same_source_document():
    raw = [
        make_item('Source A', 1),
        make_item('Source A', 1),
        make_item('Source B', 2),
    ]
    relevant = raw[:2]
    candidates = raw[:2]
    eligible = [raw[0]]
    published = [raw[0]]
    rows = dg.source_performance_snapshot(raw, relevant, candidates, eligible, published, limit=10)
    by_source = {row['source']: row for row in rows}
    source_a = by_source[dg.public_source_key(raw[0])]
    assert source_a['raw'] == 1
    assert source_a['relevant'] == 1
    assert source_a['candidate'] == 1
    assert source_a['eligible'] == 1
    assert source_a['published'] == 1


def test_source_performance_rows_show_funnel_and_rate():
    rows = [{
        'source': 'media:Source A',
        'raw': 4,
        'relevant': 2,
        'candidate': 2,
        'eligible': 1,
        'published': 1,
    }]
    lines = dg.format_source_performance_rows(rows)
    assert len(lines) == 1
    assert 'Source A' in lines[0]
    assert '原始 4 / 相关 2 / 候选 2 / 门禁 1 / 最终 1' in lines[0]
    assert '原始到最终 25.0%' in lines[0]


def test_compact_public_statistics_use_actual_published_count():
    line = dg.format_public_statistics_line(
        active_feed_sources=62,
        active_feed_count=76,
        active_web_sources=14,
        active_web_count=22,
        cross_day_skipped=151,
        public_candidate_count=12,
        published_count=8,
        compact=True,
    )
    assert '公开门禁候选 12 条' in line
    assert '最终公开推荐 8 条' in line
    assert '0 条表示' not in line


def test_zero_public_statistics_explain_zero_only_when_zero():
    line = dg.format_public_statistics_line(
        public_candidate_count=0,
        published_count=0,
        compact=True,
    )
    assert '最终公开推荐 0 条' in line
    assert '0 条表示本轮没有新的合格公开材料' in line


def test_auxiliary_statistics_include_internal_and_sino_watch_counts():
    lines = dg.format_auxiliary_statistics_lines(
        2,
        {
            'fetched': 3108,
            'signal_pass': 4,
            'prefiltered': 4,
            'quality_pass': 1,
            'history_skipped': 2,
            'same_run_skipped': 1,
            'selected': 1,
        },
    )
    assert '内部备查保留 2 条' in lines[0]
    assert '抓取 3108 条' in lines[1]
    assert '信号通过 4 条' in lines[1]
    assert '质量门槛通过 1 条' in lines[1]
    assert '本轮重复排除 1 条' in lines[1]
    assert '今日新增 1 条' in lines[1]


def test_auxiliary_statistics_keep_zero_sino_watch_visible():
    lines = dg.format_auxiliary_statistics_lines(
        0,
        {
            'fetched': 0,
            'signal_pass': 0,
            'prefiltered': 0,
            'quality_pass': 0,
            'history_skipped': 0,
            'selected': 0,
        },
    )
    assert '今日新增 0 条' in lines[1]
    assert '抓取 0 条' in lines[1]


def test_rescreen_statistics_do_not_report_fake_source_counts():
    line = dg.format_public_statistics_line(
        public_candidate_count=7,
        published_count=7,
        rescreen=True,
        rescreen_source_count=9,
    )
    assert '未重新联网' in line
    assert '入选缓存 9 条' in line
    assert '重新通过公开门禁 7 条' in line
    assert '最终公开推荐 7 条' in line
    assert 'RSS 有效源' not in line


def test_verified_cross_language_report_family_deduplicates_oxus_editions():
    english = make_item(
        'Oxus Society Publications',
        1,
        link='https://oxussociety.org/publications/zhanatas-a-test-case-for-international-financing-in-kazakhstans-green-development',
    )
    russian = make_item(
        'Oxus Society Publications',
        2,
        link='https://oxussociety.org/publications/zhanatas-ru',
    )
    assert dg.document_family_key(english['link']) == dg.document_family_key(russian['link'])
    assert dg.document_family_key(english['link']).startswith('family:')
    assert dg.item_history_keys(english) & dg.item_history_keys(russian)
    kept, skipped, _ = dg.drop_history_duplicate_items([english, russian], set())
    assert kept == [english]
    assert skipped == 1


def test_public_pool_diversity_and_academic_cap():
    academics = [
        make_item('Academic: OpenAlex', index, venue=f'Journal {index % 3}')
        for index in range(6)
    ]
    media = [make_item(f'Media {index}', 20 + index) for index in range(10)]
    selected = dg.diversify_ranked_items(
        academics + media,
        12,
        min_unique_sources=6,
        max_per_source=dg.MAX_PUBLIC_ITEMS_PER_SOURCE,
        max_academic_items=dg.MAX_PUBLIC_ACADEMIC_ITEMS,
    )
    academic_selected = [item for item in selected if dg.is_academic_item(item)]
    assert len(selected) == 12
    assert len(academic_selected) <= dg.MAX_PUBLIC_ACADEMIC_ITEMS
    assert dg.distinct_source_count(selected) >= 6


def test_pregate_diversity():
    dominant = [make_item('Dominant Source', index) for index in range(20)]
    others = []
    for source_index in range(30):
        for copy_index in range(2):
            others.append(make_item(f'Other Source {source_index}', 100 + source_index * 2 + copy_index))
    selected = dg.diversify_ranked_items(
        dominant + others,
        40,
        min_unique_sources=40,
        max_per_source=dg.PREGATE_MAX_ITEMS_PER_SOURCE,
    )
    dominant_count = sum(1 for item in selected if item['source'] == 'Dominant Source')
    assert len(selected) == 40
    assert dominant_count <= dg.PREGATE_MAX_ITEMS_PER_SOURCE
    assert dg.distinct_source_count(selected) >= 30


def test_academic_items_do_not_take_headline_slots():
    academic = make_item('Academic: OpenAlex', 1, venue='Central Asian Survey')
    assert dg.is_headline_candidate(academic) is False
    assert dg.is_substantive_deep_item(academic) is False


def test_institutional_events_do_not_become_deep_reads():
    summer_school = make_item('Deep Discovery: Google News｜University of Central Asia', 1)
    summer_school['title'] = 'University of Central Asia and partners conclude the first IPROMO Central Asia summer school'
    alumni = make_item('Deep Discovery: Google News｜University of Central Asia', 2)
    alumni['title'] = 'UCA Alumni Reunion in North America'
    alumni['link'] = 'https://ucentralasia.org/resources-and-media/events/2026/august/uca-alumni-reunion'
    assert dg.is_event_or_conference_announcement(summer_school) is True
    assert dg.is_event_or_conference_announcement(alumni) is True


def test_prestige_institution_publication_passes_with_original_evidence():
    item = make_item(
        'CSIS Search Central Asia',
        1,
        published='2026-06-12',
        source_type='institution_publication',
        source_tier=1,
        access_status='open',
        content_type='Analysis',
        word_count=1800,
        research_score=3,
        depth_term_score=2,
        policy_data_score=1,
        core_score=2,
    )
    item['title'] = "The Infrastructure Trap: What Beijing Has Learned from Moscow's Playbook in Central Asia"
    item['summary'] = (
        'This analysis examines Central Asian infrastructure, state capacity, '
        'strategic dependence, and the policy implications of Russian and Chinese approaches.'
    )
    assert dg.is_recent_item(item) is True
    assert dg.is_strict_deep_public_item(item) is True
    assert dg.is_research_grade_public_item(item) is True

    mismatched = dict(item)
    mismatched['summary'] = ''
    mismatched['summary_mismatch'] = True
    assert dg.is_research_grade_public_item(mismatched) is False


def test_institution_project_and_delegation_pages_are_rejected():
    project = make_item(
        'Davis Center Harvard Central Asia',
        1,
        source_type='institution_publication',
        source_tier=1,
        access_status='open',
        content_type='Research initiative',
        word_count=1800,
        core_score=2,
        research_score=2,
        depth_term_score=1,
    )
    project['title'] = 'The Imperiia Project'
    project['link'] = 'https://daviscenter.fas.harvard.edu/research-initiatives/imperiia-project'
    delegation = dict(project)
    delegation['source'] = 'FPRI Search Central Asia'
    delegation['title'] = 'FPRI Hosts a Delegation from Eastern Europe and Central Asia'
    delegation['link'] = 'https://www.fpri.org/news/2018/06/fpri-hosts-a-delegation-from-eastern-europe-and-central-asia/'
    assert dg.is_event_or_conference_announcement(project) is True
    assert dg.is_event_or_conference_announcement(delegation) is True
    assert dg.is_strict_deep_public_item(project) is False
    assert dg.is_strict_deep_public_item(delegation) is False


def test_broad_eurasia_corridor_is_not_a_central_asia_anchor():
    item = make_item('CSIS Search Central Asia', 1)
    item['title'] = 'Resilience Through Linkage: Russia, Iran, and Aspirations for North-South Trade'
    item['summary'] = 'The report evaluates a route linking Russia and Iran and its implications for global trade.'
    item['link'] = 'https://example.com/resilience-through-linkage-russia-iran-north-south-trade'
    assert dg.has_strong_central_asia_anchor(item) is False


def test_old_institution_longform_is_scored_before_durable_gate():
    item = {
        'source': 'Foreign Policy Centre Search Central Asia',
        'title': 'How the West Is Trying to Get Back to Central Asia',
        'link': 'https://fpc.org.uk/how-the-west-is-trying-to-get-back-to-central-asia/',
        'summary': (
            'This long-form analysis examines geopolitical competition, policy choices, '
            'China, Russia, and Western engagement with Kazakhstan and Central Asia.'
        ),
        'published': '2023-10-20',
        'content_type': 'article',
        'word_count': 3100,
        'access_status': 'open',
        'source_type': 'institution_publication',
        'institution_publication_kind': 'research_analysis',
        'source_tier': 2,
    }
    relevant, negative = dg.filter_items([item])
    assert negative == []
    assert relevant == [item]
    assert dg.is_recent_item(item) is True
    assert dg.is_research_grade_public_item(item) is True


def test_ascii_term_matching_uses_word_boundaries():
    text = 'Political turmoil in Kyrgyzstan and wider Eurasian affairs.'
    assert dg.count_terms(text.lower(), ['oil']) == 0
    assert dg.count_terms(text.lower(), ['eu']) == 0
    assert dg.count_terms(text.lower(), ['kyrgyzstan']) == 1


def test_institution_name_does_not_make_it_top_tier_media():
    item = make_item(
        'Foreign Policy Centre Search Central Asia',
        1,
        source_type='institution_publication',
    )
    assert dg.is_top_tier_media_item(item) is False


def test_refresh_clears_stale_substring_scores():
    item = make_item('Foreign Policy Centre Search Central Asia', 1)
    item['title'] = 'Retreating Rights - Kyrgyzstan: Introduction'
    item['summary'] = 'A human rights assessment of rapid political change in Kyrgyzstan.'
    item['priority_topics'] = ['关键矿产与能源转型']
    item['priority_score'] = 999
    item['research_score'] = 999
    dg.refresh_item_evidence(item)
    assert '关键矿产与能源转型' not in item['priority_topics']
    assert '治理改革与制度演进' in item['priority_topics']
    assert item['research_score'] < 999


def test_shortfall_allows_only_one_extra_item_per_source():
    source_a = [make_item('Source A', index) for index in range(5)]
    source_b = [make_item('Source B', 10 + index) for index in range(2)]
    source_c = [make_item('Source C', 20 + index) for index in range(2)]
    eligible = source_a + source_b + source_c
    initial = dg.diversify_ranked_items(eligible, 12, max_per_source=2)
    extended = dg.extend_public_shortfall(initial, eligible, minimum=8)
    assert len(extended) == 7
    assert sum(1 for item in extended if item['source'] == 'Source A') == 3


def test_publication_title_and_summary_boilerplate_cleanup():
    item = make_item('Foreign Policy Centre Search Central Asia', 1)
    item['title'] = 'Italy and Central Asia - The Foreign Policy Centre'
    assert '外交政策中心' not in dg.item_title_cn(item)
    summary = 'Substantive institutional analysis. Read more » Repeated card title'
    assert dg.clean_rss_summary_html(summary) == 'Substantive institutional analysis.'


def test_institution_metadata_prefers_body_over_download_card_boilerplate():
    metadata = {
        'summary': 'Скачать полный отчет Данная краткая аналитическая записка оценивает новую политику Казахстана... ',
        'body_summary': 'Данная краткая аналитическая записка оценивает новую политику Казахстана и ее влияние на добывающие города и регионы страны.',
    }
    assert dg.summary_is_boilerplate(metadata['summary']) is True
    assert dg.best_metadata_summary(metadata, prefer_body=True) == metadata['body_summary']


def test_institution_metadata_keeps_matching_meta_over_unrelated_body_excerpt():
    metadata = {
        'title': 'Central Asia: Facing Radical Islam',
        'summary': (
            'Twenty-five years after independence, the republics of Central Asia still face '
            'major questions of stability, security, and radicalization.'
        ),
        'body_summary': (
            'The EU India Defense Partnership in an Evolving Security Environment One Month '
            'Ahead of the Midterms: Where Is Trump America Headed?'
        ),
    }
    assert dg.best_metadata_summary(metadata, prefer_body=True) == metadata['summary']


def test_summary_display_strips_oxus_report_card_metadata():
    item = {
        'title': 'Ранние браки в Кыргызстане: Одни минусы без плюсов — The Oxus Society',
        'summary': (
            'Авторы: Динара Калдыбек кызы, Джамиля Капарова '
            'Ментор: Беймишева Айгуль, профессор, PhD '
            'Скачать полный отчет В настоящей работе представлен актуальный анализ ситуации '
            'с ранними браками в Кыргызстане...'
        ),
    }
    cleaned = dg.normalize_summary_display_text(item['summary'], item)
    assert cleaned.startswith('В настоящей работе представлен актуальный анализ')
    assert 'Скачать' not in cleaned
    assert 'Ментор' not in cleaned


def test_summary_display_strips_academic_venue_and_author_prefix():
    item = {
        'title': 'Kazakhstan leverage and diversification',
        'academic_venue': 'Third World Quarterly',
        'summary': (
            'Third World Quarterly | 作者：Linda Yin-nor Tjia | '
            '自独立以来，哈萨克斯坦一直采用多向量方法在大国之间保持平衡。'
        ),
    }
    assert dg.normalize_summary_display_text(item['summary'], item).startswith('自独立以来')
    assert dg.item_summary_cn(item).startswith('自独立以来')


def test_summary_display_marks_fixed_length_excerpt_as_incomplete():
    item = {
        'title': '中亚城市发展观察',
        'summary': (
            '中亚城市地图正在迅速变化。贸易多元化、年轻人口和快速的数字化采用正在将其主要城市转变为'
            '区域经济、创新与治理的重要节点，新的竞争格局仍在形成中。与此同时，跨境资本和技术网络正在重塑城市分工，'
            '并推动基础设施、人才流动、公共服务和区域治理关系发生持续变化，相关政策协调仍面临新的制度约束'
        ),
    }
    assert dg.item_summary_cn(item).endswith('……')


def test_known_eurasia_review_summaries_use_chinese_fallbacks():
    city = {
        'title': 'Astana And Almaty: Central Asia’s Two-City Advantage – OpEd - Eurasia Review',
        'summary': 'Central Asia’s urban map is changing fast. Trade diversification, youthful populations and rapid digital adoption are turning its principal cities into substantial markets.',
    }
    sanctions = {
        'title': 'Iran Sanctions Reach Beyond Tehran: What’s At Stake For Central Asia – Analysis - Eurasia Review',
        'summary': 'Key Takeaways: - After Trump’s 19 August “ECONOMIC D-DAY” and Bessent’s 24 August “Operation Economic Outcast,” secondary sanctions threaten Central Asia’s use of Iran as a southern door.',
    }
    assert dg.item_summary_cn(city).startswith('中亚的城市地图正在迅速变化')
    assert dg.item_summary_cn(sanctions).startswith('报告指出')


def test_current_verified_summaries_use_clean_chinese_fallbacks():
    fiber = {
        'title': 'Trans-Caspian Fiber Optic Cable — Unpacked',
        'summary': (
            "The Trans-Caspian Fiber-Optic Cable will loosen Russia's grip on Central Asia's internet. "
            'The undersea portion is complete and commercial service is targeted for the end of 2026.'
        ),
    }
    interview = {
        'title': '“A Fourth Political Reset?”: An Interview with Kyrgyz Political Scientist Emil Dzhuraev',
        'summary': (
            'An assessment of President Sadyr Japarov and the political realignment following the dismissal '
            'of the head of the security services, alongside growing economic dependence on China.'
        ),
    }
    futures = {
        'title': 'Диалогтың қысқаша мазмұны: Kazakhstan Futures — The Oxus Society',
        'summary': '2023 жылғы қаңтарда Kazakhstan Futures бағдарламасы басталып, 2050 жылға дейінгі сценарийлер зерделенді.',
    }
    assert dg.item_summary_cn(fiber).startswith('跨里海光纤电缆已完成')
    assert dg.item_summary_cn(interview).startswith('访谈评估萨德尔·扎帕罗夫')
    assert dg.item_summary_cn(futures).startswith('2023年1月')


def test_discovery_only_report_archives_are_not_hard_fetched():
    for source in dg.REPORT_DISCOVERY_ONLY_SOURCES:
        assert dg.REPORT_SOURCE_ACCESS_MODE[source] == 'discovery-only'
        assert dg.fetch_pdf_reports(source, 'https://example.org/archive') == []


def test_foreign_policy_centre_longform_is_not_treated_as_media_blurb():
    item = {
        'source': 'Foreign Policy Centre Search Central Asia',
        'title': 'Retreating Rights - Kyrgyzstan: Introduction',
        'link': 'https://fpc.org.uk/retreating-rights-kyrgyzstan-introduction/',
        'summary': 'A long-form human rights assessment of political change and governance in Kyrgyzstan.',
        'published': '2021-02-28',
        'content_type': 'research analysis',
        'word_count': 5400,
        'access_status': 'open',
        'source_type': 'institution_publication',
        'institution_publication_kind': 'research_analysis',
        'source_tier': 2,
    }
    dg.refresh_item_evidence(item)
    assert dg.is_thin_analytical_news(item) is False
    assert dg.is_public_simple_news(item) is False
    assert dg.is_research_grade_public_item(item) is True


def test_panorama_country_report_uses_normal_topic_framework():
    item = {
        'source': 'Foreign Policy Centre Search Central Asia',
        'title': 'Retreating Rights - Kyrgyzstan: Introduction',
        'link': 'https://fpc.org.uk/retreating-rights-kyrgyzstan-introduction/',
        'summary': (
            'A wide-ranging human rights assessment of political change, governance, '
            'civil society, media freedom, and the current situation in Kyrgyzstan.'
        ),
        'published': '2021-02-28',
        'content_type': 'research analysis',
        'word_count': 5400,
        'access_status': 'open',
        'source_type': 'institution_publication',
        'institution_publication_kind': 'research_analysis',
        'source_tier': 2,
    }
    dg.refresh_item_evidence(item)
    assert dg.is_country_assessment_item(item) is True
    assert '综合国情与国家形势评估' not in item['priority_topics']
    assert '综合国情与国家形势评估' not in {
        topic['label'] for topic in dg.RESEARCH_TOPIC_PRIORITIES
    }
    assert '综合国情与国家形势评估' not in dg.CORE_RESEARCH_PILLARS
    assert dg.core_research_pillar(item) == '治理改革与制度演进'
    assert item['document_form'] == 'report'


def test_year_only_country_report_is_eligible_without_fake_date():
    item = {
        'source': 'Freedom House Central Asia Country Reports',
        'title': 'Kyrgyzstan: Freedom in the World 2026 Country Report',
        'link': 'https://freedomhouse.org/country/kyrgyzstan/freedom-world/2026',
        'summary': (
            'A country report on democracy, political rights, civil liberties, '
            'governance, and freedom in Kyrgyzstan.'
        ),
        'published': '',
        'publication_year': 2026,
        'date_precision': 'year',
        'content_type': 'article',
        'word_count': 1200,
        'access_status': 'open',
        'source_type': 'institution_publication',
        'institution_publication_kind': 'research_report',
        'source_tier': 1,
        'country_assessment': True,
    }
    dg.refresh_item_evidence(item)
    assert dg.parse_item_published_date(item) is None
    assert dg.parse_item_publication_year(item) == 2026
    assert dg.has_verifiable_publication_time(item) is True
    assert dg.item_time_status(item) == '本年度研究报告（首次收录）'
    assert dg.is_recent_item(item) is True
    assert dg.is_research_grade_public_item(item) is True


def test_stable_country_report_url_is_versioned_by_edition():
    base = {
        'source': 'BTI Central Asia Country Reports',
        'link': 'https://bti-project.org/en/reports/country-report/KAZ',
        'versioned_stable_url': True,
    }
    report_2026 = dict(base, title='BTI 2026 Kazakhstan Country Report', edition_id='2026:KAZ')
    report_2028 = dict(base, title='BTI 2028 Kazakhstan Country Report', edition_id='2028:KAZ')
    keys_2026 = dg.item_history_keys(report_2026)
    keys_2028 = dg.item_history_keys(report_2028)
    assert keys_2026.isdisjoint(keys_2028)
    assert not any(key.startswith('url:') for key in keys_2026)


def test_country_assessment_direct_sources_cover_all_five_states():
    records = dg.country_assessment_seed_records()
    assert len(records) == 25
    assert {record['country'] for record in records} == {
        '哈萨克斯坦', '乌兹别克斯坦', '吉尔吉斯斯坦', '塔吉克斯坦', '土库曼斯坦',
    }
    assert {record['source'] for record in records} == dg.COUNTRY_ASSESSMENT_SOURCE_NAMES
    assert {
        dg.COUNTRY_ASSESSMENT_PROVIDERS[source]['kind']
        for source in dg.COUNTRY_ASSESSMENT_SOURCE_NAMES
    } == {'research_report'}
    assert {
        dg.INSTITUTION_SOURCE_REGISTRY[source]['kind']
        for source in dg.COUNTRY_ASSESSMENT_SOURCE_NAMES
    } == {'research_report'}


def test_neighboring_country_sources_use_multilingual_institution_pipeline():
    locales = {
        (group['hl'], group['gl'], group['ceid'])
        for group in dg.NEIGHBOR_DEEP_DISCOVERY_GROUPS
    }
    assert {('ru', 'RU', 'RU:ru'), ('tr', 'TR', 'TR:tr'), ('fa', 'IR', 'IR:fa')} <= locales
    assert dg.DEEP_DISCOVERY_TOTAL_TASKS == sum(
        len(urls) for urls in dg.DEEP_DISCOVERY_SOURCES.values()
    )
    assert dg.NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES <= dg.DURABLE_PRESTIGE_DISCOVERY_SOURCES
    assert dg.NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES <= dg.HIGH_SIGNAL_DEEP_SOURCES
    assert dg.NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES <= dg.PRESTIGE_LONGFORM_SOURCES
    assert dg.neighboring_perspective_region('Russian International Affairs Council') == '俄罗斯'
    assert dg.neighboring_perspective_region('Institute for Iran and Eurasia Studies') == '伊朗'
    assert dg.neighboring_perspective_region('Indian Council of World Affairs') == '印度'


def test_turkish_and_persian_research_signals_are_recognized():
    turkish = 'Orta Asya Kazakistan üzerine analiz ve araştırma raporu'
    persian = 'تحلیل و گزارش پژوهش درباره آسیای مرکزی و قزاقستان'
    assert dg.count_terms(turkish.lower(), dg.CORE_CA_LOWER) >= 2
    assert dg.count_terms(turkish.lower(), dg.DEPTH_LOWER) >= 3
    assert dg.count_terms(persian.lower(), dg.CORE_CA_LOWER) >= 2
    assert dg.count_terms(persian.lower(), dg.DEPTH_LOWER) >= 3


def test_neighbor_institution_discovery_keeps_durable_quality_gate():
    item = {
        'source': 'Deep Discovery: Google News RU Neighbors｜Russian International Affairs Council',
        'publisher': 'Russian International Affairs Council',
        'title': 'Central Asia: Political Economy and Regional Security',
        'link': 'https://russiancouncil.ru/en/analytics-and-comments/analytics/central-asia/',
        'summary': (
            'This research analysis examines state capacity, economic reform, '
            'regional security, migration, and foreign policy in Central Asia.'
        ),
        'published': '2025-01-10',
        'content_type': 'Analysis',
        'word_count': 1800,
        'access_status': 'open',
        'source_type': 'institution_publication',
        'institution_publication_kind': 'research_analysis',
        'source_tier': 1,
        'perspective_region': '俄罗斯',
    }
    dg.refresh_item_evidence(item)
    assert dg.is_recent_item(item) is True
    assert dg.is_strict_deep_public_item(item) is True
    assert dg.is_research_grade_public_item(item) is True


def test_china_publishers_are_not_active_when_disabled():
    assert dg.ENABLE_CHINA_PUBLISHER_SOURCES is False
    assert dg.CN_SOURCES <= dg.skipped_feed_sources()


def test_select_public_items_uses_real_publisher_quota():
    pool = []
    for index in range(6):
        source = 'The Times of Central Asia' if index % 2 == 0 else 'Deep Discovery: Google News｜The Times Of Central Asia'
        pool.append(make_item(source, index))
    selected = []
    dg.select_public_items(
        pool,
        6,
        set(),
        selected,
        source_counts={},
        max_per_source=2,
    )
    assert len(selected) == 2


def test_newer_date_and_richer_variant_win_ties():
    older = make_item('Source A', 1, published='2026-07-01')
    newer = make_item('Source B', 2, published='2026-08-02')
    assert dg.published_ordinal(newer) > dg.published_ordinal(older)

    google_copy = make_item(
        'Deep Discovery: Google News｜Source A',
        3,
        summary='Short summary.',
    )
    google_copy['link'] = 'https://news.google.com/rss/articles/example'
    direct_copy = make_item(
        'Source A',
        4,
        summary='A much longer original-page summary with policy evidence and research context.' * 3,
        word_count=1200,
    )
    assert dg.same_run_variant_score(direct_copy) > dg.same_run_variant_score(google_copy)


def test_history_covers_all_eligibility_windows():
    assert dg.SEEN_HISTORY_DAYS >= dg.ACADEMIC_LOOKBACK_DAYS
    assert dg.SEEN_HISTORY_DAYS >= dg.MAX_DEEP_ANALYSIS_AGE_DAYS


def test_runtime_settings_accept_date_and_safe_defaults():
    settings = RuntimeSettings.from_process(
        ['--date', '2026-08-18', '--no-translation'],
        environ={'DIGEST_VERIFY_TLS': 'true'},
    )
    assert settings.run_date == dt.date(2026, 8, 18)
    assert settings.translation_enabled is False
    assert settings.verify_tls is True
    assert settings.replay is False
    assert settings.rescreen is False


def test_runtime_settings_accept_offline_rescreen():
    settings = RuntimeSettings.from_process(
        ['--date', '2026-08-18', '--rescreen'],
        environ={},
    )
    assert settings.rescreen is True
    assert settings.replay is False


def test_archive_old_outputs_keeps_recent_root_files_and_supports_history_reads():
    original_out_dir = dg.OUT_DIR
    original_archive_dir = dg.ARCHIVE_DIR
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dg.OUT_DIR = root
            dg.ARCHIVE_DIR = root / 'archive'
            old_date = str(dg.TODAY - dt.timedelta(days=10))
            recent_date = str(dg.TODAY - dt.timedelta(days=2))
            old_public = root / f'CentralAsia_Research_{old_date}.md'
            old_internal = root / f'CentralAsia_Internal_Review_{old_date}.md'
            old_cache = root / f'.daily_selection_{old_date}.json'
            old_maintenance = root / f'source_maintenance_{old_date}.log'
            recent_public = root / f'CentralAsia_Research_{recent_date}.md'
            old_public.write_text(
                '**1. 可复核的历史材料**\n原文链接：https://example.com/archive-item\n',
                encoding='utf-8',
            )
            old_internal.write_text(
                '**1. 内部历史材料**\n原文链接：https://example.com/internal-item\n',
                encoding='utf-8',
            )
            old_cache.write_text('{"date":"2026-09-26","items":[]}', encoding='utf-8')
            old_maintenance.write_text('维护日志\n', encoding='utf-8')
            recent_public.write_text('# 最近一期\n', encoding='utf-8')

            moved = dg.archive_old_outputs()

            archived_public = root / 'archive' / '2026' / '09' / old_public.name
            archived_internal = root / 'archive' / '2026' / '09' / old_internal.name
            archived_cache = root / 'archive' / '2026' / '09' / old_cache.name
            archived_maintenance = root / 'archive' / '2026' / '09' / old_maintenance.name
            assert archived_public.exists()
            assert archived_internal.exists()
            assert archived_cache.exists()
            assert archived_maintenance.exists()
            assert not old_public.exists()
            assert not old_internal.exists()
            assert not old_cache.exists()
            assert not old_maintenance.exists()
            assert recent_public.exists()
            assert dg.find_output_file(root / old_public.name) == archived_public
            assert dg.find_output_file(root / old_cache.name) == archived_cache

            history = dg.seed_seen_history_from_outputs({})
            assert old_date in history
            assert dg.collect_prior_internal_review_keys()
            assert dg.archive_old_outputs() == []
            assert len(moved) == 4
    finally:
        dg.OUT_DIR = original_out_dir
        dg.ARCHIVE_DIR = original_archive_dir


def test_sino_watch_rejects_old_durable_institution_items():
    item = make_item(
        'Iran Eurasia Studies Central Asia',
        1,
        published='2018-05-17',
        source_type='institution_publication',
        source_tier=1,
        word_count=2200,
    )
    item['title'] = 'Iran and Eurasian Economic Union Negotiations: Upgrading EAEU-Iran Preferential Trade Agreement into a Free Trade Agreement'
    item['summary'] = 'This study discusses China, Iran, Central Asia, and political criticism of regional arrangements.'
    dg.refresh_item_evidence(item)
    assert dg.is_recent_item(item) is True
    assert dg.is_sino_watch_recent_item(item) is False
    assert dg.sino_watch_basic_candidate(item) is False
    assert dg.sino_watch_rejection_reason(item) == '日期缺失或超出时效窗口'


def test_retry_after_supports_seconds_and_http_dates():
    assert retry_after_seconds('5') == 5.0
    now = dt.datetime(2026, 8, 18, 0, 0, tzinfo=dt.timezone.utc)
    assert retry_after_seconds('Tue, 18 Aug 2026 00:00:03 GMT', now=now) == 3.0


def test_atomic_json_write_is_valid_utf8_json():
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / 'state.json'
        atomic_write_json(path, {'标题': '中亚', 'items': [1, 2]})
        assert json.loads(path.read_text(encoding='utf-8')) == {'标题': '中亚', 'items': [1, 2]}


def test_same_day_anchor_is_ranked_before_unanchored_items():
    cached = [make_item('Anchor Source', 1)]
    live_anchor = make_item('Anchor Source', 2, summary='A richer live variant.')
    live_other = make_item('Other Source', 3)
    assert dg.mark_same_day_anchors([live_anchor, live_other], cached) == 1
    assert dg.same_day_anchor_sort_key(live_anchor) < dg.same_day_anchor_sort_key(live_other)


def test_daily_selection_cache_rejects_old_pipeline_version():
    original_path = dg.DAILY_SELECTION_CACHE_FILE
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            dg.DAILY_SELECTION_CACHE_FILE = Path(temp_dir) / '.daily_selection.json'
            atomic_write_json(dg.DAILY_SELECTION_CACHE_FILE, {
                'date': str(dg.TODAY),
                'pipeline_version': '0.0.0',
                'items': [make_item('Old Cache', 1)],
            })
            assert dg.load_daily_selection_cache() == []
            stale_items = dg.load_daily_selection_cache(allow_pipeline_mismatch=True)
            assert len(stale_items) == 1
            assert stale_items[0]['stale_pipeline_cache'] is True
    finally:
        dg.DAILY_SELECTION_CACHE_FILE = original_path


def test_recent_research_has_higher_freshness_bucket_than_archive_work():
    recent = make_item('CSIS Russia and Eurasia Publications', 1, published=str(dg.TODAY - dt.timedelta(days=20)))
    archive = make_item('CSIS Russia and Eurasia Publications', 2, published=str(dg.TODAY - dt.timedelta(days=1500)))
    assert dg.research_freshness_bucket(recent) > dg.research_freshness_bucket(archive)


def test_known_dead_feeds_are_not_restored_by_s_tier_force():
    skipped = dg.skipped_feed_sources()
    assert dg.KNOWN_DEAD_FEED_SOURCES <= skipped
    active = {source for source in dg.FEEDS if source not in skipped}
    assert not (active & dg.KNOWN_DEAD_FEED_SOURCES)
    assert {'Oxus Society', 'Oxus Society RSS'} <= skipped


def test_maintenance_window_moves_repeated_failures_out_of_daily_hard_fetches():
    skipped = dg.skipped_feed_sources()
    assert {
        'Central Asia Program (Wilson Center)',
        'Eurasian Research Institute', 'Human Rights Watch', 'KAS Central Asia',
    } <= skipped
    active_candidates = dg.get_active_candidate_web_sources()
    assert not ({
        'IMF Central Asia', 'Ifri Central Asia', 'MERICS China Central Asia',
        'RIAC Search Central Asia', 'RUSI Search Central Asia',
        'Stimson Search Central Asia', 'Manohar Parrikar IDSA Central Asia',
    } & set(active_candidates))


def test_source_maintenance_distinguishes_repair_from_low_frequency_retest():
    inventory = {
        entry['key']: entry for entry in dg.source_maintenance_inventory()
    }
    wilson = inventory['RSS|Central Asia Program (Wilson Center)']
    assert wilson['status'] == 'known-dead'
    assert inventory['CANDIDATE_WEB|IMF Central Asia']['status'] == 'discovery-only'
    assert 'RSS|TDH TM' not in inventory
    assert all(
        entry['status'] in {'known-dead', 'stable-skip', 'paused', 'discovery-only'}
        for entry in inventory.values()
    )


def test_source_maintenance_existing_endpoint_is_only_for_stable_skip():
    original_state = dg.SOURCE_MAINTENANCE_FILE
    original_log = dg.SOURCE_MAINTENANCE_LOG
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            dg.SOURCE_MAINTENANCE_FILE = Path(temp_dir) / 'state.json'
            dg.SOURCE_MAINTENANCE_LOG = Path(temp_dir) / 'maintenance.log'
            summary = dg.run_source_maintenance(max_probes=0)
            state = json.loads(dg.SOURCE_MAINTENANCE_FILE.read_text(encoding='utf-8'))
            dead = state['sources']['RSS|bne IntelliNews Central Asia']
            candidate = state['sources']['CANDIDATE_WEB|IMF Central Asia']
            assert dead['last_result'] == 'awaiting-repair-path'
            assert candidate['last_result'] == 'awaiting-repair-path'
            assert summary['probed'] == 0
            assert summary['awaiting_repair'] >= 2
    finally:
        dg.SOURCE_MAINTENANCE_FILE = original_state
        dg.SOURCE_MAINTENANCE_LOG = original_log


def maintenance_entry(kind='RSS', source='Fixture', status='stable-skip', url='https://example.org/feed'):
    return {'key': kind + '|' + source, 'kind': kind, 'source': source,
            'status': status, 'urls': [url]}


@contextmanager
def isolated_source_maintenance(entries, state=None, repairs=None):
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = Path(temp_dir) / 'state.json'
        if state is not None:
            atomic_write_json(state_file, state)
        with patch.multiple(dg, SOURCE_MAINTENANCE_FILE=state_file,
                            SOURCE_MAINTENANCE_LOG=Path(temp_dir) / 'maintenance.log',
                            SOURCE_MAINTENANCE_REPAIR_CANDIDATES=repairs or {}), \
                patch.object(dg, 'source_maintenance_inventory', return_value=entries), \
                patch.object(dg, '_maintenance_alternative_coverage', return_value=[]):
            yield state_file


class MaintenanceResponse:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass


def test_source_maintenance_dispatches_actual_rss_and_web_probes():
    entries = [maintenance_entry(), maintenance_entry('WEB', 'Web Fixture', url='https://example.org/research')]
    rss = MaintenanceResponse(
        '<rss version="2.0"><channel><title>Research</title><item>'
        '<title>Central Asia institutions report</title><link>https://example.org/report</link>'
        '<pubDate>Fri, 09 Oct 2026 00:00:00 GMT</pubDate></item></channel></rss>'
    )
    web = MaintenanceResponse('<article><h2><a href="/analysis/central-asia">Central Asia institutions report</a></h2></article>')
    with isolated_source_maintenance(entries) as state_file, \
            patch.object(dg, 'request_url', side_effect=[rss, web]) as request:
        summary = dg.run_source_maintenance(max_probes=2)
        assert summary['success'] == 2 and summary['failed'] == 0
        assert request.call_count == 2
        state = json.loads(state_file.read_text(encoding='utf-8'))
        assert state['sources']['RSS|Fixture']['repair_candidates'][0]['kind'] == 'RSS'
        assert state['sources']['WEB|Web Fixture']['last_item_count'] == 1


def test_source_maintenance_rss_requires_verifiable_entry_dates():
    response = MaintenanceResponse(
        '<rss version="2.0"><channel><item><title>Central Asia institutions report</title>'
        '<link>https://example.org/report</link></item></channel></rss>'
    )
    with isolated_source_maintenance([maintenance_entry()]), \
            patch.object(dg, 'request_url', return_value=response):
        summary = dg.run_source_maintenance(max_probes=1)
        assert summary['failed'] == 1 and summary['success'] == 0


def test_source_maintenance_web_does_not_count_navigation_or_wrong_paths():
    response = MaintenanceResponse(
        '<nav><a href="/publication/archive">All Central Asia publications</a></nav>'
        '<article><a href="/event/central-asia">Central Asia institutions seminar</a></article>'
    )
    with patch.object(dg, 'request_url', return_value=response):
        try:
            dg._maintenance_probe_web('https://example.org/region/central-asia', 'Central Asia Program (Wilson Center)')
        except RuntimeError as exc:
            assert 'no usable publication links' in str(exc)
        else:
            raise AssertionError('Navigation or an event must not qualify as a publication link')


def test_source_maintenance_dead_and_discovery_entries_never_probe_old_urls():
    entries = [maintenance_entry(status='known-dead'), maintenance_entry('CANDIDATE_WEB', 'Discovery', 'discovery-only')]
    repairs = {entries[0]['key']: [{'kind': 'RSS', 'url': entries[0]['urls'][0]}]}
    with isolated_source_maintenance(entries, repairs=repairs), \
            patch.object(dg, '_maintenance_probe') as probe:
        summary = dg.run_source_maintenance(max_probes=6)
        assert summary['awaiting_repair'] == 2 and summary['probed'] == 0
        probe.assert_not_called()


def test_source_maintenance_verified_replacement_can_change_adapter_kind():
    entry = maintenance_entry(status='known-dead')
    repairs = {entry['key']: [{'kind': 'WEB', 'url': 'https://example.org/new-publications'}]}
    with isolated_source_maintenance([entry], repairs=repairs), \
            patch.object(dg, '_maintenance_probe', return_value=3) as probe:
        assert dg.run_source_maintenance(max_probes=1)['success'] == 1
        assert probe.call_args.args[0]['kind'] == 'WEB'


def test_source_maintenance_discovery_only_report_is_not_active_coverage():
    with patch.dict(dg.REPORT_SOURCE_ACCESS_MODE, {'ADB Publications': 'discovery-only'}):
        coverage = dg._maintenance_alternative_coverage('WEB', 'ADB Central and West Asia')
        assert not any(route.get('source') == 'ADB Publications' for route in coverage)
    with patch.dict(dg.REPORT_SOURCE_ACCESS_MODE, {'ADB Publications': 'direct-archive'}):
        coverage = dg._maintenance_alternative_coverage('WEB', 'ADB Central and West Asia')
        assert any(route.get('source') == 'ADB Publications' for route in coverage)
    for entry in dg.source_maintenance_inventory():
        for route in dg._maintenance_alternative_coverage(entry['kind'], entry['source']):
            if route['kind'] == 'REPORT':
                assert dg.REPORT_SOURCE_ACCESS_MODE.get(route['source']) != 'discovery-only'


def test_source_maintenance_successes_are_endpoint_bound_and_on_distinct_days():
    entry = maintenance_entry()
    start = dg.TODAY
    with isolated_source_maintenance([entry]) as state_file, \
            patch.object(dg, '_maintenance_due', return_value=True), \
            patch.object(dg, '_maintenance_probe', return_value=3) as probe:
        assert dg.run_source_maintenance(max_probes=1)['promotable'] == 0
        assert dg.run_source_maintenance(max_probes=1)['promotable'] == 0
        with patch.object(dg, 'TODAY', start + dt.timedelta(days=7)):
            assert dg.run_source_maintenance(max_probes=1)['promotable'] == 1
        entry['urls'] = ['https://example.org/different-feed']
        with patch.object(dg, 'TODAY', start + dt.timedelta(days=14)):
            assert dg.run_source_maintenance(max_probes=1)['promotable'] == 0
        probe.side_effect = RuntimeError('connection failure')
        with patch.object(dg, 'TODAY', start + dt.timedelta(days=21)):
            assert dg.run_source_maintenance(max_probes=1)['failed'] == 1
        probe.side_effect = None
        with patch.object(dg, 'TODAY', start + dt.timedelta(days=28)):
            assert dg.run_source_maintenance(max_probes=1)['promotable'] == 0
        record = json.loads(state_file.read_text(encoding='utf-8'))['sources'][entry['key']]
        assert record['successes'] == 1 and 'recommendation' not in record
        assert len(record['endpoint_checks']) == 2 and 'last_error' not in record


def test_source_maintenance_budget_prioritizes_oldest_due_entry():
    first = maintenance_entry()
    older = maintenance_entry('WEB', 'Z Older', url='https://example.org/research')
    state = {'sources': {
        first['key']: {'next_due': str(dg.TODAY)},
        older['key']: {'next_due': str(dg.TODAY - dt.timedelta(days=7))},
    }}
    with isolated_source_maintenance([first, older], state=state), \
            patch.object(dg, '_maintenance_probe', return_value=3) as probe:
        summary = dg.run_source_maintenance(max_probes=1)
        assert summary['probed'] == 1
        assert probe.call_args.args[0]['source'] == 'Z Older'


def test_source_maintenance_invalidates_legacy_dispatch_errors():
    entry = maintenance_entry()
    state = {'version': 1, 'sources': {entry['key']: {
        'next_due': str(dg.TODAY + dt.timedelta(days=7)),
        'last_error': 'unsupported maintenance candidate kind',
        'successes': 5, 'recommendation': 'ready-for-adapter-review',
    }}}
    with isolated_source_maintenance([entry], state=state) as state_file, \
            patch.object(dg, '_maintenance_probe', return_value=2):
        summary = dg.run_source_maintenance(max_probes=1)
        record = json.loads(state_file.read_text(encoding='utf-8'))['sources'][entry['key']]
        assert summary['probed'] == 1 and summary['promotable'] == 0
        assert record['successes'] == 1 and 'invalidated_dispatch_error' in record


def test_source_maintenance_corrupt_record_and_storage_failure_are_isolated():
    entry = maintenance_entry()
    with isolated_source_maintenance([entry], state={'sources': {entry['key']: 'corrupt'}}), \
            patch.object(dg, '_maintenance_probe', return_value=2):
        assert dg.run_source_maintenance(max_probes=1)['success'] == 1
        with patch.object(dg, '_save_source_maintenance_state', side_effect=PermissionError('state locked')), \
                patch.object(dg, 'SOURCE_WARNINGS', []):
            summary = dg.run_source_maintenance_safely()
            assert 'state locked' in summary['error']
            assert dg.SOURCE_WARNINGS[-1]['kind'] == 'SOURCE_MAINTENANCE'


def test_candidate_history_does_not_record_unrequested_sources_as_empty():
    requested = next(iter(dg.CANDIDATE_WEB_SOURCES))
    skipped = 'IMF Central Asia'
    assert requested != skipped
    previous = [{'date': str(dg.TODAY - dt.timedelta(days=1)), 'items': 3, 'warning': False}]
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / 'candidate-history.json'
        atomic_write_json(path, {skipped: previous})
        with patch.multiple(dg, CANDIDATE_HEALTH_FILE=path, TEST_CANDIDATE_SOURCES=True, SOURCE_WARNINGS=[]):
            dg.update_candidate_history([(requested, 2)])
        history = json.loads(path.read_text(encoding='utf-8'))
        assert history[skipped] == previous
        assert history[requested][-1]['items'] == 2


def test_offline_modes_never_run_source_maintenance():
    for flag, operation in [('--replay', 'replay_saved_render'), ('--rescreen', 'rescreen_saved_selection')]:
        runtime = RuntimeSettings.from_process(argv=[flag], environ={})
        with patch.object(dg, 'RUNTIME', runtime), patch.object(dg, operation) as offline, \
                patch.object(dg, 'run_source_maintenance_safely') as maintenance:
            dg.main()
            offline.assert_called_once()
            maintenance.assert_not_called()


def test_restored_official_entrypoints_use_current_urls_and_selectors():
    assert dg.FEEDS['TDH TM'] == ['https://www.tdh.gov.tm/en/rss.xml']
    assert dg.WEB_SOURCES['TDH TM'] == 'https://www.tdh.gov.tm/en'
    assert dg.FEEDS['Kapital.kz English'] == ['https://kapital.kz/feed']
    assert dg.WEB_SOURCES['Central Asia Program (Wilson Center)'] == 'https://www.wilsoncenter.org/region/central-asia'
    assert dg.SOURCE_WEB_REQUIRED_PATHS['Central Asia Program (Wilson Center)'] == ['/publication/', '/article/']
    assert dg.WEB_SOURCES['Eurasian Research Institute'] == 'https://eurasian-research.org/publications?category=ebulletin'
    assert dg.SOURCE_WEB_REQUIRED_PATHS['Eurasian Research Institute'] == ['/publications/']
    assert 'TDH TM' not in dg.KNOWN_DEAD_FEED_SOURCES
    assert 'Kapital.kz English' not in dg.KNOWN_DEAD_FEED_SOURCES


def test_academic_daily_tasks_do_not_duplicate_crossref_broad_searches():
    assert dg.ACADEMIC_DAILY_TASK_COUNT == len(dg.ACADEMIC_QUERIES) + 2
    assert len(dg.CROSSREF_DAILY_JOURNAL_KEYS) <= 8


def test_fulltext_extraction_adapters_are_available():
    html = (
        '<html><body><nav>Navigation</nav><article>'
        '<h1>Central Asia governance study</h1>'
        '<p>Substantive evidence about Kazakhstan and regional institutions.</p>'
        '</article></body></html>'
    )
    text = extract_main_text(html, 'https://example.org/study')
    assert 'Substantive evidence' in text
    assert extract_pdf_text(b'') == ''


def test_turkish_titles_are_detected_for_translation():
    title = 'Türk Dünyasının Yeni Şahdamarı: Orta Asya’dan Kafkasya’ya Uzanan Genişletilmiş Hazar Havzası'
    assert dg.detect_lang(title) == 'tr'
    title = "ANKASAM Başkanı Prof. Dr. Mehmet Seyfettin EROL, Inbusiness.kz'de Kazakistan Güvenlik Konseyi'nin dönüşümünü değerlendirdi."
    assert dg.detect_lang(title) == 'tr'


def test_mixed_source_translation_is_rejected():
    original = 'Türk Dünyasının Yeni Şahdamarı: Orta Asya’dan Kafkasya’ya Uzanan Genişletilmiş Hazar Havzası'
    mixed = 'Türk Dünyasının Yeni Şahdamarı：Orta Asya’dan Kafkasya’ya Uzanan Genişletilmiş Hazar Havzası — 安卡拉危机与政策研究中心'
    assert dg.looks_like_source_mixture(mixed, original) is True
    assert dg.looks_translated(mixed, original) is False


def test_known_turkish_report_title_uses_verified_translation():
    title = 'Türk Dünyasının Yeni Şahdamarı: Orta Asya’dan Kafkasya’ya Uzanan Genişletilmiş Hazar Havzası ve Özbek Diplomasisinin 35. Yıl Zaferi — ANKASAM | Ankara Kriz ve Siyaset Araştırmaları Merkezi —'
    assert dg.translate_text(title) == '突厥世界的新生命线：从中亚延伸至高加索的扩展里海盆地与乌兹别克斯坦外交35周年的胜利'


def test_current_turkish_report_title_uses_verified_translation():
    title = "ANKASAM Başkanı Prof. Dr. Mehmet Seyfettin EROL, Inbusiness.kz'de Kazakistan Güvenlik Konseyi'nin dönüşümünü ve yeni kilit pozisyonun ortaya çıkışını değerlendirdi. — ANKASAM | Ankara Kriz ve Siyaset Araştırmaları Merkezi —"
    assert dg.item_title_cn({'title': title, 'link': 'https://example.org/report'}) == 'ANKASAM主席Mehmet Seyfettin Erol教授在《Inbusiness.kz》评估了哈萨克斯坦安全委员会的转型及新关键职位的出现'


def test_sino_watch_requires_china_central_asia_and_dispute_signals():
    item = make_item(
        'Sino Watch: Google News EN｜Azattyq',
        1,
        published=RECENT_TEST_DATE,
        publisher='Azattyq',
        source_type='sino_watch_discovery',
        source_tier=2,
        word_count=900,
    )
    item['title'] = 'Investigation: Chinese company project in Kazakhstan faces pollution and labor dispute'
    item['summary'] = 'This investigation examines pollution allegations, local jobs, Chinese workers, and the government response in Kazakhstan.'
    dg.refresh_item_evidence(item)
    assert dg.is_sino_watch_candidate(item) is True
    item['title'] = 'Chinese company project in Kazakhstan and its regional strategy'
    item['summary'] = 'This report describes a new Chinese investment project in Kazakhstan and its regional strategy.'
    dg.refresh_item_evidence(item)
    assert dg.is_sino_watch_candidate(item) is False


def test_recruitment_notice_is_not_research_material():
    item = make_item(
        'KISI KazISS RSS',
        1,
        published=RECENT_TEST_DATE,
        source_type='feed',
        source_tier=3,
        word_count=764,
    )
    item['title'] = (
        'The Kazakhstan Institute for Strategic Studies announces a competition '
        'for the vacant position of Head of the Department of European and American Studies'
    )
    item['link'] = 'https://kisi.kz/en/the-kazakhstan-institute-announces-a-competition-for-the-vacant-position/'
    item['summary'] = 'The institute invites applications for a vacant department head position.'
    dg.refresh_item_evidence(item)
    assert dg.is_non_research_notice(item) is True
    assert dg.is_public_low_value(item) is True
    assert dg.is_research_grade_public_item(item) is False


def test_aviation_incident_news_is_not_research_material():
    item = make_item(
        'Kursiv Kazakhstan English',
        1,
        published=RECENT_TEST_DATE,
        source_type='feed',
        source_tier=3,
        word_count=302,
    )
    item['title'] = 'Kazakhstan to strengthen aviation incident investigation'
    item['summary'] = (
        'The aviation administration and the transport accident investigation center '
        'agreed to expand cooperation to improve flight safety.'
    )
    item['link'] = 'https://example.com/kazakhstan-aviation-incident-investigation'
    dg.refresh_item_evidence(item)
    assert dg.is_transport_safety_news(item) is True
    assert dg.is_public_simple_news(item) is True
    assert dg.is_research_grade_public_item(item) is False


def test_specialist_business_news_needs_research_frame():
    item = make_item(
        'The Times of Central Asia',
        1,
        published=RECENT_TEST_DATE,
        source_type='feed',
        source_tier=3,
        word_count=870,
    )
    item['title'] = 'U.S. Eyes Tajikistan Antimony as China Dominates Processing'
    item['summary'] = (
        'U.S. interest in Tajikistan antimony is growing as Washington seeks '
        'alternative critical mineral supplies while China retains a strong '
        'position in mining and processing.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_specialist_article_without_research_frame(item) is True
    assert dg.is_research_grade_public_item(item) is False


def test_specialist_longform_with_analysis_signals_passes():
    item = make_item(
        'Caspian Policy Center RSS',
        1,
        published=RECENT_TEST_DATE,
        source_type='feed',
        source_tier=3,
        word_count=756,
    )
    item['title'] = 'Trans-Caspian Links: The Growing Investment Portfolio Between Azerbaijan and Uzbekistan'
    item['summary'] = (
        'This analysis examines the investment portfolio, institutional constraints, '
        'transport corridor governance, and the strategic implications of deeper '
        'Azerbaijan-Uzbekistan cooperation.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_specialist_article_without_research_frame(item) is False
    assert dg.is_research_grade_public_item(item) is True


def test_specialist_short_summary_is_not_saved_by_body_word_count():
    item = make_item(
        'Caspian Policy Center RSS',
        1,
        published=RECENT_TEST_DATE,
        source_type='feed',
        source_tier=3,
        word_count=756,
    )
    item['title'] = 'Trans-Caspian Links: The Growing Investment Portfolio Between Azerbaijan and Uzbekistan'
    item['summary'] = (
        'Cooperation between Azerbaijan and Uzbekistan remains vital as the two sides '
        'look to bolster trans-Caspian relations.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_specialist_article_without_research_frame(item) is True
    assert dg.is_research_grade_public_item(item) is False


def test_leaders_attending_summit_is_not_deep_research():
    item = make_item(
        'China Global South Central Asia',
        1,
        published=RECENT_TEST_DATE,
        source_type='web_discovery',
        source_tier=2,
        word_count=359,
    )
    item['title'] = 'Leaders of Russia, China, and Iran to Attend Summit in Kyrgyzstan'
    item['summary'] = (
        'The leaders will attend an upcoming summit in Kyrgyzstan alongside other '
        'heads of state, highlighting the importance of the regional grouping.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_event_preview_or_diplomatic_blurb(item) is True
    assert dg.is_public_simple_news(item) is True
    assert dg.is_research_grade_public_item(item) is False


def test_specialist_cultural_feature_is_not_research_material():
    item = make_item(
        'Novastan English',
        1,
        published=RECENT_TEST_DATE,
        source_type='feed',
        source_tier=3,
        word_count=2177,
    )
    item['title'] = 'In the Country of Ozbeg: Photographing Uzbekistan 136 Years After Paul Nadar'
    item['summary'] = (
        'A photographer revisits Uzbekistan through historical archives and images '
        'from a journey made more than a century ago.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_non_research_feature(item) is True
    assert dg.is_public_low_value(item) is True
    assert dg.is_specialist_article_without_research_frame(item) is True
    assert dg.is_research_grade_public_item(item) is False


def test_sino_watch_rejects_social_platform_sources():
    item = make_item(
        'Sino Watch: Google News EN｜Facebook',
        1,
        published=RECENT_TEST_DATE,
        publisher='Facebook',
        source_type='sino_watch_discovery',
        source_tier=2,
        word_count=1200,
    )
    item['title'] = 'Investigation: Chinese company project in Kazakhstan faces pollution dispute'
    item['summary'] = 'This investigation examines pollution allegations and local opposition in Kazakhstan.'
    dg.refresh_item_evidence(item)
    assert dg.is_sino_watch_candidate(item) is False


def test_sino_watch_query_groups_are_kept_separate():
    assert set(dg.SINO_WATCH_DISCOVERY_SOURCES) == {
        'Sino Watch: Google News EN',
        'Sino Watch: Google News RU',
        'Sino Watch: Google News TR/CA',
        'Sino Watch Local Search: Kazakhstan (kk)',
        'Sino Watch Local Search: Uzbekistan (uz)',
        'Sino Watch Local Search: Kyrgyzstan (ky)',
        'Sino Watch Local Search: Tajikistan (tg)',
        'Sino Watch Local Search: Turkmenistan (tk)',
    }
    assert dg.SINO_WATCH_TOTAL_TASKS == sum(
        len(urls) for urls in dg.SINO_WATCH_DISCOVERY_SOURCES.values()
    )
    assert all(len(urls) > 0 for urls in dg.SINO_WATCH_DISCOVERY_SOURCES.values())


def test_sino_watch_local_source_heads_are_wired():
    direct_sources = set(dg.SINO_WATCH_LOCAL_FEEDS) | set(dg.SINO_WATCH_LOCAL_WEB_SOURCES)
    assert len(dg.SINO_WATCH_LOCAL_FEEDS) == 9
    assert len(dg.SINO_WATCH_LOCAL_WEB_SOURCES) == 13
    assert direct_sources <= set(dg.SINO_WATCH_LOCAL_SOURCE_META)
    assert 'Sino Watch Local: Tajikistan (tg/ru)｜Khovar' not in direct_sources
    assert dg.SINO_WATCH_LOCAL_FEEDS['Sino Watch Local: Tajikistan (tg)｜Asia-Plus'].endswith('/feed/')


def test_sino_watch_local_language_signals_use_country_metadata():
    item = make_item(
        'Sino Watch Local: Kazakhstan (kk)｜Egemen Qazaqstan',
        1,
        published=RECENT_TEST_DATE,
        source_type='sino_watch_local',
        source_tier=2,
        word_count=850,
    )
    item['title'] = 'Қытай компаниясы Қазақстандағы экология дауына тап болды'
    item['summary'] = 'Наразылық пен тергеу жергілікті қауымның қарсылығын күшейтті.'
    signals = dg.sino_watch_signal_counts(item)
    assert signals['china'] >= 1
    assert signals['central_asia'] >= 1
    assert signals['dispute'] >= 1


def test_sino_watch_local_debt_or_mobility_news_is_not_china_related():
    vehicle_item = make_item(
        'Sino Watch Local: Kyrgyzstan (ky)｜Azattyk',
        1,
        published=RECENT_TEST_DATE,
        source_type='sino_watch_local',
        source_tier=2,
        word_count=900,
    )
    vehicle_item['title'] = 'Чет өлкөдөн мыйзамсыз ташылып келген автоунаалар легалдаштырылды'
    vehicle_item['summary'] = 'Мыйзамсыз ташылып келген автоунаалар жана мамлекеттик каттоо жөнүндө маалымат.'
    dg.refresh_item_evidence(vehicle_item)
    assert dg.sino_watch_signal_counts(vehicle_item)['china'] == 0
    assert dg.is_sino_watch_candidate(vehicle_item) is False

    mobility_item = make_item(
        'Sino Watch Local: Kyrgyzstan (ky)｜Azattyk',
        2,
        published=RECENT_TEST_DATE,
        source_type='sino_watch_local',
        source_tier=2,
        word_count=900,
    )
    mobility_item['title'] = 'QazETA: Кыргыз жарандары Казакстанга тиркеме аркылуу кире баштайт'
    mobility_item['summary'] = 'Казакстанга кирүүдө колдонулуучу тиркеме жана чек арадагы каттоо тартиби.'
    dg.refresh_item_evidence(mobility_item)
    assert dg.sino_watch_signal_counts(mobility_item)['china'] == 0
    assert dg.is_sino_watch_candidate(mobility_item) is False


def test_sino_watch_short_cyrillic_china_term_uses_word_boundary():
    item = make_item(
        'Sino Watch Local: Kyrgyzstan (ky)｜Azattyk',
        1,
        published=RECENT_TEST_DATE,
        source_type='sino_watch_local',
        source_tier=2,
    )
    item['title'] = 'Маалымат: автоунаалар мыйзамдаштырылды'
    item['summary'] = 'Анын ичинен 48 машине рулу оң жактан сол тарабына которулган.'
    assert dg.sino_watch_signal_counts(item)['china'] == 0
    item['title'] = 'Чин менен Кыргызстан кызматташтыгы тууралуу талдоо'
    item['summary'] = 'Чин тараптын долбоорлору боюнча коомдук пикир жана талаштар талданат.'
    assert dg.sino_watch_signal_counts(item)['china'] >= 1


def test_sino_watch_international_source_scope_covers_non_local_layers():
    assert 'Reuters World' in dg.SINO_WATCH_INTERNATIONAL_MEDIA_SOURCES
    assert 'International Crisis Group Central Asia' in dg.SINO_WATCH_INTERNATIONAL_RESEARCH_SOURCES
    assert 'World Bank ECA Publications' in dg.SINO_WATCH_INTERNATIONAL_MULTILATERAL_SOURCES
    assert 'RIAC Search Central Asia' in dg.SINO_WATCH_INTERNATIONAL_NEIGHBOR_SOURCES
    assert 'Academic: OpenAlex' in dg.SINO_WATCH_INTERNATIONAL_SOURCE_NAMES
    assert len(dg.SINO_WATCH_INTERNATIONAL_SOURCE_NAMES) >= 100


def test_sino_watch_reuses_international_main_pipeline_records_without_mutation():
    item = make_item(
        'Reuters World',
        1,
        published=RECENT_TEST_DATE,
        source_type='feed',
        source_tier=3,
        word_count=1100,
    )
    item['title'] = 'Investigation: Chinese-backed project in Kazakhstan faces pollution and labor dispute'
    item['summary'] = (
        'This investigation examines pollution allegations, local jobs, Chinese workers, '
        'contract disputes, and the government response in Kazakhstan.'
    )
    reused, audit = dg.collect_sino_watch_international_items([item])
    assert len(reused) == 1
    assert audit['reused_items'] == 1
    assert reused[0]['sino_watch_origin'] == 'international_main_pipeline'
    assert reused[0]['sino_watch_source_type'] == 'sino_watch_international'
    assert reused[0]['sino_watch_source_family'] == '国际媒体与区域专业媒体'
    assert reused[0]['source_tier'] == 1
    assert 'sino_watch_origin' not in item
    dg.refresh_item_evidence(reused[0])
    assert dg.is_sino_watch_candidate(reused[0]) is True


def test_sino_watch_international_scope_handles_discovery_publishers_and_excludes_local_heads():
    discovery = make_item(
        'Deep Discovery: Google News｜Reuters',
        1,
        published=RECENT_TEST_DATE,
        source_type='top_tier_media_discovery',
        publisher='Reuters',
    )
    local = make_item(
        'Sino Watch Local: Kazakhstan (kk)｜Azattyq',
        2,
        published=RECENT_TEST_DATE,
        source_type='sino_watch_local',
    )
    assert dg.is_sino_watch_international_source(discovery) is True
    assert dg.sino_watch_international_source_family(discovery['source']) == '国际媒体与区域专业媒体'
    assert dg.is_sino_watch_international_source(local) is False


def test_sino_watch_positive_cooperation_is_not_negative_signal():
    item = make_item(
        'Sino Watch Local: Kyrgyzstan (ky/ru)｜Kabar',
        1,
        published=RECENT_TEST_DATE,
        source_type='sino_watch_local',
        source_tier=2,
    )
    item['title'] = 'Kyrgyzstan can export berries to China'
    item['summary'] = (
        'Kyrgyzstan signed a phytosanitary protocol and can export berries '
        'to the Chinese market.'
    )
    dg.refresh_item_evidence(item)
    assert dg.sino_watch_signal_counts(item)['negative'] == 0
    assert dg.sino_watch_basic_candidate(item) is False

    local_item = dict(item)
    local_item['title'] = 'Бишкек менен Чанша шаары кызматташтыкты бекемдейт'
    local_item['summary'] = 'Бишкек шаарынын мэри Кытайдан келген делегация менен жолугушту.'
    dg.refresh_item_evidence(local_item)
    assert dg.sino_watch_is_positive_only(local_item) is True
    assert dg.sino_watch_basic_candidate(local_item) is False

    neutral_competition = dict(item)
    neutral_competition['title'] = (
        'Pax Silica же WAICO: Казакстан жасалма интеллект үчүн '
        'жарышта кайсы компанияны тандайт?'
    )
    neutral_competition['summary'] = (
        'Кытай түзгөн уюм жана Казакстандын көп векторлуу саясатынын '
        'компаниясынын тандоосу талкууланат.'
    )
    dg.refresh_item_evidence(neutral_competition)
    assert dg.sino_watch_signal_counts(neutral_competition)['negative'] == 0
    assert dg.sino_watch_basic_candidate(neutral_competition) is False


def test_sino_watch_russian_transport_word_does_not_trigger_spor_dispute_term():
    item = make_item(
        'Sino Watch: Google News RU',
        1,
        published=RECENT_TEST_DATE,
        publisher='open.kg',
        source_type='sino_watch_discovery',
        source_tier=3,
        word_count=480,
    )
    item['title'] = (
        'Эрлист Акунбеков призвал китайские компании совместно развивать '
        'транспортные маршруты Кыргызстана'
    )
    item['summary'] = (
        'Во время рабочей поездки китайская делегация обсудила сотрудничество '
        'и развитие транспортных маршрутов.'
    )
    dg.refresh_item_evidence(item)
    assert 'спор' not in dg.sino_watch_negative_terms_near_china(item)
    assert dg.sino_watch_is_positive_only(item) is True
    assert dg.sino_watch_basic_candidate(item) is False


def test_sino_watch_negative_issue_keeps_candidate_path():
    item = make_item(
        'Sino Watch Local: Kazakhstan (ru/kk)｜Vlast.kz',
        1,
        published=RECENT_TEST_DATE,
        source_type='sino_watch_local',
        source_tier=2,
        word_count=1200,
    )
    item['title'] = 'Investigation finds pollution linked to Chinese company in Kazakhstan'
    item['summary'] = (
        'The investigation documents pollution, local complaints, and a dispute '
        'over the company project in Kazakhstan.'
    )
    dg.refresh_item_evidence(item)
    signals = dg.sino_watch_signal_counts(item)
    assert signals['negative'] >= 2
    assert dg.sino_watch_basic_candidate(item) is True
    assert dg.is_sino_watch_candidate(item) is True


def test_sino_watch_lead_pool_keeps_strict_rejection_and_is_independent():
    item = make_item(
        'Sino Watch: Google News EN',
        1,
        published=RECENT_TEST_DATE,
        publisher='Low Signal Blog',
        source_type='sino_watch_discovery',
        source_tier=3,
        word_count=260,
    )
    item['title'] = 'Investigation: Chinese company project in Kazakhstan faces pollution and labor dispute'
    item['summary'] = (
        'This investigation examines pollution allegations, local jobs, Chinese workers, '
        'contract disputes, and the government response in Kazakhstan.'
    )
    dg.refresh_item_evidence(item)
    assert dg.sino_watch_basic_candidate(item) is True
    assert dg.is_sino_watch_candidate(item) is False
    assert dg.sino_watch_rejection_reason(item) == '来源信誉不足'

    original_paths = {
        'SINO_WATCH_FILE': dg.SINO_WATCH_FILE,
        'SINO_WATCH_JSON_FILE': dg.SINO_WATCH_JSON_FILE,
        'SINO_WATCH_HISTORY_FILE': dg.SINO_WATCH_HISTORY_FILE,
        'SINO_WATCH_LEAD_POOL_FILE': dg.SINO_WATCH_LEAD_POOL_FILE,
        'SINO_WATCH_LEAD_POOL_JSON_FILE': dg.SINO_WATCH_LEAD_POOL_JSON_FILE,
        'SINO_WATCH_LEAD_HISTORY_FILE': dg.SINO_WATCH_LEAD_HISTORY_FILE,
    }
    original_resolvers = (dg.resolve_item_links, dg.enrich_items_for_output)
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dg.SINO_WATCH_FILE = root / 'strict.md'
            dg.SINO_WATCH_JSON_FILE = root / 'strict.json'
            dg.SINO_WATCH_HISTORY_FILE = root / 'strict-history.json'
            dg.SINO_WATCH_LEAD_POOL_FILE = root / 'lead.md'
            dg.SINO_WATCH_LEAD_POOL_JSON_FILE = root / 'lead.json'
            dg.SINO_WATCH_LEAD_HISTORY_FILE = root / 'lead-history.json'
            history_key = sorted(dg.sino_watch_history_keys(item))[0]
            atomic_write_json(dg.SINO_WATCH_HISTORY_FILE, {
                str(dg.TODAY - dt.timedelta(days=1)): [history_key],
            })
            dg.resolve_item_links = lambda items, label='': None
            dg.enrich_items_for_output = lambda items, label='': None
            stats = dg.run_sino_watch_pipeline([item])
            assert stats['selected'] == 0
            assert stats['lead_pool']['selected'] == 1
            assert stats['lead_pool']['rejection_reasons'] == {'来源信誉不足': 1}
            lead_payload = json.loads(dg.SINO_WATCH_LEAD_POOL_JSON_FILE.read_text(encoding='utf-8'))
            assert len(lead_payload['items']) == 1
            assert lead_payload['items'][0]['strict_rejection_reason'] == '来源信誉不足'
    finally:
        for name, value in original_paths.items():
            setattr(dg, name, value)
        dg.resolve_item_links, dg.enrich_items_for_output = original_resolvers


def test_sino_watch_lead_pool_does_not_repeat_strict_selection():
    item = make_item(
        'Sino Watch Local: Kazakhstan (ru/kk)｜Vlast.kz',
        1,
        published=RECENT_TEST_DATE,
        publisher='Vlast.kz',
        source_type='sino_watch_local',
        source_tier=2,
        word_count=1200,
    )
    item['title'] = 'Investigation finds pollution linked to Chinese company in Kazakhstan'
    item['summary'] = (
        'The investigation documents pollution, local complaints, Chinese workers, and a dispute '
        'over the company project in Kazakhstan.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_sino_watch_candidate(item) is True

    original_paths = {
        'SINO_WATCH_FILE': dg.SINO_WATCH_FILE,
        'SINO_WATCH_JSON_FILE': dg.SINO_WATCH_JSON_FILE,
        'SINO_WATCH_HISTORY_FILE': dg.SINO_WATCH_HISTORY_FILE,
        'SINO_WATCH_LEAD_POOL_FILE': dg.SINO_WATCH_LEAD_POOL_FILE,
        'SINO_WATCH_LEAD_POOL_JSON_FILE': dg.SINO_WATCH_LEAD_POOL_JSON_FILE,
        'SINO_WATCH_LEAD_HISTORY_FILE': dg.SINO_WATCH_LEAD_HISTORY_FILE,
    }
    original_resolvers = (dg.resolve_item_links, dg.enrich_items_for_output)
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dg.SINO_WATCH_FILE = root / 'strict.md'
            dg.SINO_WATCH_JSON_FILE = root / 'strict.json'
            dg.SINO_WATCH_HISTORY_FILE = root / 'strict-history.json'
            dg.SINO_WATCH_LEAD_POOL_FILE = root / 'lead.md'
            dg.SINO_WATCH_LEAD_POOL_JSON_FILE = root / 'lead.json'
            dg.SINO_WATCH_LEAD_HISTORY_FILE = root / 'lead-history.json'
            dg.resolve_item_links = lambda items, label='': None
            dg.enrich_items_for_output = lambda items, label='': None
            stats = dg.run_sino_watch_pipeline([item])
            assert stats['selected'] == 1
            assert stats['lead_pool']['selected'] == 0
            assert json.loads(dg.SINO_WATCH_LEAD_POOL_JSON_FILE.read_text(encoding='utf-8'))['items'] == []
    finally:
        for name, value in original_paths.items():
            setattr(dg, name, value)
        dg.resolve_item_links, dg.enrich_items_for_output = original_resolvers


def test_gallery_pages_are_not_research_items():
    item = make_item('Iran Eurasia Studies Central Asia', 1, source_type='institution_publication')
    item['title'] = 'Historical Squares of Eurasia; The Narrative of Civilizations'
    item['link'] = (
        'https://www.iras.ir/en/gallery/'
        'historical-squares-of-eurasia-the-narrative-of-civilizations-in-the-heart-of-cities/'
    )
    item['summary'] = (
        'Call for Membership in IRAS Study Groups. The Meeting on collective '
        'security was held. Historical squares and cultural heritage.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_non_research_feature(item) is True
    assert dg.is_public_low_value(item) is True
    assert dg.is_research_grade_public_item(item) is False


def test_project_launch_news_is_not_research_item():
    item = make_item(
        'Deep Discovery: Google News｜University of Central Asia',
        1,
        source_type='discovery',
        source_tier=2,
        published=RECENT_TEST_DATE,
        word_count=810,
    )
    item['title'] = (
        'UCA and Tajik researchers launch long-term monitoring '
        'of the glacier in the Murghab river basin'
    )
    item['summary'] = (
        'UCA and Tajik researchers launched a long-term glacier monitoring '
        'project in the Murghab river basin. The first expedition took place '
        'in August 2026.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_non_research_feature(item) is True
    assert dg.is_public_low_value(item) is True
    assert dg.is_research_grade_public_item(item) is False


def test_policy_memo_collection_page_is_not_a_research_item():
    item = make_item(
        'PONARS Eurasia Policy Memos',
        1,
        published='2022-02-23',
        source_type='institution_publication',
        source_tier=1,
        word_count=607,
    )
    item['title'] = 'Relevant Past PONARS Eurasia Policy Memos on Kazakhstan'
    item['summary'] = 'A collection page linking to previously published policy memos on Kazakhstan.'
    item['link'] = 'https://www.ponarseurasia.org/relevant-past-ponars-eurasia-policy-memos-on-kazakhstan/'
    dg.refresh_item_evidence(item)
    assert dg.is_non_research_feature(item) is True
    assert dg.is_research_grade_public_item(item) is False


def test_title_only_discovery_is_not_research_grade():
    item = make_item(
        'Deep Discovery: Google News｜ORF',
        1,
        source_type='institution_publication',
        source_tier=1,
        published=RECENT_TEST_DATE,
        word_count=0,
    )
    item['title'] = 'Central Asia Strategic Rebalancing'
    item['summary'] = 'Central Asia Strategic Rebalancing ORF'
    item['link'] = 'https://example.com/research/central-asia-strategic-rebalancing'
    dg.refresh_item_evidence(item)
    assert dg.has_original_depth_evidence(item) is False
    assert dg.is_research_grade_public_item(item) is False


def test_durable_academic_backfill_can_surface_unread_older_paper():
    item = make_item(
        'Academic: OpenAlex',
        1,
        published='2020-06-01',
        source_type='academic_paper',
        source_tier=1,
        academic_quality=True,
        academic_venue='Central Asian Survey',
        content_type='journal article',
        durable_backfill=True,
        word_count=1800,
        summary=(
            'This journal article examines Central Asia, state capacity, regional governance, '
            'political economy, and institutional change across Kazakhstan and Uzbekistan.'
        ),
    )
    assert dg.is_recent_item(item) is True
    item['durable_backfill'] = False
    assert dg.is_recent_item(item) is False


def test_archive_history_key_is_checked_before_result_cap():
    item = make_item(
        'ADB Publications',
        1,
        published='2026-08-01',
        source_type='institution_publication',
        source_tier=1,
    )
    history_keys = dg.item_history_keys(item)
    assert history_keys
    assert dg.item_history_keys(item) & history_keys


def test_sitemap_pagination_query_is_preserved():
    page_one = dg.sitemap_visit_key('https://www.csis.org/sitemap.xml?page=1')
    page_nine = dg.sitemap_visit_key('https://www.csis.org/sitemap.xml?page=9')
    assert page_one != page_nine
    assert page_nine.endswith('page=9')


def test_sitemap_child_priority_checks_current_and_recent_numbered_maps_first():
    maps = [
        'https://www.brookings.edu/article-sitemap2.xml',
        'https://www.brookings.edu/article-sitemap55.xml',
        'https://www.brookings.edu/article-sitemap.xml',
    ]
    ordered = sorted(maps, key=dg.sitemap_child_priority)
    assert ordered == [maps[2], maps[1], maps[0]]
    assert dg.sitemap_child_priority('https://ecfr.eu/sitemap-post-type-post.xml') < dg.sitemap_child_priority('https://ecfr.eu/sitemap-post-type-event.xml')


def test_archive_url_year_beats_sitemap_refresh_date():
    assert dg.publication_year_from_url('https://www.sipri.org/publications/2015/example-report') == 2015
    assert dg.publication_year_from_url('https://www.sipri.org/publications/example-report') is None


def test_geopolitical_actor_is_not_entertainment_low_value():
    item = make_item(
        'CSIS Russia and Eurasia Publications',
        1,
        published='2026-06-12',
        source_type='institution_publication',
        source_tier=1,
        access_status='open',
        word_count=2550,
    )
    item['title'] = 'Eurasia’s Shifting Balance of Influence: An Opening for the United States?'
    item['summary'] = (
        'The analysis examines Central Asia and the South Caucasus as regional actors '
        'respond to the changing influence of Russia, China, Iran, and the United States.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_public_low_value(item) is False
    assert dg.is_research_grade_public_item(item) is True


def test_exchange_rate_language_does_not_exclude_macro_research():
    item = make_item(
        'World Bank Documents & Reports',
        1,
        published='2026-06-01',
        source_type='institution_publication',
        source_tier=1,
        access_status='open',
        word_count=4200,
    )
    item['title'] = 'Kazakhstan Macroeconomic Assessment and Policy Options'
    item['summary'] = (
        'This report examines Kazakhstan’s exchange rate, inflation, public finance, '
        'banking sector, and structural policy options using official economic data.'
    )
    dg.refresh_item_evidence(item)
    assert dg.is_public_low_value(item) is False


def test_fulltext_anchor_count_separates_substantive_from_incidental_mentions():
    substantive = make_item('CSIS Russia and Eurasia Publications', 1)
    substantive['title'] = 'Eurasia’s Shifting Balance of Influence'
    substantive['central_asia_fulltext_mentions'] = 14
    incidental = make_item('CSIS Russia and Eurasia Publications', 2)
    incidental['title'] = 'Maritime Strategy in the Indo-Pacific'
    incidental['central_asia_fulltext_mentions'] = 1
    assert substantive['central_asia_fulltext_mentions'] >= 2
    assert incidental['central_asia_fulltext_mentions'] < 2


def test_substantive_meta_description_is_not_replaced_by_embedded_json():
    meta_summary = (
        'With growing friction between great powers, the SCO helps smaller Central Asian '
        'states adapt strategically without entering a rigid alliance.'
    )
    embedded = '- Temur Umarov {"authors": ["Temur Umarov"], "type": "commentary", "regions": ["Central Asia"]}'
    body = (
        'The Shanghai Cooperation Organization gives Central Asian states room to balance '
        'China, Russia, and other external powers while limiting formal commitments.'
    )
    html = (
        '<html><head><title>SCO at 25</title><meta name="description" content="'
        + meta_summary
        + '"></head><body><article><p>'
        + embedded
        + '</p><p>'
        + body
        + '</p></article></body></html>'
    )

    class FakeResponse:
        headers = {'content-type': 'text/html; charset=utf-8'}
        content = html.encode('utf-8')
        text = html

        @staticmethod
        def raise_for_status():
            return None

    metadata = dg.fetch_article_metadata('https://example.org/sco-at-25', response_fetcher=lambda *args, **kwargs: FakeResponse())
    assert metadata['summary'] == meta_summary


def test_short_central_asia_meta_description_beats_unrelated_sidebar_text():
    meta_summary = 'This report analyzes the economic and geopolitical situation in Central Asia.'
    unrelated = 'The EU India Defense Partnership in an Evolving Security Environment One Month Ahead of the Midterms.'
    html = (
        '<html><head><title>Central Asia: Making Use of a Historic Opportunity</title>'
        '<meta name="description" content="' + meta_summary + '"></head><body><main>'
        '<p>' + unrelated + '</p><p>' + meta_summary + '</p>'
        '<p>It explores Western, Russian, and Chinese involvement in Central Asia and proposes concrete policy options.</p>'
        '</main></body></html>'
    )

    class FakeResponse:
        headers = {'content-type': 'text/html; charset=utf-8'}
        content = html.encode('utf-8')
        text = html

        @staticmethod
        def raise_for_status():
            return None

    metadata = dg.fetch_article_metadata('https://www.ifri.org/en/papers/example', response_fetcher=lambda *args, **kwargs: FakeResponse())
    assert metadata['summary'] == meta_summary
    assert metadata['body_summary'].startswith(meta_summary)


def test_unrelated_central_asia_meta_description_is_discarded():
    html = (
        '<html><head><title>Central Asia: Facing Radical Islam</title>'
        '<meta name="description" content="The EU India Defense Partnership in an '
        'Evolving Security Environment One Month Ahead of the Midterms: Where Is '
        'Trump America Headed?"></head><body><main><p>Subscribe for updates.</p></main>'
        '</body></html>'
    )

    class FakeResponse:
        headers = {'content-type': 'text/html; charset=utf-8'}
        content = html.encode('utf-8')
        text = html

        @staticmethod
        def raise_for_status():
            return None

    metadata = dg.fetch_article_metadata(
        'https://example.org/central-asia-facing-radical-islam',
        response_fetcher=lambda *args, **kwargs: FakeResponse(),
    )
    assert metadata['summary'] == ''
    assert metadata['summary_mismatch'] is True


def test_valid_body_summary_clears_intermediate_meta_mismatch_flag():
    long_unrelated_meta = (
        'This institutional overview discusses broad international security trends, '
        'strategic competition, public order, and policy choices across several regions '
        'without naming the country or article subject. '
    ) * 2
    html = (
        '<html><head><title>Russia Response to Unrest in Kazakhstan: Risk Versus Reward</title>'
        '<meta name="description" content="' + long_unrelated_meta + '"></head>'
        '<body><article><p>The Russian-led peacekeeping operation and the unrest in '
        'Kazakhstan created a serious test of state order, regional security and the '
        'political response to violence.</p></article></body></html>'
    )

    class FakeResponse:
        headers = {'content-type': 'text/html; charset=utf-8'}
        content = html.encode('utf-8')
        text = html

        @staticmethod
        def raise_for_status():
            return None

    metadata = dg.fetch_article_metadata(
        'https://example.org/russia-response-kazakhstan',
        response_fetcher=lambda *args, **kwargs: FakeResponse(),
    )
    assert metadata['summary_mismatch'] is False
    assert 'Kazakhstan' in metadata['summary']


def test_cached_valid_summary_clears_stale_mismatch_flag():
    item = {
        'title': 'Building cyber resilience in a digitalizing state: Kazakhstan national security',
        'summary': (
            'This study examines Kazakhstan cyber resilience, public administration, '
            'critical infrastructure and national security governance.'
        ),
        'summary_mismatch': True,
    }
    dg.refresh_item_evidence(item)
    assert item['summary_mismatch'] is False


def test_cached_unrelated_summary_is_not_rendered():
    item = {
        'title': 'Central Asia: Facing Radical Islam',
        'summary': (
            'The EU India Defense Partnership in an Evolving Security Environment One Month '
            'Ahead of the Midterms: Where Is Trump America Headed?'
        ),
    }
    assert dg.summary_context_mismatch(item['title'], item['summary']) is True
    assert dg.item_summary_cn(item) == ''


def test_priority_titles_have_researcher_quality_chinese_overrides():
    assert dg.item_title_cn({
        'title': 'Astana And Almaty: Central Asia’s Two-City Advantage – OpEd - Eurasia Review',
        'link': 'https://www.eurasiareview.com/example',
    }) == '阿斯塔纳和阿拉木图：中亚的双城优势'
    assert dg.item_title_cn({
        'title': 'Iran Sanctions Reach Beyond Tehran: What’s At Stake For Central Asia – Analysis - Eurasia Review',
        'link': 'https://www.eurasiareview.com/example',
    }) == '伊朗制裁超越德黑兰：中亚面临哪些风险'
    assert dg.item_title_cn({
        'title': 'As It Turns 25, the Shanghai Cooperation Organization Reflects the New World Order',
        'link': 'https://carnegieendowment.org/example',
    }) == '上海合作组织成立25周年：它折射出怎样的新世界秩序'
    assert dg.item_title_cn({
        'title': 'Eurasia’s Shifting Balance of Influence: An Opening for the United States?',
        'link': 'https://www.csis.org/example',
    }) == '欧亚力量格局正在重塑：美国的机会窗口？'
    assert dg.item_title_cn({
        'title': 'Цикл в новых декорациях. Как изменился Узбекистан за 10 лет без Каримова',
        'link': 'https://carnegieendowment.org/example',
    }) == '新场景下的循环：没有卡里莫夫的十年，乌兹别克斯坦发生了什么变化'
    assert '布鲁金斯' not in dg.item_title_cn({
        'title': 'China in Central Asia: Is China winning the new great game? | Brookings',
        'link': 'https://www.brookings.edu/example',
    })
    assert dg.item_title_cn({
        'title': 'Afghanistan Critical Minerals: Can Central Asia Help Unlock Them? - The Times Of Central Asia',
        'link': 'https://timesca.com/afghanistan-critical-minerals-central-asia/',
    }) == '阿富汗关键矿产：中亚能否帮助释放其开发潜力？'
    assert dg.item_title_cn({
        'title': 'Trans-Caspian Fiber Optic Cable — Unpacked',
        'link': 'https://caspianpolicy.org/research/azerbaijan/trans-caspian-fiber-optic-cable-unpacked',
    }) == '跨里海光纤电缆：进展、影响与待解问题'
    assert dg.item_title_cn({
        'title': "Azerbaijan's Pivot to Central Asia",
        'link': 'https://www.rusi.org/example',
    }) == '阿塞拜疆转向中亚'
    assert dg.item_title_cn({
        'title': 'Central Asia: Facing Radical Islam',
        'link': 'https://www.ifri.org/example',
    }) == '中亚：面对激进伊斯兰主义'
    assert dg.item_title_cn({
        'title': 'New OECD Assessment Reveals Central Asia’s Student Performance - The Times of Central Asia',
        'link': 'https://timesca.com/example',
    }) == '经合组织最新评估揭示中亚学生学业表现'
    assert dg.item_title_cn({
        'title': '“A Fourth Political Reset?”: An Interview with Kyrgyz Political Scientist Emil Dzhuraev',
        'link': 'https://novastan.org/example',
    }) == '“第四次政治重置？”：专访吉尔吉斯斯坦政治学家埃米尔·朱拉耶夫'
    assert dg.item_title_cn({
        'title': 'Диалогтың қысқаша мазмұны: Kazakhstan Futures — The Oxus Society',
        'link': 'https://oxussociety.org/example',
    }) == '对话摘要：哈萨克斯坦未来情景'


def test_summary_removes_repeated_original_title_prefix():
    item = {
        'title': '欧亚力量格局正在重塑',
        'summary': '欧亚力量格局正在重塑：本文分析中亚各国如何应对俄罗斯、中国与美国影响力变化。',
    }
    assert dg.item_summary_cn(item) == '本文分析中亚各国如何应对俄罗斯、中国与美国影响力变化。'


def test_sino_watch_same_day_history_is_replaceable():
    history = {
        str(dg.TODAY): ['url:today'],
        str(dg.TODAY - dt.timedelta(days=1)): ['url:yesterday'],
    }
    assert dg.sino_watch_prior_keys(history) == {'url:yesterday'}


def test_report_entrypoints_use_current_archive_paths():
    assert dg.PDF_REPORT_SOURCES['IWEP Kazakhstan Publications'] == 'https://iwep.kz/'
    assert dg.PDF_REPORT_SOURCES['UNICEF Europe Central Asia Reports'].endswith('/research-and-reports')
    assert 'https://isrs.uz/oz' in dg.INSTITUTION_ADAPTER_CONFIG['ISRS Analytical Materials']['alternate_urls']
    assert 'https://centralasiaforum.org/articles/' in dg.INSTITUTION_ADAPTER_CONFIG['Central Asia Forum Publications']['alternate_urls']
    assert dg.INSTITUTION_ADAPTER_CONFIG['RUSI Russia-Eurasia Publications']['sitemap_urls'] == ['https://www.rusi.org/sitemap-index.xml']
    assert dg.INSTITUTION_ADAPTER_CONFIG['ECFR Wider Europe Central Asia']['sitemap_urls'] == ['https://ecfr.eu/sitemap.xml']
    assert dg.INSTITUTION_ADAPTER_CONFIG['Ifri Papers Central Asia']['sitemap_urls'] == ['https://www.ifri.org/sitemap.xml']
    assert dg.INSTITUTION_ADAPTER_CONFIG['SIPRI Publications']['sitemap_urls'] == ['https://www.sipri.org/sitemap.xml']


if __name__ == '__main__':
    tests = [value for name, value in sorted(globals().items()) if name.startswith('test_')]
    for test in tests:
        test()
    print(f'passed {len(tests)} source-diversity tests')
