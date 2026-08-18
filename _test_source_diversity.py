import digest_generator as dg
from digest_core.http_client import retry_after_seconds
from digest_core.extraction import extract_main_text, extract_pdf_text
from digest_core.runtime import RuntimeSettings
from digest_core.state import atomic_write_json

import datetime as dt
import json
import tempfile
from pathlib import Path


def make_item(source, index, venue='', published='2026-08-03', **extra):
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


def test_known_dead_feeds_are_not_restored_by_s_tier_force():
    skipped = dg.skipped_feed_sources()
    assert dg.KNOWN_DEAD_FEED_SOURCES <= skipped
    active = {source for source in dg.FEEDS if source not in skipped}
    assert not (active & dg.KNOWN_DEAD_FEED_SOURCES)


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


if __name__ == '__main__':
    tests = [value for name, value in sorted(globals().items()) if name.startswith('test_')]
    for test in tests:
        test()
    print(f'passed {len(tests)} source-diversity tests')
