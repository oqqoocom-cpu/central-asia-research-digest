import datetime
import hashlib
import json
import sys
import os
import re
import time
import warnings
from html import unescape
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

os.environ['PYTHONIOENCODING'] = 'utf-8'

try:
    import feedparser
except ImportError:
    sys.exit('feedparser not installed')

try:
    import requests
    from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
except ImportError:
    sys.exit('requests + beautifulsoup4 not installed')

from digest_core.extraction import excerpt, extract_main_text, extract_pdf_text
from digest_core.http_client import PoliteHttpClient
from digest_core.runtime import RuntimeSettings
from digest_core.state import atomic_write_json, atomic_write_text

import urllib.request
import urllib.parse
import json as jsonmod
import urllib3
from urllib.parse import urljoin
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

PIPELINE_VERSION = '2.0.0'
GITHUB_REPOSITORY_URL = 'https://github.com/oqqoocom-cpu/central-asia-research-digest'
RUNTIME = RuntimeSettings.from_process()
if not RUNTIME.verify_tls:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
OUT_DIR = RUNTIME.output_dir or Path(__file__).parent
OUT_DIR.mkdir(parents=True, exist_ok=True)
TODAY = RUNTIME.run_date
OUTPUT_FILE = OUT_DIR / f'CentralAsia_Research_{TODAY}.md'
INTERNAL_REVIEW_FILE = OUT_DIR / f'CentralAsia_Internal_Review_{TODAY}.md'
CACHE_FILE = OUT_DIR / '.cache.json'
TRANSLATE_CACHE_FILE = OUT_DIR / '.translation_cache.json'
SOURCE_HEALTH_LOG = OUT_DIR / f'source_health_{TODAY}.log'
CANDIDATE_HEALTH_FILE = OUT_DIR / 'candidate_source_history.json'
SEEN_HISTORY_FILE = OUT_DIR / 'seen_item_history.json'
DAILY_SELECTION_CACHE_FILE = OUT_DIR / f'.daily_selection_{TODAY}.json'
DAILY_RENDER_CACHE_FILE = OUT_DIR / f'.daily_render_{TODAY}.md'
GOOGLE_NEWS_RESOLVE_CACHE_FILE = OUT_DIR / '.google_news_resolve_cache.json'
SELECTION_AUDIT_FILE = OUT_DIR / f'selection_audit_{TODAY}.json'
MAX_TRANSLATIONS_PER_RUN = 180 if RUNTIME.translation_enabled else 0
MAX_TRANSLATION_SECONDS_PER_RUN = 60
MAX_ITEM_AGE_DAYS = 14
MAX_DEEP_ANALYSIS_AGE_DAYS = 30
MAX_SLOW_PUBLICATION_AGE_DAYS = 30
NEW_DISCOVERY_LOOKBACK_DAYS = 30
DURABLE_RESEARCH_MAX_AGE_DAYS = 3650
INTERNAL_FAST_SIGNAL_MAX_AGE_DAYS = 3
ACADEMIC_LOOKBACK_DAYS = 120
DURABLE_ACADEMIC_BACKFILL_DAYS = 3650
MAX_PUBLIC_ITEMS_PER_SOURCE = 2
MAX_PUBLIC_ACADEMIC_ITEMS = 3
MAX_PUBLIC_ITEMS_PER_ACADEMIC_VENUE = 2
MIN_PUBLIC_SOURCE_DIVERSITY = 6
PREGATE_MAX_ITEMS_PER_SOURCE = 2
PREGATE_POOL_SIZE = 80
PREGATE_RECOVERY_POOL_SIZE = 60
PREGATE_RECOVERY_MAX_ITEMS_PER_SOURCE = 4
MIN_PUBLIC_RECOMMENDATIONS = 8
TARGET_PUBLIC_RECOMMENDATIONS = 12
SHORTFALL_MAX_PUBLIC_ITEMS_PER_SOURCE = 3
PUBLIC_CONVERSION_SECTION_LIMIT = 5
# Keep history at least as long as the broadest eligibility window so a paper or
# deep report cannot reappear after the old 14-day history expires.
SEEN_HISTORY_DAYS = max(
    MAX_ITEM_AGE_DAYS,
    MAX_DEEP_ANALYSIS_AGE_DAYS,
    MAX_SLOW_PUBLICATION_AGE_DAYS,
    ACADEMIC_LOOKBACK_DAYS,
    DURABLE_RESEARCH_MAX_AGE_DAYS,
)
STABLE_MODE = RUNTIME.stable_mode
TEST_CANDIDATE_SOURCES = RUNTIME.test_candidate_sources
WECHAT_SAFE_MODE = True
DOUBAO_STYLE_PUBLIC = True
RESEARCHER_LINKLIST_PUBLIC = True
ENABLE_TELEGRAM_SOURCES = False
ENABLE_PDF_REPORT_SOURCES = True
ENABLE_MEETING_MINUTES_SOURCES = True
ENABLE_ACADEMIC_SOURCES = True
ENABLE_CHINA_PUBLISHER_SOURCES = False
ENABLE_GDELT_DISCOVERY = RUNTIME.enable_gdelt
TRANSLATE_CALLS = 0
TRANSLATE_CACHE = {}
TRANSLATION_STARTED_AT = None
SOURCE_WARNINGS = []

RESEARCH_TOPIC_PRIORITIES = [
    {
        'label': '政治经济与国家能力',
        'weight': 100,
        'terms': [
            'political economy', 'state capacity', 'state-owned enterprise',
            'public finance', 'fiscal', 'budget', 'taxation', 'central bank',
            'inflation', 'exchange rate', 'debt', 'banking sector', 'privatization',
            'industrial policy', 'economic reform', 'informal economy',
            '政治经济', '国家能力', '国有企业', '公共财政', '财政', '预算',
            '税收', '央行', '通胀', '汇率', '债务', '银行业', '私有化', '产业政策',
            'экономик', 'государственн', 'бюджет', 'налог', 'инфляц', 'долг',
            'банковск', 'приватизац',
        ],
    },
    {
        'label': '安全、防务与边境秩序',
        'weight': 98,
        'terms': [
            'military', 'defence', 'defense', 'armed forces', 'security sector',
            'defence reform', 'defense reform', 'border security', 'border governance',
            'csto', 'collective security', 'arms procurement', 'military doctrine',
            'counterterrorism', 'terrorism', 'extremism', 'afghanistan', 'taliban',
            '军事', '国防', '武装力量', '安全部门', '边境安全', '边境治理', '军备采购',
            '军事学说', '反恐', '极端主义', '阿富汗', '塔利班',
            'военн', 'оборона', 'вооруженн', 'безопасност', 'границ', 'террор',
            'экстрем', 'афганистан', 'талибан',
        ],
    },
    {
        'label': '治理改革与制度演进',
        'weight': 96,
        'terms': [
            'reform', 'constitution', 'election', 'parliament', 'judiciary',
            'authoritarianism', 'elite politics', 'political transition', 'civil service',
            'public administration', 'decentralization', 'local government', 'governance',
            'human rights', 'political rights', 'civil liberties', 'rule of law',
            'media freedom', 'press freedom',
            '改革', '宪法', '选举', '议会', '司法', '威权', '精英政治', '政治转型',
            '公务员制度', '公共行政', '地方政府', '治理',
            'реформ', 'конституц', 'выбор', 'парламент', 'судебн', 'авторитар',
            'элит', 'государственн', 'децентрализац',
        ],
    },
    {
        'label': '大国关系与多向量外交',
        'weight': 94,
        'terms': [
            'china', 'russia', 'european union', 'united states',
            'u.s.', 'turkey', 'c5+1', 'sco', 'ots', 'turkic',
            'eaeu', 'csto', 'multi-vector', 'partnership', 'summit',
            '中国', '俄罗斯', '欧盟', '美国', '土耳其', '上合',
            '突厥', '欧亚经济联盟', '集安组织', '多向量', '峰会',
            'китай', 'росси', 'евросоюз', 'сша', 'турц', 'шанхай',
        ],
    },
    {
        'label': '水资源与气候约束',
        'weight': 84,
        'terms': [
            'water', 'water crisis', 'water dispute', 'irrigation', 'drought',
            'climate', 'glacier', 'hydropower', 'dam', 'aral sea',
            'amu darya', 'syr darya', 'water code',
            '水资源', '跨境水资源', '灌溉', '干旱', '气候', '冰川',
            '水电', '水库', '阿姆河', '锡尔河', '咸海',
            'вода', 'водн', 'засух', 'орошен', 'ледник', 'гидро',
        ],
    },
    {
        'label': '中间走廊与互联互通',
        'weight': 86,
        'terms': [
            'middle corridor', 'trans-caspian', 'titr', 'transport corridor',
            'corridor', 'transit', 'logistics', 'railway', 'seaport', 'port of', 'aktau',
            'kuryk', 'baku', 'alat', 'caspian',
            '中间走廊', '跨里海', '运输走廊', '过境运输', '物流',
            '铁路', '港口', '阿克套', '库雷克', '巴库',
            'коридор', 'транзит', 'логист', 'железн', 'порт',
        ],
    },
    {
        'label': '关键矿产与能源转型',
        'weight': 90,
        'terms': [
            'critical minerals', 'rare earth', 'uranium', 'copper', 'lithium',
            'gold', 'mining', 'nuclear', 'rosatom', 'cnnc', 'renewable',
            'electricity', 'power grid', 'data center', 'ai center',
            '关键矿产', '稀土', '铀', '铜', '锂', '黄金', '采矿',
            '核电', '新能源', '电力', '电网', '数据中心', '人工智能',
            'редкозем', 'уран', 'медь', 'литий', 'золото', 'добыч',
            'атом', 'электроэнерг', 'энергет',
        ],
    },
    {
        'label': '劳务移民与社会结构',
        'weight': 74,
        'terms': [
            'migration', 'labor migration', 'remittance', 'diaspora',
            'demographic', 'census', 'education', 'language policy',
            'women', 'youth', 'poverty', 'inequality',
            '劳务移民', '移民', '侨汇', '人口', '普查', '教育',
            '语言政策', '女性', '青年', '贫困', '不平等',
            'миграц', 'денежн', 'перевод', 'перепис', 'образован',
        ],
    },
    {
        'label': '阿富汗关联与边境风险',
        'weight': 82,
        'terms': [
            'afghanistan', 'taliban', 'border', 'refugee', 'drug trafficking',
            'extremism', 'terrorism', 'border closure',
            '阿富汗', '塔利班', '边境', '难民', '贩毒', '极端主义',
            '恐怖主义',
            'афганистан', 'талибан', 'границ', 'бежен', 'наркот',
        ],
    },
]

RESEARCH_TOPIC_TERMS_LOWER = [
    {
        'label': topic['label'],
        'weight': topic['weight'],
        'terms': [term.lower() for term in topic['terms']],
    }
    for topic in RESEARCH_TOPIC_PRIORITIES
]

def load_cache():
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {}

def load_translation_cache():
    if TRANSLATE_CACHE_FILE.exists():
        try:
            with open(TRANSLATE_CACHE_FILE, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_translation_cache():
    try:
        atomic_write_json(TRANSLATE_CACHE_FILE, TRANSLATE_CACHE)
    except Exception:
        pass

def parse_date_from_output_name(path):
    match = re.search(r'CentralAsia_(?:Research|Digest)_(\d{4}-\d{2}-\d{2})\.md$', path.name)
    if not match:
        return None
    try:
        return datetime.date.fromisoformat(match.group(1))
    except Exception:
        return None

def parse_internal_review_date_from_name(path):
    match = re.search(r'CentralAsia_Internal_Review_(\d{4}-\d{2}-\d{2})\.md$', path.name)
    if not match:
        return None
    try:
        return datetime.date.fromisoformat(match.group(1))
    except Exception:
        return None

def collect_history_keys_from_markdown(path):
    """Rebuild full history keys (incl. slug_sig/title_core_sig) from past digests."""
    keys = set()
    try:
        text = path.read_text(encoding='utf-8')
    except Exception:
        return keys
    current_title = ''
    for line in text.splitlines():
        stripped = line.strip()
        # Digest bullets look like: "- 原文链接：https://..."
        stripped = re.sub(r'^[-*•]\s+', '', stripped)
        if re.match(r'^\*\*\d+\.\s+.+\*\*$', stripped):
            title = re.sub(r'^\*\*\d+\.\s+', '', stripped)
            title = re.sub(r'\*\*$', '', title)
            current_title = title
            keys.update(title_history_keys(title))
            continue
        if stripped.startswith('原题｜') or stripped.startswith('原题：'):
            title = stripped.split('｜', 1)[-1] if '｜' in stripped else stripped.split('：', 1)[-1]
            title = title.strip()
            current_title = title
            keys.update(title_history_keys(title))
            continue
        if stripped.startswith('原文链接｜') or stripped.startswith('原文链接：'):
            link = stripped.split('｜', 1)[-1] if '｜' in stripped else stripped.split('：', 1)[-1]
            link = link.strip()
            # Full item keys so cross-publisher slug signatures are preserved.
            keys.update(item_history_keys({'title': current_title, 'link': link}))
            # Also key the raw English slug path even if title is Chinese.
            keys.update(link_history_keys(link))
            slug_sig = link_slug_signature_key(link)
            if slug_sig:
                keys.add(slug_sig)
            current_title = ''
            continue
        # Fallback: bare http(s) links under an item block.
        if stripped.startswith('http://') or stripped.startswith('https://'):
            link = stripped.split()[0].strip()
            keys.update(item_history_keys({'title': current_title, 'link': link}))
            slug_sig = link_slug_signature_key(link)
            if slug_sig:
                keys.add(slug_sig)
    return keys

def prune_seen_history(history):
    cutoff = TODAY - datetime.timedelta(days=SEEN_HISTORY_DAYS)
    pruned = {}
    for date_text, keys in history.items():
        try:
            date_value = datetime.date.fromisoformat(date_text)
        except Exception:
            continue
        if cutoff <= date_value <= TODAY:
            pruned[date_text] = sorted(set(keys))
    return pruned

def seed_seen_history_from_outputs(history):
    """Merge keys from past digests using CURRENT key algorithms.

    Always re-collect from markdown so upgraded signatures (slug_sig /
    title_core_sig) backfill older history that was saved under old formats.
    Existing keys are kept and merged to avoid losing in-run-only signals.
    """
    history = dict(history or {})
    for path in OUT_DIR.glob('CentralAsia_*_*.md'):
        date_value = parse_date_from_output_name(path)
        if not date_value or date_value == TODAY:
            continue
        if (TODAY - date_value).days > SEEN_HISTORY_DAYS:
            continue
        date_text = str(date_value)
        keys = set(history.get(date_text, []))
        keys.update(collect_history_keys_from_markdown(path))
        if keys:
            history[date_text] = sorted(keys)
    return prune_seen_history(history)

def load_seen_history():
    history = {}
    if SEEN_HISTORY_FILE.exists():
        try:
            history = json.loads(SEEN_HISTORY_FILE.read_text(encoding='utf-8'))
        except Exception:
            history = {}
    return seed_seen_history_from_outputs(history)

def collect_prior_internal_review_keys():
    keys = set()
    for path in OUT_DIR.glob('CentralAsia_Internal_Review_*.md'):
        date_value = parse_internal_review_date_from_name(path)
        if not date_value or date_value == TODAY:
            continue
        if (TODAY - date_value).days > SEEN_HISTORY_DAYS:
            continue
        keys.update(collect_history_keys_from_markdown(path))
    return keys

def prior_seen_keys(history):
    keys = set()
    today_text = str(TODAY)
    for date_text, date_keys in history.items():
        if date_text == today_text:
            continue
        keys.update(date_keys)
    return keys

def save_seen_history(history, published_items):
    history = prune_seen_history(history)
    # A rerun replaces today's digest. Do not permanently consume items that
    # appeared only in an earlier, superseded run of the same day's file.
    today_keys = set()
    for item in published_items:
        today_keys.update(item_history_keys(item))
    history[str(TODAY)] = sorted(today_keys)
    atomic_write_json(SEEN_HISTORY_FILE, history)

def load_daily_selection_cache():
    """Keep qualified same-day reads stable across transient source failures."""
    if not DAILY_SELECTION_CACHE_FILE.exists():
        return []
    try:
        payload = json.loads(DAILY_SELECTION_CACHE_FILE.read_text(encoding='utf-8'))
    except Exception:
        return []
    items = payload.get('items', []) if isinstance(payload, dict) else []
    return [item for item in items if isinstance(item, dict)]

def save_daily_selection_cache(published_items):
    payload = {
        'date': str(TODAY),
        'pipeline_version': PIPELINE_VERSION,
        'items': list(published_items or []),
    }
    atomic_write_json(DAILY_SELECTION_CACHE_FILE, payload, default=str)


def replay_saved_render():
    """Restore the exact saved edition for a date without touching the network."""
    if not DAILY_RENDER_CACHE_FILE.exists():
        raise SystemExit(
            'No saved render is available for ' + str(TODAY) + ': '
            + str(DAILY_RENDER_CACHE_FILE)
        )
    rendered_markdown = DAILY_RENDER_CACHE_FILE.read_text(encoding='utf-8')
    atomic_write_text(OUTPUT_FILE, rendered_markdown)
    print('Replayed saved edition without network access: ' + str(OUTPUT_FILE))


def same_day_anchor_identity_keys(item):
    """Stable identities used only to preserve qualified selections on same-day reruns."""
    keys = set()
    link = normalize_history_link(item.get('link', ''))
    if link:
        keys.add('url:' + link)
    normalized = normalize_key(item)
    if normalized:
        keys.add('item:' + normalized)
    title_key = title_core_signature_key(item.get('title', ''))
    if title_key:
        keys.add(title_key)
    return keys


def mark_same_day_anchors(items, cached_items):
    """Mark live variants that correspond to the prior qualified same-day selection."""
    anchor_ranks = {}
    for rank, cached in enumerate(cached_items or []):
        for key in same_day_anchor_identity_keys(cached):
            anchor_ranks[key] = min(rank, anchor_ranks.get(key, rank))
    marked = 0
    for item in items or []:
        ranks = [
            anchor_ranks[key]
            for key in same_day_anchor_identity_keys(item)
            if key in anchor_ranks
        ]
        if not ranks:
            continue
        item['same_day_anchor'] = True
        item['same_day_anchor_rank'] = min(ranks)
        marked += 1
    return marked


def same_day_anchor_sort_key(item):
    if item.get('same_day_anchor'):
        return (0, int(item.get('same_day_anchor_rank', 9999) or 0))
    return (1, 9999)


def append_project_collaboration_note(lines):
    """Keep the open-source invitation visible without competing with readings."""
    lines.append('项目协作：本简报项目已在 GitHub 开源，欢迎研究者使用、反馈并贡献来源适配器、筛选规则和测试：['
                 + GITHUB_REPOSITORY_URL.rsplit('/', 1)[-1] + '](' + GITHUB_REPOSITORY_URL + ')。请勿上传文章原文、个人凭据、缓存或历史日报。')


def selection_quality_evidence(item):
    evidence = []
    if has_verifiable_publication_time(item):
        evidence.append('可核验发布时间或报告年份')
    if has_strong_central_asia_anchor(item):
        evidence.append('标题/摘要/正文含强中亚锚点')
    if item.get('enriched') or item.get('summary_enriched'):
        evidence.append('已抓取原文页面并富集摘要')
    if int(item.get('word_count', 0) or 0) >= 700:
        evidence.append('原文正文达到长文阈值')
    if item.get('source_type') == 'academic_paper' or item.get('academic_quality'):
        evidence.append('通过白名单学术论文门禁')
    if is_report_grade_item(item) or item.get('source_type') == 'institution_publication':
        evidence.append('机构报告/研究出版物形态')
    if item.get('access_status') == 'open':
        evidence.append('公开全文或正文可访问')
    if item.get('same_day_anchor'):
        evidence.append('同日合格选择锚点')
    return evidence


def selection_audit_item(item, rank):
    return {
        'rank': rank,
        'title': clean_text(item.get('title', '')),
        'source': clean_text(item.get('source', '')),
        'publisher': clean_text(item.get('publisher', '') or deep_discovery_publisher(item.get('source', ''))),
        'canonical_source': canonical_source_name(item.get('publisher', '') or item.get('source', '')),
        'link': clean_text(item.get('link', '')),
        'published': clean_text(item.get('published', '')),
        'publication_year': item.get('publication_year'),
        'date_precision': clean_text(item.get('date_precision', '')),
        'source_type': clean_text(item.get('source_type', '')),
        'source_tier': int(item.get('source_tier', 3) or 3),
        'access_status': clean_text(item.get('access_status', 'unknown')),
        'content_type': clean_text(item.get('content_type', '')),
        'document_form': clean_text(item.get('document_form', '')),
        'evidence_type': clean_text(item.get('evidence_type', '')),
        'research_function': clean_text(item.get('research_function', '')),
        'geography_scope': list(item.get('geography_scope', []) or []),
        'tags': item_public_tags(item),
        'priority_topics': list(item.get('priority_topics', []) or []),
        'scores': {
            'priority': item.get('priority_score', 0),
            'depth': item.get('depth_score', 0),
            'research': item.get('research_score', 0),
            'policy_data': item.get('policy_data_score', 0),
            'central_asia': item.get('core_score', 0),
            'keywords': item.get('kw_score', 0),
        },
        'word_count': int(item.get('word_count', 0) or 0),
        'academic_venue': clean_text(item.get('academic_venue', '')),
        'academic_authors': list(item.get('academic_authors', []) or []),
        'same_day_anchor': bool(item.get('same_day_anchor')),
        'quality_evidence': selection_quality_evidence(item),
    }


def write_selection_audit(
    published_items,
    *,
    all_items,
    relevant,
    deduped,
    internal_review_items,
    cross_day_skipped,
    same_run_skipped,
):
    payload = {
        'pipeline_version': PIPELINE_VERSION,
        'date': str(TODAY),
        'runtime': {
            'stable_mode': STABLE_MODE,
            'replay': RUNTIME.replay,
            'translation_enabled': RUNTIME.translation_enabled,
            'verify_tls': RUNTIME.verify_tls,
            'openalex_api_key_configured': bool(RUNTIME.openalex_api_key),
            'crossref_mailto_configured': bool(RUNTIME.crossref_mailto),
        },
        'funnel': {
            'fetched': len(all_items),
            'central_asia_relevant': len(relevant),
            'public_pool_after_gates': len(deduped),
            'published': len(published_items),
            'internal_review': len(internal_review_items),
            'cross_day_skipped': cross_day_skipped,
            'same_run_skipped': same_run_skipped,
        },
        'items': [selection_audit_item(item, index) for index, item in enumerate(published_items, start=1)],
    }
    atomic_write_json(SELECTION_AUDIT_FILE, payload, default=str)

def repair_mojibake(text):
    """Repair common UTF-8-as-Latin-1 corruption without touching valid text."""
    if not text or not isinstance(text, str):
        return text
    # Cyrillic UTF-8 decoded as Latin-1 typically contains repeated Ã/Ð/Ñ
    # markers. Require a clear density signal before attempting reversal.
    markers = sum(text.count(ch) for ch in ('Ã', 'Â', 'Ð', 'Ñ', 'â', '�'))
    if markers < 2:
        return text
    try:
        repaired = text.encode('latin1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    # Accept only if the corruption markers materially decrease.
    new_markers = sum(repaired.count(ch) for ch in ('Ã', 'Â', 'Ð', 'Ñ', 'â', '�'))
    return repaired if new_markers < markers else text


def clean_text(text):
    if not text:
        return ''
    if isinstance(text, (list, tuple, set)):
        text = '; '.join(clean_text(value) for value in text if value)
    elif isinstance(text, dict):
        text = '; '.join(clean_text(value) for value in text.values() if value)
    else:
        text = str(text)
    text = repair_mojibake(text)
    text = BeautifulSoup(text, 'lxml').get_text(' ')
    text = unescape(text)
    # A few Drupal/search pages emit replacement characters for curly
    # apostrophes and en dashes even when the declared charset is UTF-8.
    text = re.sub(r'��(?=[st]\b)', "'", text)
    text = re.sub(r'(?<=\w)�C(?=\w)', '–', text)
    text = text.replace('�', '')
    text = re.sub(r'\s+', ' ', text).strip()
    return text

SOURCE_BOILERPLATE_PATTERNS = [
    r'\bThe Astana Times provides news and information from Kazakhstan and around the world\.?\s*',
    r'\bASTANA\b\s*[-–—:|?]\s*',
    # Strip publisher brand suffixes so "Central Asia-Caucasus Analyst" in titles
    # does not fake a Central Asia content anchor.
    r'\s*[-–—|]\s*Central Asia-Caucasus Analyst\b.*$',
    r'\s*[-–—|]\s*The Times Of Central Asia\b.*$',
    r'\s*[-–—|]\s*The Diplomat(?:\s*[–—-]\s*Asia-Pacific)?\b.*$',
    r'\s*[-–—|]\s*Eurasianet\b.*$',
    r'\s*[-–—|]\s*The Conversation\b.*$',
    r'\s*[-–—|]\s*Radio Free Europe(?:/Radio Liberty)?\b.*$',
]

def strip_source_boilerplate(text):
    text = clean_text(text)
    for pattern in SOURCE_BOILERPLATE_PATTERNS:
        text = re.sub(pattern, ' ', text, flags=re.IGNORECASE)
    return clean_text(text)

def link_relevance_text(link):
    parsed = urllib.parse.urlparse(link or '')
    path = urllib.parse.unquote(parsed.path or '')
    path = path.replace('-', ' ').replace('_', ' ')
    return clean_text(path)

def normalize_key(item):
    link = (item.get('link') or '').split('#')[0].split('?')[0].rstrip('/')
    title = re.sub(r'\W+', '', (item.get('title') or '').lower())
    return link or title

def normalize_history_link(link):
    return (link or '').strip().split('#')[0].split('?')[0].rstrip('/')

def normalize_title_key(title):
    return re.sub(r'\W+', '', clean_text(title or '').lower())

GENERIC_HISTORY_SLUGS = {
    'news', 'article', 'articles', 'analysis', 'publication', 'publications',
    'en', 'ru', 'uz', 'kz', 'kg', 'tj', 'tm', 'world', 'asia', 'index',
    'index.html', 'main',
}

HISTORY_TITLE_STOPWORDS = {
    'the', 'and', 'for', 'with', 'from', 'into', 'onto', 'about', 'over',
    'under', 'amid', 'after', 'before', 'this', 'that', 'will', 'has',
    'have', 'had', 'was', 'were', 'are', 'its', 'his', 'her', 'their',
    'president', 'minister', 'ministry', 'republic', 'official', 'news',
    'jan', 'january', 'feb', 'february', 'mar', 'march', 'apr', 'april',
    'may', 'jun', 'june', 'jul', 'july', 'aug', 'august', 'sep', 'sept',
    'september', 'oct', 'october', 'nov', 'november', 'dec', 'december',
    # Publisher / syndication brands that differ across reposts of the same piece.
    'eurasianet', 'eurasia', 'review', 'diplomat', 'conversation',
    'cacianalyst', 'analyst', 'cabar', 'novastan', 'jamestown',
    'analysis', 'commentary', 'op-ed', 'oped', 'feature',
    'times', 'astana', 'radio', 'liberty', 'google',
}

def link_history_keys(link):
    keys = set()
    normalized = normalize_history_link(link)
    if not normalized:
        return keys
    keys.add('url:' + normalized)
    parsed = urllib.parse.urlparse(normalized)
    domain = parsed.netloc.lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    path = parsed.path.rstrip('/').lower()
    if domain and path:
        keys.add('url_path:' + domain + path)
        parts = [part for part in path.split('/') if part]
        if parts:
            slug = parts[-1]
            if len(slug) >= 4 and slug not in GENERIC_HISTORY_SLUGS:
                keys.add('url_slug:' + domain + '/' + slug)
    return keys

def strip_leading_date_from_title(title):
    text = clean_text(title or '').strip().lower()
    if not text:
        return ''
    month_pattern = '|'.join(MONTH_NAME_TO_NUMBER)
    text = re.sub(r'^\s*\d{1,2}\s+(' + month_pattern + r')\.?\s*[-–—:|]?\s+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^\s*(' + month_pattern + r')\.?\s+\d{1,2}\s*[-–—:|]?\s+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^\s*\d{1,2}\s*月\s*\d{0,2}\s*日?\s*[-–—:|]?\s*', '', text)
    text = re.sub(r'^\s*\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\s*[-–—:|]?\s*', '', text)
    text = re.sub(r'^\s*\d{1,2}[-/.]\d{1,2}(?:[-/.]\d{2,4})?\s*[-–—:|]?\s*', '', text)
    return clean_text(text)

def clean_history_title(title):
    """Normalize titles for cross-publisher dedupe (strip dates and outlet brands)."""
    text = strip_source_boilerplate(title or '')
    text = strip_leading_date_from_title(text)
    # Drop trailing " - Publisher Name" / " | Publisher Name".
    text = re.sub(r'\s*[-–—|]\s*[A-Za-z][A-Za-z0-9 .,&/\-]{2,80}$', '', text)
    # Drop leading format labels.
    text = re.sub(r'^(news analysis|analysis|expert views|commentary|feature)\s*[:：\-]\s*', '', text, flags=re.I)
    return clean_text(text)

def title_signature_tokens(title):
    text = clean_history_title(title)
    if not text:
        return []
    tokens = re.findall(r'[a-zA-Z][a-zA-Z0-9-]{2,}|[А-Яа-яЁёӘәҒғҚқҢңӨөҰұҮүҺһІіЎўҲҳ]{3,}|[\u4e00-\u9fff]{2,}', text.lower())
    filtered = []
    for token in tokens:
        token = token.strip('-')
        if not token or token in HISTORY_TITLE_STOPWORDS or token.isdigit():
            continue
        # Split hyphenated compounds for more stable signatures.
        if '-' in token:
            parts = [p for p in token.split('-') if p and p not in HISTORY_TITLE_STOPWORDS and len(p) >= 3]
            filtered.extend(parts)
        else:
            filtered.append(token)
    # unique preserve semantic content; sort for stable key
    return sorted(set(filtered))

def title_signature_key(title):
    unique_tokens = title_signature_tokens(title)
    if len(unique_tokens) < 4:
        return ''
    return 'title_sig:' + ' '.join(unique_tokens[:14])

def title_core_signature_key(title):
    """Publisher-agnostic signature used to catch syndicated reposts."""
    unique_tokens = title_signature_tokens(title)
    if len(unique_tokens) < 4:
        return ''
    # Prefer the densest content tokens; ignore residual brand fragments.
    return 'title_core_sig:' + ' '.join(unique_tokens[:10])

def link_slug_signature_key(link):
    """Match same story when reposted on another domain with similar slug."""
    normalized = normalize_history_link(link)
    if not normalized:
        return ''
    parsed = urllib.parse.urlparse(normalized)
    path = urllib.parse.unquote(parsed.path or '').rstrip('/').lower()
    if not path:
        return ''
    slug = path.split('/')[-1]
    if not slug or slug in GENERIC_HISTORY_SLUGS:
        return ''
    slug = re.sub(r'^\d{6,8}-', '', slug)  # leading date like 16072026-
    slug = re.sub(r'^(news-analysis|analysis|expert-views|commentary)-', '', slug)
    slug = re.sub(r'-(analysis|commentary|feature|html?)$', '', slug)
    parts = [p for p in re.split(r'[-_]+', slug) if p and p not in HISTORY_TITLE_STOPWORDS and not p.isdigit() and len(p) >= 3]
    parts = sorted(set(parts))
    if len(parts) < 4:
        return ''
    return 'slug_sig:' + ' '.join(parts[:12])

def title_history_keys(title):
    keys = set()
    cleaned = clean_history_title(title)
    title_key = normalize_title_key(cleaned or title)
    if title_key:
        keys.add('title:' + title_key)
    core_key = normalize_title_key(cleaned)
    if core_key and core_key != title_key and len(core_key) >= 8:
        keys.add('title_core:' + core_key)
    signature = title_signature_key(title)
    if signature:
        keys.add(signature)
    core_sig = title_core_signature_key(title)
    if core_sig:
        keys.add(core_sig)
    return keys

def item_history_keys(item):
    keys = set()
    edition_id = clean_text(item.get('edition_id', ''))
    versioned_stable_url = item.get('versioned_stable_url') is True
    if edition_id and versioned_stable_url:
        normalized_link = normalize_history_link(item.get('link', ''))
        if normalized_link:
            keys.add('edition_url:' + normalized_link + ':' + edition_id.lower())
        title_key = normalize_title_key(item.get('title', ''))
        if title_key:
            keys.add('edition_title:' + edition_id.lower() + ':' + title_key)
    else:
        keys.update(link_history_keys(item.get('link', '')))
        keys.update(title_history_keys(item.get('title', '')))
    slug_sig = '' if versioned_stable_url else link_slug_signature_key(item.get('link', ''))
    if slug_sig:
        keys.add(slug_sig)
    return keys

def exact_history_link_keys(item):
    """Return only exact normalized URL identities for post-resolution checks."""
    edition_id = clean_text(item.get('edition_id', ''))
    if edition_id and item.get('versioned_stable_url') is True:
        normalized_link = normalize_history_link(item.get('link', ''))
        return ({'edition_url:' + normalized_link + ':' + edition_id.lower()}
                if normalized_link else set())
    return {
        key for key in link_history_keys(item.get('link', ''))
        if key.startswith('url:') or key.startswith('url_path:')
    }


def drop_history_duplicate_items(items, prior_keys, current_keys=None, skipped_sink=None, exact_link_only=False):
    """Drop items whose history keys collide with prior days or this pass.

    The initial pass uses title and slug signatures to catch syndicated copies.
    After Google News resolution, however, title cleaning can introduce broad
    fuzzy collisions. At that stage only an exact resolved URL is decisive.
    """
    current_keys = set(current_keys or [])
    kept = []
    skipped = 0
    for item in items or []:
        item_keys = exact_history_link_keys(item) if exact_link_only else item_history_keys(item)
        if item_keys & prior_keys or item_keys & current_keys:
            skipped += 1
            if skipped_sink is not None:
                skipped_sink.append(item)
            continue
        current_keys.update(item_keys)
        kept.append(item)
    return kept, skipped, current_keys


MONTH_NAME_TO_NUMBER = {
    'january': 1, 'jan': 1,
    'february': 2, 'feb': 2,
    'march': 3, 'mar': 3,
    'april': 4, 'apr': 4,
    'may': 5,
    'june': 6, 'jun': 6,
    'july': 7, 'jul': 7,
    'august': 8, 'aug': 8,
    'september': 9, 'sep': 9, 'sept': 9,
    'october': 10, 'oct': 10,
    'november': 11, 'nov': 11,
    'december': 12, 'dec': 12,
}

def safe_date(year, month, day):
    try:
        return datetime.date(int(year), int(month), int(day))
    except Exception:
        return None

def parse_date_text(text):
    text = clean_text(text)
    if not text:
        return None
    normalized = text.replace('年', '-').replace('月', '-').replace('日', ' ')
    iso_match = re.search(r'\b(20\d{2})[-/.](0?[1-9]|1[0-2])[-/.](0?[1-9]|[12]\d|3[01])(?=\D|$)', normalized)
    if iso_match:
        return safe_date(*iso_match.groups())
    path_match = re.search(r'/(20\d{2})/(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])(?:/|$)', normalized)
    if path_match:
        return safe_date(*path_match.groups())
    slash_date_match = re.search(r'\b(0?[1-9]|[12]\d|3[01])/(0?[1-9]|[12]\d|3[01])/(20\d{2})\b', normalized)
    if slash_date_match:
        first, second, year = slash_date_match.groups()
        day_first = safe_date(year, second, first)
        month_first = safe_date(year, first, second)
        plausible = [date for date in [day_first, month_first] if date and date <= TODAY + datetime.timedelta(days=1)]
        if len(plausible) == 1:
            return plausible[0]
        if plausible:
            # Archive/search pages normally lead with their newest material.
            return max(plausible)
    day_first_match = re.search(r'\b(0?[1-9]|[12]\d|3[01])[-.](0?[1-9]|1[0-2])[-.](20\d{2})\b', normalized)
    if day_first_match:
        day, month, year = day_first_match.groups()
        return safe_date(year, month, day)
    month_match = re.search(
        r'\b(' + '|'.join(MONTH_NAME_TO_NUMBER) + r')\.?\s+(0?[1-9]|[12]\d|3[01]),?\s+(20\d{2})\b',
        normalized,
        re.IGNORECASE,
    )
    if month_match:
        month_name, day, year = month_match.groups()
        return safe_date(year, MONTH_NAME_TO_NUMBER[month_name.lower().rstrip('.')], day)
    reverse_month_match = re.search(
        r'\b(0?[1-9]|[12]\d|3[01])\s+(' + '|'.join(MONTH_NAME_TO_NUMBER) + r')\.?,?\s+(20\d{2})\b',
        normalized,
        re.IGNORECASE,
    )
    if reverse_month_match:
        day, month_name, year = reverse_month_match.groups()
        return safe_date(year, MONTH_NAME_TO_NUMBER[month_name.lower().rstrip('.')], day)
    month_no_year_match = re.search(
        r'\b(' + '|'.join(MONTH_NAME_TO_NUMBER) + r')\.?\s+(0?[1-9]|[12]\d|3[01])\b',
        normalized,
        re.IGNORECASE,
    )
    if month_no_year_match:
        month_name, day = month_no_year_match.groups()
        return safe_date(TODAY.year, MONTH_NAME_TO_NUMBER[month_name.lower().rstrip('.')], day)
    reverse_month_no_year_match = re.search(
        r'\b(0?[1-9]|[12]\d|3[01])\s+(' + '|'.join(MONTH_NAME_TO_NUMBER) + r')\.?\b',
        normalized,
        re.IGNORECASE,
    )
    if reverse_month_no_year_match:
        day, month_name = reverse_month_no_year_match.groups()
        return safe_date(TODAY.year, MONTH_NAME_TO_NUMBER[month_name.lower().rstrip('.')], day)
    return None

def parse_item_published_date(item):
    published = clean_text(item.get('published', ''))
    if published:
        try:
            return datetime.datetime.fromisoformat(published.replace('Z', '+00:00')).date()
        except Exception:
            pass
        try:
            return parsedate_to_datetime(published).date()
        except Exception:
            pass
        parsed = parse_date_text(published)
        if parsed:
            return parsed
    return parse_date_text(
        item.get('title', '') + ' ' +
        item.get('summary', '') + ' ' +
        item.get('link', '')
    )

def parse_item_publication_year(item):
    """Return a verified publication year without inventing a calendar date."""
    for key in ['publication_year', 'published_year']:
        raw = clean_text(item.get(key, ''))
        if re.fullmatch(r'20\d{2}', raw):
            year = int(raw)
            if 2000 <= year <= TODAY.year + 1:
                return year
    item_date = parse_item_published_date(item)
    return item_date.year if item_date else None

def has_verifiable_publication_time(item):
    return bool(parse_item_published_date(item) or parse_item_publication_year(item))

def item_age_days(item):
    """Comparable age for gates; year-only reports retain year precision."""
    item_date = parse_item_published_date(item)
    if item_date:
        return (TODAY - item_date).days
    year = parse_item_publication_year(item)
    if year is None:
        return None
    return (TODAY.year - year) * 365

def infer_date_from_context(*parts):
    return parse_date_text(' '.join(clean_text(part) for part in parts if part))

def detect_lang(text):
    if re.search(r'[\u4e00-\u9fff]', text or ''):
        return 'zh'
    if re.search(r'\b(o[ʻ’\'`]?zbekiston|toshkent|qarshi|islohot|qonun|vazir|prezidenti)\b', text or '', re.IGNORECASE):
        return 'uz'
    if re.search(r'[ЎўҲҳ]', text or ''):
        return 'uz'
    if re.search(r'[ӘәҒғҚқҢңӨөҰұҮүҺһІі]', text or ''):
        return 'kk'
    if re.search(r'[\u0400-\u04ff]', text or ''):
        return 'ru'
    return 'en'

MANUAL_TRANSLATION_RULES = [
    (
        ['контрольный выстрел', 'швейн', 'кыргызстан'],
        '补枪：俄罗斯如何压垮吉尔吉斯斯坦服装业',
    ),
    (
        ['төраға ауыстыру', 'жасылдар партия'],
        '更换主席、批评“阿迪列特”：绿党代表大会如何举行？',
    ),
    (
        ['ресейдегі нысандарға дрон', 'қазақстаннан'],
        '阿斯塔纳否认有关无人机从哈萨克斯坦飞向俄罗斯境内目标的报道',
    ),
    (
        ['ўзбекистоннинг коррупцияга қарши ислоҳоти'],
        '乌兹别克斯坦反腐改革正获得国际层面的评价',
    ),    (
        ['recommendations for scaling-up nature-based solutions', 'resilient landscapes', 'central asia'],
        '扩大中亚韧性景观中基于自然的解决方案应用：政策建议',
    ),
]

COUNTRY_TRANSLATION_EXPECTATIONS = [
    (['ўзбекистон', 'узбекистан', 'o‘zbekiston', 'oʻzbekiston', "o'zbekiston", 'uzbekistan'], '乌兹别克'),
    (['қазақстан', 'казахстан', 'kazakhstan'], '哈萨克'),
    (['қырғызстан', 'кыргызстан', 'киргизия', 'kyrgyzstan'], '吉尔吉斯'),
    (['тәжікстан', 'таджикистан', 'tajikistan'], '塔吉克'),
    (['түрікменстан', 'туркменистан', 'turkmenistan'], '土库曼'),
]

CHINA_ORIGINAL_TERMS = ['china', 'chinese', 'қытай', 'китай', 'хитой', '中国', '中國']

def manual_translation_override(text):
    lowered = clean_text(text).lower()
    for required_terms, translation in MANUAL_TRANSLATION_RULES:
        if all(term in lowered for term in required_terms):
            return translation
    return ''

def violates_country_translation_sanity(translated, original):
    translated = translated or ''
    original_lowered = (original or '').lower()
    original_mentions_china = any(term in original_lowered for term in CHINA_ORIGINAL_TERMS)
    if '中国' not in translated or original_mentions_china:
        return False
    for original_terms, expected_chinese in COUNTRY_TRANSLATION_EXPECTATIONS:
        if any(term in original_lowered for term in original_terms) and expected_chinese not in translated:
            return True
    return False

def looks_translated(text, original):
    if not text:
        return False
    lowered = text.lower()
    if 'mymemory warning' in lowered or 'available free translations' in lowered:
        return False
    bad_translation_terms = [
        'please contact us', 'contact us for more details',
        '请联系我们', '联系我们了解更多详情',
    ]
    original_lowered = (original or '').lower()
    if any(term in lowered for term in bad_translation_terms) and not any(term in original_lowered for term in bad_translation_terms):
        return False
    if violates_country_translation_sanity(text, original):
        return False
    if 'төраға' in original_lowered and '椅子' in text:
        return False
    if 'әділет' in original_lowered and '正义' in text:
        return False
    if 'контрольный выстрел' in original_lowered and '控制镜头' in text:
        return False
    if text.strip() == (original or '').strip():
        return False
    return bool(re.search(r'[\u4e00-\u9fff]', text))

def translation_budget_exhausted():
    if TRANSLATE_CALLS >= MAX_TRANSLATIONS_PER_RUN:
        return True
    if TRANSLATION_STARTED_AT is None:
        return False
    return (time.monotonic() - TRANSLATION_STARTED_AT) >= MAX_TRANSLATION_SECONDS_PER_RUN

def translate_text(text, target_lang='zh-cn'):
    global TRANSLATE_CALLS, TRANSLATION_STARTED_AT
    text = clean_text(text)
    if not text or len(text.strip()) == 0:
        return text
    manual_translation = manual_translation_override(text)
    if manual_translation:
        return manual_translation
    source_lang = detect_lang(text)
    if source_lang == 'zh':
        return text
    cache_key = source_lang + '|' + text.strip()
    if cache_key in TRANSLATE_CACHE:
        cached_translation = TRANSLATE_CACHE[cache_key]
        if looks_translated(cached_translation, text):
            return cached_translation
        TRANSLATE_CACHE.pop(cache_key, None)
    if TRANSLATION_STARTED_AT is None:
        TRANSLATION_STARTED_AT = time.monotonic()
    if translation_budget_exhausted():
        return text
    translated = ''
    try:
        encoded_text = urllib.parse.quote(text)
        url = 'https://api.mymemory.translated.net/get?q=' + encoded_text + '&langpair=' + source_lang + '|' + target_lang
        req = urllib.request.Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0'})
        TRANSLATE_CALLS += 1
        time.sleep(0.2)
        with urllib.request.urlopen(req, timeout=5) as response:
            data = response.read().decode('utf-8')
            result = jsonmod.loads(data)
            if 'responseData' in result and result['responseData'].get('translatedText'):
                translated = clean_text(result['responseData']['translatedText'])
                if looks_translated(translated, text):
                    TRANSLATE_CACHE[cache_key] = translated
                    return translated
    except Exception:
        pass
    if translation_budget_exhausted():
        return text
    try:
        tl = 'zh-CN'
        params = urllib.parse.urlencode({
            'client': 'gtx',
            'sl': source_lang,
            'tl': tl,
            'dt': 't',
            'q': text,
        })
        url = 'https://translate.googleapis.com/translate_a/single?' + params
        req = urllib.request.Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0'})
        time.sleep(0.1)
        with urllib.request.urlopen(req, timeout=5) as response:
            data = response.read().decode('utf-8')
            result = jsonmod.loads(data)
            if result and result[0]:
                translated = clean_text(''.join(part[0] for part in result[0] if part and part[0]))
                if looks_translated(translated, text):
                    TRANSLATE_CACHE[cache_key] = translated
                    return translated
    except Exception:
        pass
    if translation_budget_exhausted():
        return text
    try:
        source = source_lang if source_lang in {'ru', 'kk', 'uz'} else 'en'
        url = 'https://lingva.ml/api/v1/' + source + '/zh/' + urllib.parse.quote(text)
        req = urllib.request.Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0'})
        time.sleep(0.1)
        with urllib.request.urlopen(req, timeout=5) as response:
            data = response.read().decode('utf-8')
            result = jsonmod.loads(data)
            translated = clean_text(result.get('translation', ''))
            if looks_translated(translated, text):
                TRANSLATE_CACHE[cache_key] = translated
                return translated
    except Exception:
        pass
    return text

# ================================================================
#  一、中亚五国官方媒体 RSS
# ================================================================
FEEDS = {
    # 哈萨克斯坦
    'Kazinform': ['https://www.kazinform.kz/rss.xml'],
    'Informburo.kz': ['https://informburo.kz/rss.xml'],
    'Tengrinews.kz': ['https://tengrinews.kz/rss.php'],
    'Zakon.kz': ['https://www.zakon.kz/rss/feed.html'],
    'Aktualno.kz': ['https://aktualno.kz/rss'],
    'Nur.kz': ['https://nur.kz/rss/feed.json'],
    'Kazpravda': ['https://kazpravda.kz/rss'],
    'Turbina7.kz': ['https://turbina7.kz/rss'],
    'The Astana Times': ['https://astanatimes.com/feed/'],
    'Vlast.kz': ['https://vlast.kz/feed/'],
    'Kursiv Kazakhstan English': ['https://kz.kursiv.media/en/feed/'],
    # 乌兹别克斯坦
    'UzA': ['https://www.uzA.uz/rss'],
    'Gazeta.uz': ['https://gazeta.uz/uz/rss'],
    'Kun.uz': ['https://kun.uz/rss'],
    'Report.uz': ['https://report.uz/rss'],
    'Dunyoxabarlari.uz': ['https://dunyoxabarlari.uz/rss'],
    'XalqSozi': ['https://xalqsozi.uz/rss'],
    'Zonadaily.uz': ['https://zonadaily.uz/rss'],
    'Podrobno.uz': ['https://podrobno.uz/rss/'],
    'Spot.uz': ['https://www.spot.uz/en/rss/'],
    # 吉尔吉斯斯坦
    'Kabar KG': ['https://kabar.kg/rss'],
    '24.kg': ['https://24.kg/rss'],
    'KgNews': ['https://kgnews.kg/rss'],
    'Asiacenter.kg': ['https://asicenter.kg/rss'],
    'Arna.kg': ['https://arna.kg/rss'],
    'Kyrgyzstan Today': ['https://kgtoday.kg/rss'],
    'AKIpress': ['https://akipress.com/rss/en.rss'],
    'Kloop': ['https://kloop.kg/feed/'],
    # 塔吉克斯坦
    'Khovar TJ': ['https://khovar.tj/rss'],
    'Asia-Plus TJ': ['https://en.asia-plus.tj/rss'],
    'Somon.tj': ['https://somon.tj/rss'],
    'Ziyo.net': ['https://ziyo.net/rss'],
    'Nahod.tj': ['https://nahod.tj/rss'],
    'Vazhnoe.tj': ['https://vazhnoe.tj/rss'],
    'Avesta TJ': ['https://avesta.tj/feed/'],
    # 土库曼斯坦
    'TDH TM': ['https://tdh.tm/rss'],
    'Tribune.TM': ['https://tribune.tm/rss'],
    'News-tm': ['https://news-tm.com/rss'],
    'Business Turkmenistan': ['https://business.com.tm/rss'],
    # 区域
    'Trend AZ': ['https://en.trend.az/rss.xml'],
    'Day.az': ['https://day.az/feeds/rss'],
}

# ================================================================
#  二、智库与深度分析
# ================================================================
FEEDS.update({
    'CABAR.asia': ['https://cabar.asia/en/feed'],
    'The Times of Central Asia': ['https://timesca.com/feed/'],
    'Carnegie Endowment Central Asia': ['https://carnegieendowment.org/search?search=&f[0]=program%3A1127'],
    'Eurasianet': [
        'https://eurasianet.org/feed',
        'https://www.eurasianet.org/feed',
    ],
    'Central Asia-Caucasus Analyst': ['https://www.cacianalyst.org/?format=feed&type=rss'],
    'Central Asia New Strategies': ['https://www.caspi.net/feed'],
    'Carnegie Endowment': ['https://carnegieendowment.org/feed?lang=en'],
    'CSIS Central Asia': ['https://www.csis.org/feeds/all.xml'],
    'Atlantic Council Central Asia': ['https://www.atlanticcouncil.org/about/feed/'],
    'Stimson Center Eurasia': ['https://www.stimson.org/rss.xml'],
    'Brookings Russia and Eurasia': ['https://www.brookings.edu/feed/'],
    'Chatham House Russia and Eurasia': ['https://www.chathamhouse.org/about-us/rss'],
    'RUSI Central Asia': ['https://www.rusi.org/rss/latest-publications.xml'],
    'ISW': ['https://www.understandingwar.org/rss'],
    'Lowy Interpreter': ['https://www.lowyinstitute.org/international/the-interpreter/rss'],
    'Eurasia Daily Monitor (Jamestown)': ['https://jamestown.org/feed/'],
    'Central Asia Program (Wilson Center)': ['https://www.wilsoncenter.org/blog-post/feed?taxonomy_term=1161'],
    'Central Asia Foundation': ['https://caf.kz/feed'],
    'Open Society Foundations (Central Asia)': ['https://www.soros.org/initiatives/central-asia/feed'],
    'SAIIA': ['https://www.saiia.org.za/?feed=rss2'],
    'RSIS Singapore': ['https://rsis.edu.sg/?feed=rss2'],
    'International Crisis Group Central Asia': ['https://www.crisisgroup.org/rss.xml'],
    'PONARS Eurasia': ['https://www.ponarseurasia.org/feed/'],
    'NISI Kyrgyzstan': ['https://nisi.gov.kg/en/feed/'],
    'Tajik CSR Analytical Articles': ['https://mts.tj/?cat=73&feed=rss2'],
    'Voices on Central Asia': ['https://voicesoncentralasia.org/feed/'],
    'Novastan English': ['https://novastan.org/en/feed/'],
    # P1 2026-07-18: Doubao-aligned deep/business discovery feeds
    'bne IntelliNews Central Asia': ['https://www.intellinews.com/rss/'],
    'Fergana News English': ['https://en.fergana.news/rss.php', 'https://fergana.agency/rss.php'],
    'Kapital.kz English': ['https://kapital.kz/rss'],
    'Eurasian Research Institute': ['https://www.eurasian-research.org/feed/'],
    'Global Voices Central Asia': ['https://globalvoices.org/-/world/central-asia/feed/'],
    'Caspian Policy Center RSS': ['https://www.caspianpolicy.org/rss.xml'],
    'Oxus Society': ['https://oxussociety.org/feed/'],
    'Central Asia Forum': ['https://www.centralasiaforum.org/feed'],
    'Afghanistan Analysts Network': ['https://www.afghanistan-analysts.org/en/feed/'],
    'ECFR': ['https://ecfr.eu/feed/'],
    'Human Rights Watch': ['https://www.hrw.org/rss'],
    'KAS Central Asia': ['https://www.kas.de/en/web/zentralasien/rss'],
    'GMF': ['https://www.gmfus.org/rss.xml'],
    'The Diplomat Central Asia': ['https://thediplomat.com/tag/central-asia/feed/'],
    'The Diplomat China-Central Asia': ['https://thediplomat.com/tag/china-central-asia/feed/'],
    'Riddle Russia': ['https://ridl.io/feed/'],
    'SWP Berlin': ['https://www.swp-berlin.org/en/publications/rss.xml'],
    'Clingendael': ['https://www.clingendael.org/rss.xml'],
    'EUISS': ['https://www.iss.europa.eu/rss.xml'],
    'OSW Central Asia': ['https://www.osw.waw.pl/en/rss.xml'],
    'EUCAM Policy Briefs RSS': ['https://eucentralasia.eu/category/research-publications/policy-briefs/feed/'],
    # 2026-07-16 deep-source expansion: verified stable RSS for institutes / specialist research outlets.
    'KISI KazISS RSS': ['https://kisi.kz/en/feed/'],
    'Central Asia Program RSS': ['https://centralasiaprogram.org/feed/'],
    'CAPS Unlock RSS': ['https://capsunlock.org/feed/'],
    'ECFR': ['https://ecfr.eu/feed/'],
    'Dialogue Earth': ['https://dialogue.earth/en/feed/'],
    # 2026-07-18 priority deep expansion
    'The Third Pole': ['https://www.thethirdpole.net/en/feed/', 'https://www.thethirdpole.net/feed/'],
    'IWPR Central Asia': ['https://iwpr.net/rss', 'https://iwpr.net/global/rss.xml'],
    'Oxus Society RSS': ['https://oxussociety.org/feed/'],
    'ISRS Uzbekistan': ['https://isrs.uz/en/rss', 'https://isrs.uz/rss'],
    'IISS Online Analysis': ['https://www.iiss.org/online-analysis/rss/', 'https://www.iiss.org/blogs/rss'],
    'The Loop ECPR': ['https://theloop.ecpr.eu/feed/'],
    'E-International Relations': ['https://www.e-ir.info/feed/'],
    'LSE International Development': ['https://blogs.lse.ac.uk/internationaldevelopment/feed/'],
})

# ================================================================
#  三、学术期刊
# ================================================================
FEEDS.update({
    'Central Asian Survey': ['https://www.tandfonline.com/rss/cci20.xml'],
    'Post-Soviet Affairs': ['https://www.tandfonline.com/rss/vpsa20.xml'],
    'Inner Asia': ['https://brill.com/view/journals/innr/innr-overview.xml'],
    'Kritika': ['https://muse.jhu.edu/rss_feed/kritika.xml'],
})

# ================================================================
#  四、国际主流媒体
# ================================================================
FEEDS.update({
    'The Diplomat': ['https://thediplomat.com/feed/'],
    'Foreign Policy': ['https://foreignpolicy.com/feed/'],
    'Foreign Affairs': ['https://www.foreignaffairs.com/rss.xml'],
    'Reuters World': ['https://feeds.reuters.com/reuters/worldNews'],
    'Al Jazeera': ['https://www.aljazeera.com/xml/rss/all.xml'],
    'BBC World': ['https://feeds.bbci.co.uk/news/world/rss.xml'],
    'DW English': ['https://rss.dw.com/xml/rss-all-en'],
    'The Guardian World': ['https://www.theguardian.com/world/rss'],
    'France 24 EN': ['https://www.france24.com/en/rss'],
    'Euronews': ['https://www.euronews.com/rss'],
    'AP News': ['https://rsshub.app/apnews/topics/world-news'],
    'Washington Post World': ['https://www.washingtonpost.com/rss/world/rss.xml'],
    'Le Monde': ['https://www.lemonde.fr/rss/une.xml'],
    'Financial Times World': ['https://www.ft.com/world?format=rss'],
    'Financial Times Asia': ['https://www.ft.com/asia-pacific?format=rss'],
    'The Economist Asia': ['https://www.economist.com/asia/rss.xml'],
    'The Economist Europe': ['https://www.economist.com/europe/rss.xml'],
    'New York Times World': ['https://rss.nytimes.com/services/xml/rss/nyt/World.xml'],
    'Nikkei Asia': ['https://asia.nikkei.com/rss/feed/nar'],
    'War on the Rocks': ['https://warontherocks.com/feed/'],
    'German Marshall Fund': ['https://www.gmfus.org/rss.xml'],
    'NDTV World': ['https://ndtvworld.feedspot.com/rss_fetch.php?rssfeed=ndtv'],
    'Al Arabiya English': ['https://english.alarabiya.net/feed/default.aspx'],
    'Middle East Eye': ['https://www.middleeasteye.org/rss.xml'],
})

# ================================================================
#  五、俄语媒体
# ================================================================
FEEDS.update({
    'Meduza': ['https://meduza.io/rss/en/all'],
    'TASS': ['https://tass.com/rss/v2.xml'],
    'RIA Novosti': ['https://ria.ru/export/rss2/archive/index.xml'],
    'Regnum Agency': ['https://regnum.ru/api/news/feed/country/14'],
    'Kavkaz.Realii': ['https://caucasusrealii.org/feed'],
    'Lenta.ru': ['https://lenta.ru/rss'],
    'Gazeta.ru': ['https://www.gazeta.ru/social/index.xml'],
    'Novaya Gazeta': ['https://novayagazeta.eu/feed/rss'],
    'Interfax': ['https://www.interfax.ru/rss.asp'],
    'RFE/RL Central Asia': ['https://www.rferl.org/api/'],
    'Azattyq (Kazakh)': ['https://www.azattyq.org/api/'],
    'Ozodi (Uzbek/Tajik)': ['https://www.ozodi.org/api/'],
    'Khronika.info': ['https://khronika.info/feed/'],
})

# ================================================================
#  六、中国来源
# ================================================================
FEEDS.update({
    'SCMP Asia': ['https://www.scmp.com/rss/3/feed/'],
    'Caixin Global': ['https://global.caixin.com/rss.xml'],
    'CGTN': ['https://www.cgtn.com/subscribe/rss/section/world.xml'],
    'China Daily': ['https://www.chinadaily.com.cn/rss/world_rss.xml'],
    'China-US Focus': ['https://www.china-usfocus.com/feed/'],
})

# ================================================================
#  网页抓取源
# ================================================================
WEB_SOURCES = {
    'Kazinform': 'https://www.kazinform.kz',
    'Informburo': 'https://informburo.kz',
    'Tengrinews': 'https://tengrinews.kz',
    'Zakon.kz': 'https://www.zakon.kz',
    'The Astana Times': 'https://astanatimes.com',
    'Vlast.kz': 'https://vlast.kz',
    'Orda.kz': 'https://orda.kz',
    'Kapital.kz': 'https://kapital.kz',
    'Kazpravda': 'https://kazpravda.kz/',
    'Nur.kz': 'https://www.nur.kz/',
    'UzA': 'https://www.uzA.uz',
    'Gazeta.uz': 'https://gazeta.uz',
    'Kun.uz': 'https://kun.uz',
    'Report.uz': 'https://report.uz',
    'XalqSozi': 'https://xs.uz/uz',
    'Daryo.uz': 'https://daryo.uz/en',
    'Podrobno.uz': 'https://podrobno.uz',
    'Spot.uz': 'https://www.spot.uz/en',
    'Kabar KG': 'https://kabar.kg',
    '24.kg': 'https://24.kg',
    'AKIpress': 'https://akipress.com',
    'Kloop': 'https://kloop.kg',
    'Khovar TJ': 'https://khovar.tj',
    'Asia-Plus TJ': 'https://en.asia-plus.tj',
    'Avesta TJ': 'https://avesta.tj',
    'TDH TM': 'https://tdh.tm',
    'Turkmenportal': 'https://turkmenportal.com/en',
    'Orient TM': 'https://orient.tm/en',
    'Business Turkmenistan': 'https://business.com.tm',
    'Trend AZ': 'https://en.trend.az',
    'CABAR.asia': 'https://cabar.asia/en/',
    'The Times of Central Asia': 'https://timesca.com/',
    'Eurasianet': 'https://eurasianet.org/region/central-asia',
    'Caspian Policy Center': 'https://www.caspianpolicy.org/',
    'Oxus Society': 'https://oxussociety.org/',
    'IWPR Central Asia': 'https://iwpr.net/global-voices/central-asia',
    'The Third Pole': 'https://www.thethirdpole.net/en/',
    'ISRS Uzbekistan': 'https://isrs.uz/en/',
    'Dialogue Earth Web': 'https://dialogue.earth/en/',
    'Eurasian Development Bank': 'https://eabr.org/en/press/news/',
    'UNRCCA': 'https://unrcca.unmissions.org/en',
    'OSCE News': 'https://news.osce.org/',
    'ADB Central and West Asia': 'https://www.adb.org/news/regions/central-west-asia',
    'World Bank ECA': 'https://www.worldbank.org/en/region/eca/news',
    'International Crisis Group Central Asia': 'https://www.crisisgroup.org/europe-central-asia',
    'Human Rights Watch Central Asia': 'https://www.hrw.org/europe/central-asia',
    'UNDP Eurasia': 'https://www.undp.org/eurasia/news-centre',
    'OSW Central Asia': 'https://www.osw.waw.pl/en/publikacje?f%5B0%5D=obszary%3A399',
}

CANDIDATE_WEB_SOURCES = {
    # 官方与政策源
    'Akorda': 'https://www.akorda.kz/en',
    'Kazakhstan Government': 'https://www.gov.kz/memleket/entities/primeminister?lang=en',
    'Kazakhstan MFA': 'https://www.gov.kz/memleket/entities/mfa?lang=en',
    'National Bank of Kazakhstan': 'https://nationalbank.kz/en',
    'President of Uzbekistan': 'https://president.uz/en',
    'Uzbekistan Government': 'https://gov.uz/en',
    'Central Bank of Uzbekistan': 'https://cbu.uz/en/',
    'Statistics Agency Uzbekistan': 'https://stat.uz/en/',
    'President of Kyrgyzstan': 'http://president.kg/en',
    'Kyrgyz Cabinet': 'https://www.gov.kg/en',
    'Kyrgyz MFA': 'https://mfa.gov.kg/en',
    'National Bank Kyrgyzstan': 'https://www.nbkr.kg/index1.jsp?lang=ENG',
    'Kyrgyz Statistics': 'https://www.stat.gov.kg/en/',
    'Tajik MFA': 'https://mfa.tj/en/main',
    'National Bank Tajikistan': 'https://nbt.tj/en/',
    'Turkmenistan Official': 'https://turkmenistan.gov.tm/en',
    'Turkmenistan MFA': 'https://www.mfa.gov.tm/en',
    # 本地经济媒体与社会观察
    'Forbes Kazakhstan': 'https://forbes.kz/',
    'KISI KazISS Analytics': 'https://kisi.kz/en/category/analytics/',
    'Kaktus.media': 'https://kaktus.media/',
    'Your.tj': 'https://your.tj/',
    # 区域项目与深度分析
    'New Lines Central Asia': 'https://newlinesinstitute.org/?s=Central+Asia',
    'SpecialEurasia Central Asia': 'https://www.specialeurasia.com/?s=Central+Asia',
    'CAREC': 'https://www.carecprogram.org/',
    'IMF Central Asia': 'https://www.imf.org/en/search#q=Central%20Asia',
    'MERICS Central Asia Search': 'https://merics.org/en/search?search=Central%20Asia',
    'IDOS Central Asia Search': 'https://www.idos-research.de/en/search?search=Central%20Asia',
    'Carnegie Search Central Asia': 'https://carnegieendowment.org/regions/russia-eurasia/central-asia',
    'Brookings Search Central Asia': 'https://www.brookings.edu/search/?s=Central%20Asia',
    'CSIS Search Central Asia': 'https://www.csis.org/regions/russia-and-eurasia/central-asia',
    'Atlantic Council Search Central Asia': 'https://www.atlanticcouncil.org/?s=Central+Asia',
    'Stimson Search Central Asia': 'https://www.stimson.org/?s=Central+Asia',
    'Chatham House Search Central Asia': 'https://www.chathamhouse.org/search?search=Central%20Asia',
    'RUSI Search Central Asia': 'https://www.rusi.org/search?search=Central%20Asia',
    'SWP Search Central Asia': 'https://www.swp-berlin.org/en/publications',
    'Clingendael Search Central Asia': 'https://www.clingendael.org/search?keys=Central%20Asia',
    'EUISS Search Central Asia': 'https://www.iss.europa.eu/publications',
    'ECFR Search Central Asia': 'https://ecfr.eu/search/?q=Central%20Asia',
    'ORF Search Central Asia': 'https://www.orfonline.org/search?q=Central%20Asia',
    'Observer Research Foundation Central Asia': 'https://www.orfonline.org/tags/central-asia',
    'Manohar Parrikar IDSA Central Asia': 'https://www.idsa.in/searchresult/central%20asia',
    'Ankasam Central Asia': 'https://www.ankasam.org/?s=Central+Asia',
    'Valdai Search Central Asia': 'https://valdaiclub.com/search/?q=Central%20Asia',
    'RIAC Search Central Asia': 'https://russiancouncil.ru/en/search/index.php?q=Central%20Asia',
    'Russia in Global Affairs Central Asia': 'https://eng.globalaffairs.ru/?s=Central+Asia',
    'ORSAM Central Asia': 'https://orsam.org.tr/en/?s=Central+Asia',
    'Iran Eurasia Studies Central Asia': 'https://www.iras.ir/en/?s=Central+Asia',
    'IPIS Iran Central Asia': 'https://ipis.ir/en',
    'Afghan Institute for Strategic Studies Central Asia': 'https://www.aissonline.org/en?s=Central+Asia',
    'Ifri Central Asia': 'https://www.ifri.org/en/regions/russia-eurasia/central-asia',
    'IISS Search Central Asia': 'https://www.iiss.org/search/?query=Central%20Asia',
    'IISS Strategic Comments Central Asia': 'https://www.iiss.org/search/?query=Central%20Asia%20strategic%20comments',
    'MERICS China Central Asia': 'https://merics.org/en/topics/china-central-asia',
    'Kennan Cable Central Asia': 'https://www.wilsoncenter.org/publication-series/kennan-cable',
    'ISRS Publications': 'https://isrs.uz/en/analytical-materials',
    'IWPR Investigations Central Asia': 'https://iwpr.net/global-voices/central-asia',
    'The Third Pole Central Asia': 'https://www.thethirdpole.net/en/?s=Central+Asia',
}

CITATION_DERIVED_WEB_SOURCES = {
    # 来自高质量报告参考文献网络的候选源：先以候选源运行，必须继续通过深度、时效和中亚强相关门槛。
    'Wilson Center Search Central Asia': 'https://www.wilsoncenter.org/search?search=Central%20Asia',
    'Kennan Institute Search Central Asia': 'https://www.wilsoncenter.org/search?search=Central%20Asia%20Kennan',
    'Davis Center Harvard Central Asia': 'https://daviscenter.fas.harvard.edu/search?search=Central%20Asia',
    'FPRI Search Central Asia': 'https://www.fpri.org/?s=Central+Asia',
    'Foreign Policy Centre Search Central Asia': 'https://fpc.org.uk/?s=Central+Asia',
    'SIPRI Search Central Asia': 'https://www.sipri.org/search?keys=Central%20Asia',
    'RAND Search Central Asia': 'https://www.rand.org/search.html?query=Central%20Asia',
    'China Global South Central Asia': 'https://chinaglobalsouth.com/?s=Central+Asia',
    'CER Search Central Asia': 'https://www.cer.eu/search?search=Central%20Asia',
}

CANDIDATE_WEB_SOURCES.update(CITATION_DERIVED_WEB_SOURCES)

# This is an internal source-perspective registry, not a public digest category.
# It gives authoritative institutions in countries surrounding Central Asia the
# same publication-page treatment already used for major Western institutions.
NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES = {
    # Russia
    'Valdai Search Central Asia', 'RIAC Search Central Asia',
    'Russia in Global Affairs Central Asia',
    # Turkey
    'Ankasam Central Asia', 'ORSAM Central Asia',
    # Iran and Afghanistan
    'Iran Eurasia Studies Central Asia', 'IPIS Iran Central Asia',
    'Afghan Institute for Strategic Studies Central Asia',
    # India
    'ORF Search Central Asia', 'Observer Research Foundation Central Asia',
    'Manohar Parrikar IDSA Central Asia',
}

# Search/archive pages from major research institutions are publication
# discovery channels, not ordinary news pages. Their unread reports and
# analyses remain eligible beyond the 30-day media window.
DURABLE_PRESTIGE_DISCOVERY_SOURCES = {
    'Carnegie Search Central Asia', 'Brookings Search Central Asia',
    'CSIS Search Central Asia', 'Atlantic Council Search Central Asia',
    'Stimson Search Central Asia', 'Chatham House Search Central Asia',
    'RUSI Search Central Asia', 'SWP Search Central Asia',
    'Clingendael Search Central Asia', 'EUISS Search Central Asia',
    'ECFR Search Central Asia', 'ORF Search Central Asia',
    'Observer Research Foundation Central Asia',
    'Manohar Parrikar IDSA Central Asia', 'Ifri Central Asia',
    'IISS Search Central Asia', 'IISS Strategic Comments Central Asia',
    'MERICS Central Asia Search', 'IDOS Central Asia Search',
    'Wilson Center Search Central Asia', 'Kennan Institute Search Central Asia',
    'Davis Center Harvard Central Asia', 'FPRI Search Central Asia',
    'Foreign Policy Centre Search Central Asia', 'SIPRI Search Central Asia',
    'RAND Search Central Asia', 'CER Search Central Asia',
    'KISI KazISS Analytics',
} | NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES

TIER_ONE_PRESTIGE_DISCOVERY_SOURCES = {
    'Carnegie Search Central Asia', 'Brookings Search Central Asia',
    'CSIS Search Central Asia', 'Chatham House Search Central Asia',
    'RUSI Search Central Asia', 'Ifri Central Asia',
    'IISS Search Central Asia', 'IISS Strategic Comments Central Asia',
    'Wilson Center Search Central Asia', 'Kennan Institute Search Central Asia',
    'Davis Center Harvard Central Asia', 'SIPRI Search Central Asia',
    'RAND Search Central Asia',
    'RIAC Search Central Asia', 'Valdai Search Central Asia',
    'IPIS Iran Central Asia',
}

TELEGRAM_SOURCES = {
    # 本地语种与区域深度渠道；通过 Telegram 公开预览页抓取，不需要登录。
    'Gazeta.uz Telegram': 'https://t.me/s/gazetauz',
    'Kun.uz Telegram': 'https://t.me/s/kunuzofficial',
    'Daryo Telegram': 'https://t.me/s/daryo_live',
    'Vlast.kz Telegram': 'https://t.me/s/vlastkz',
    'Orda.kz Telegram': 'https://t.me/s/orda_kz',
    'AKIpress Telegram': 'https://t.me/s/akipress',
    'Kloop Telegram': 'https://t.me/s/kloopnews',
    'Asia-Plus TJ Telegram': 'https://t.me/s/asiaplustj',
    'Your.tj Telegram': 'https://t.me/s/yourtj',
    'Orient TM Telegram': 'https://t.me/s/orienttm',
    'CABAR.asia Telegram': 'https://t.me/s/cabarasia',
    'Fergana Agency Telegram': 'https://t.me/s/fergananews',
    'Novastan Telegram': 'https://t.me/s/novastan',
}

PDF_REPORT_SOURCES = {
    'EDB Reports': 'https://eabr.org/en/analytics/special-reports/',
    'IEA Publications Central Asia': 'https://www.iea.org/search?q=Central%20Asia',
    'EBRD Publications': 'https://www.ebrd.com/publications',
    'World Bank ECA Publications': 'https://www.worldbank.org/en/region/eca/publication',
    'CAREC Publications': 'https://carecprogram.org/publications/?type=publications',
    'CAREC Institute Publications': 'https://www.carecinstitute.org/publications/',
    'ADB Publications': 'https://www.adb.org/publications',
    'OECD Eurasia Publications': 'https://www.oecd.org/eurasia/publications/',
    'UNDP Europe and Central Asia Publications': 'https://www.undp.org/eurasia/publications',
    # Updated official regional publication hubs (the former URLs returned 404).
    'IOM Central Asia Publications': 'https://eca.iom.int/publications',
    'UNODC Central Asia Publications': 'https://www.unodc.org/roca/en/our-work/information-centre-publication.html',
    'Central Asia Program Policy Briefs': 'https://centralasiaprogram.org/publications/central-asia-policy-forum/',
    'OSCE Academy Policy Briefs': 'https://osce-academy.net/research-publications/publications/academy-policy-briefs/',
    'EUCAM Policy Briefs': 'https://eucentralasia.eu/category/research-publications/policy-briefs/',
    'University of Central Asia Publications': 'https://ucentralasia.org/publications',
    'FES Central Asia Publications': 'https://centralasia.fes.de/publications',
    'CAPS Unlock Publications': 'https://capsunlock.org/publications/',
    'ISRS Analytical Materials': 'https://isrs.uz/en/analytical-materials',
    'IISS Publications Central Asia': 'https://www.iiss.org/publications/?query=Central%20Asia',
    'MERICS Reports Central Asia': 'https://merics.org/en/search?search=Central%20Asia',
    'Silk Road Studies Publications': 'https://www.silkroadstudies.org/publications.html',
    'Ifri Papers Central Asia': 'https://www.ifri.org/en/regions/russia-eurasia/central-asia',
    'SIPRI Publications': 'https://www.sipri.org/publications',
    'EUCAM Research Publications': 'https://eucentralasia.eu/category/research-publications/',
    # IAI official RSS is empty; keep publications listing as soft report discovery entry.
    'IAI Publications': 'https://www.iai.it/en/pubblicazioni',
    # Dedicated institution publication adapters. These are not generic news
    # pages; each entry is treated as a report/paper discovery surface.
    'PONARS Eurasia Policy Memos': 'https://www.ponarseurasia.org/publications/',
    'Carnegie Russia-Eurasia Publications': 'https://carnegieendowment.org/russia-eurasia',
    'Brookings Central Asia Research': 'https://www.brookings.edu/topic/central-asia/',
    'CSIS Russia and Eurasia Publications': 'https://www.csis.org/regions/russia-and-eurasia',
    'Chatham House Russia-Eurasia Publications': 'https://www.chathamhouse.org/regions/russia-and-eurasia',
    'RUSI Russia-Eurasia Publications': 'https://www.rusi.org/explore-our-research/regions-and-country-groups/russia-and-eurasia',
    'PISM Central Asia Publications': 'https://www.pism.pl/publications',
    'Wilson Center Central Asia Publications': 'https://www.wilsoncenter.org/search?query=Central%20Asia',
    'Davis Center Central Asia Publications': 'https://daviscenter.fas.harvard.edu/insights',
    'FPRI Eurasia Publications': 'https://www.fpri.org/region/eurasia/',
    # Additional regional, national and university research publishers.
    'KazISS Analytical Publications': 'https://kisi.kz/en/analytics/',
    'IWEP Kazakhstan Publications': 'https://iwep.kz/en/publications',
    'Oxus Society Publications': 'https://oxussociety.org/publications/',
    'Central Asia-Caucasus Institute Publications': 'https://www.silkroadstudies.org/publications.html',
    'Central Asia Forum Publications': 'https://www.centralasiaforum.org/publications',
    'KAS Central Asia Publications': 'https://www.kas.de/en/web/zentralasien',
    'Afghanistan Analysts Network': 'https://www.afghanistan-analysts.org/en/',
    'Human Rights Watch Central Asia': 'https://www.hrw.org/asia/central-asia',
    'ECFR Wider Europe Central Asia': 'https://ecfr.eu/search/?q=Central%20Asia',
    'GMF Central Asia Publications': 'https://www.gmfus.org/search?search_api_fulltext=Central%20Asia',
    'NHC Central Asia Reports': 'https://www.nhc.no/en/central-asia/',
    'UNESCAP Central Asia Publications': 'https://www.unescap.org/publications',
    'FAO Europe Central Asia Publications': 'https://www.fao.org/europe/publications/en/',
    'UNICEF Europe Central Asia Reports': 'https://www.unicef.org/eca/reports',
    'ICG Central Asia Publications': 'https://www.crisisgroup.org/regions/asia/central-asia',
}

# Explicit registry for institution-originated research publications. This
# metadata keeps formal reports separate from generic media discovery and lets
# the renderer explain why a source was included.
INSTITUTION_SOURCE_REGISTRY = {
    'Central Asia Program Policy Briefs': {'institution': 'Central Asia Program', 'kind': 'policy_brief', 'tier': 1},
    'OSCE Academy Policy Briefs': {'institution': 'OSCE Academy', 'kind': 'policy_brief', 'tier': 1},
    'University of Central Asia Publications': {'institution': 'University of Central Asia', 'kind': 'research_publication', 'tier': 1},
    'CAPS Unlock Publications': {'institution': 'CAPS Unlock', 'kind': 'research_publication', 'tier': 1},
    'PONARS Eurasia Policy Memos': {'institution': 'PONARS Eurasia', 'kind': 'policy_memo', 'tier': 1},
    'Carnegie Russia-Eurasia Publications': {'institution': 'Carnegie Endowment', 'kind': 'analysis', 'tier': 1},
    'Brookings Central Asia Research': {'institution': 'Brookings Institution', 'kind': 'analysis', 'tier': 1},
    'CSIS Russia and Eurasia Publications': {'institution': 'CSIS', 'kind': 'analysis', 'tier': 1},
    'Chatham House Russia-Eurasia Publications': {'institution': 'Chatham House', 'kind': 'analysis', 'tier': 1},
    'RUSI Russia-Eurasia Publications': {'institution': 'RUSI', 'kind': 'analysis', 'tier': 1},
    'PISM Central Asia Publications': {'institution': 'Polish Institute of International Affairs', 'kind': 'policy_paper', 'tier': 1},
    'Wilson Center Central Asia Publications': {'institution': 'Wilson Center', 'kind': 'analysis', 'tier': 1},
    'Davis Center Central Asia Publications': {'institution': 'Harvard Davis Center', 'kind': 'research_publication', 'tier': 1},
    'FPRI Eurasia Publications': {'institution': 'Foreign Policy Research Institute', 'kind': 'analysis', 'tier': 1},
    'IISS Publications Central Asia': {'institution': 'IISS', 'kind': 'analysis', 'tier': 1},
    'SIPRI Publications': {'institution': 'SIPRI', 'kind': 'research_publication', 'tier': 1},
    'OSCE Academy Policy Briefs': {'institution': 'OSCE Academy', 'kind': 'policy_brief', 'tier': 1},
    'EUCAM Research Publications': {'institution': 'EUCAM', 'kind': 'research_publication', 'tier': 1},
    'FES Central Asia Publications': {'institution': 'Friedrich Ebert Stiftung', 'kind': 'policy_brief', 'tier': 2},
    'ISRS Analytical Materials': {'institution': 'Institute for Strategic and Regional Studies', 'kind': 'analysis', 'tier': 2},
    'MERICS Reports Central Asia': {'institution': 'MERICS', 'kind': 'analysis', 'tier': 1},
    'Silk Road Studies Publications': {'institution': 'Institute for Security and Development Policy', 'kind': 'research_publication', 'tier': 1},
    'Ifri Papers Central Asia': {'institution': 'French Institute of International Relations', 'kind': 'research_paper', 'tier': 1},
    'IAI Publications': {'institution': 'Istituto Affari Internazionali', 'kind': 'research_publication', 'tier': 1},
    'KazISS Analytical Publications': {'institution': 'Kazakhstan Institute for Strategic Studies', 'kind': 'analysis', 'tier': 1},
    'IWEP Kazakhstan Publications': {'institution': 'Institute of World Economy and Politics', 'kind': 'analysis', 'tier': 1},
    'Oxus Society Publications': {'institution': 'Oxus Society', 'kind': 'research_publication', 'tier': 1},
    'Central Asia-Caucasus Institute Publications': {'institution': 'Central Asia-Caucasus Institute', 'kind': 'research_publication', 'tier': 1},
    'Central Asia Forum Publications': {'institution': 'Central Asia Forum', 'kind': 'research_publication', 'tier': 1},
    'KAS Central Asia Publications': {'institution': 'Konrad Adenauer Stiftung', 'kind': 'analysis', 'tier': 2},
    'Afghanistan Analysts Network': {'institution': 'Afghanistan Analysts Network', 'kind': 'analysis', 'tier': 1},
    'Human Rights Watch Central Asia': {'institution': 'Human Rights Watch', 'kind': 'research_report', 'tier': 1},
    'ECFR Wider Europe Central Asia': {'institution': 'European Council on Foreign Relations', 'kind': 'analysis', 'tier': 1},
    'GMF Central Asia Publications': {'institution': 'German Marshall Fund', 'kind': 'analysis', 'tier': 1},
    'NHC Central Asia Reports': {'institution': 'Norwegian Helsinki Committee', 'kind': 'research_report', 'tier': 1},
    'UNESCAP Central Asia Publications': {'institution': 'UN ESCAP', 'kind': 'research_report', 'tier': 1},
    'FAO Europe Central Asia Publications': {'institution': 'FAO', 'kind': 'research_report', 'tier': 1},
    'UNICEF Europe Central Asia Reports': {'institution': 'UNICEF', 'kind': 'research_report', 'tier': 1},
    'ICG Central Asia Publications': {'institution': 'International Crisis Group', 'kind': 'analysis', 'tier': 1},
    'BTI Central Asia Country Reports': {'institution': 'Bertelsmann Stiftung Transformation Index', 'kind': 'research_report', 'tier': 1},
    'Freedom House Central Asia Country Reports': {'institution': 'Freedom House', 'kind': 'research_report', 'tier': 1},
    'Human Rights Watch Central Asia Country Chapters': {'institution': 'Human Rights Watch', 'kind': 'research_report', 'tier': 1},
}

INSTITUTION_ADAPTER_CONFIG = {
    'Central Asia Program Policy Briefs': {
        'selectors': ['article a[href]', 'a[href*="/publications/"]', 'a[href*="/policy-"]'],
        'allowed_paths': ['/publications/', '/policy-'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'fellowship', 'scholarship', 'training', 'course'],
    },
    'Carnegie Russia-Eurasia Publications': {
        'selectors': ['a[href*="/russia-eurasia/"]'],
        'allowed_paths': ['/russia-eurasia/'],
        'exclude_terms': ['podcast', 'event', 'about'],
        'sitemap_urls': ['https://carnegieendowment.org/sitemap.xml'],
    },
    'Brookings Central Asia Research': {
        'selectors': ['a[href*="/articles/"]', 'a[href*="/research/"]'],
        'allowed_paths': ['/articles/', '/research/'],
        'exclude_terms': ['event', 'webinar', 'podcast'],
        'alternate_urls': ['https://www.brookings.edu/search/?q=Central%20Asia'],
        'sitemap_urls': ['https://www.brookings.edu/sitemap_index.xml'],
    },
    'CSIS Russia and Eurasia Publications': {
        'selectors': ['a[href*="/analysis/"]', 'a[href*="/reports/"]'],
        'allowed_paths': ['/analysis/', '/reports/'],
        'exclude_terms': ['event', 'webinar', 'podcast'],
        'alternate_urls': ['https://www.csis.org/regions/russia-and-eurasia'],
        'sitemap_urls': ['https://www.csis.org/sitemap.xml'],
    },
    'PONARS Eurasia Policy Memos': {
        'selectors': ['a[href*="/memos/"]', 'a[href*="policy-memos"]', 'a[href*="/perspectives/"]'],
        'allowed_paths': ['/memos/', 'policy-memos', '/perspectives/'],
        'exclude_terms': ['podcast', 'event', 'membership'],
        'alternate_urls': ['https://www.ponarseurasia.org/publications/'],
        'sitemap_urls': ['https://www.ponarseurasia.org/sitemap.xml'],
    },
    'Davis Center Central Asia Publications': {
        'selectors': ['a[href*="/insights/"]'],
        'allowed_paths': ['/insights/'],
        'exclude_terms': ['book review', 'journal issue', 'podcast', 'event', 'webinar', 'course', 'memoir'],
        'sitemap_urls': ['https://daviscenter.fas.harvard.edu/sitemap.xml'],
    },
    'FPRI Eurasia Publications': {
        'selectors': ['a[href*="/article/"]', 'a[href*="/analysis/"]'],
        'allowed_paths': ['/article/', '/analysis/'],
        'exclude_terms': ['event', 'webinar', 'podcast'],
    },
    'Wilson Center Central Asia Publications': {
        'selectors': ['a[href*="/publication/"]', 'a[href*="/research/"]', 'a[href*="/blog-post/"]'],
        'allowed_paths': ['/publication/', '/research/', '/blog-post/'],
        'exclude_terms': ['event', 'webinar', 'podcast'],
    },
    'Chatham House Russia-Eurasia Publications': {
        'selectors': ['article a[href]', 'a[href*="/2026/"]', 'a[href*="/2025/"]'],
        'allowed_paths': ['/research-papers/', '/research/', '/expert-comment/', '/2025/', '/2026/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'opinion'],
    },
    'RUSI Russia-Eurasia Publications': {
        'selectors': ['a[href*="/explore-our-research/"]', 'article a[href]'],
        'allowed_paths': ['/research/', '/commentary/', '/analysis/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'membership'],
        'alternate_urls': ['https://www.rusi.org/explore-our-research/publications'],
        'sitemap_urls': ['https://www.rusi.org/sitemap.xml'],
    },
    'PISM Central Asia Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications/"]'],
        'allowed_paths': ['/publications/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter', 'book-review'],
        'alternate_urls': ['https://www.pism.pl/publikacje'],
    },
    'IISS Publications Central Asia': {
        'selectors': ['article a[href]', 'a[href*="/publications/"]'],
        'allowed_paths': ['/publications/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'membership'],
    },
    'SIPRI Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications/"]'],
        'allowed_paths': ['/publications/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'yearbook'],
    },
    'OSCE Academy Policy Briefs': {
        'selectors': ['article a[href]', 'a[href*="/publication"]'],
        'allowed_paths': ['/research-publications/', '/publication'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'training'],
    },
    'EUCAM Research Publications': {
        'selectors': ['article a[href]', 'a[href*="/category/research-publications/"]'],
        'allowed_paths': ['/research-publications/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter'],
    },
    'University of Central Asia Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications/"]'],
        'allowed_paths': ['/publications/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'admission', 'course'],
    },
    'CAPS Unlock Publications': {
        'selectors': ['article a[href]', 'a[href*="/publication"]'],
        'allowed_paths': ['/publication'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'training'],
    },
    'FES Central Asia Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications"]'],
        'allowed_paths': ['/publications'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter'],
    },
    'ISRS Analytical Materials': {
        'selectors': ['article a[href]', 'a[href*="/analytical-materials/"]'],
        'allowed_paths': ['/analytical-materials/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'news'],
    },
    'MERICS Reports Central Asia': {
        'selectors': ['article a[href]', 'a[href*="/en/report/"]', 'a[href*="/en/analysis/"]'],
        'allowed_paths': ['/en/report/', '/en/analysis/', '/en/publication/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter'],
        'alternate_urls': ['https://merics.org/en/search?search=Central%20Asia'],
    },
    'Silk Road Studies Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications/"]'],
        'allowed_paths': ['/publications/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter'],
    },
    'Ifri Papers Central Asia': {
        'selectors': ['article a[href]', 'a[href*="/papers/"]'],
        'allowed_paths': ['/papers/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter'],
    },
    'IAI Publications': {
        'selectors': ['article a[href]', 'a[href*="/pubblicazioni/"]'],
        'allowed_paths': ['/pubblicazioni/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter'],
    },
    'KazISS Analytical Publications': {
        'selectors': ['article a[href]', 'a[href*="/analytics/"]'],
        'allowed_paths': ['/analytics/', '/en/analytics/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'news'],
    },
    'IWEP Kazakhstan Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications"]'],
        'allowed_paths': ['/publications'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'news'],
    },
    'Oxus Society Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications"]'],
        'allowed_paths': ['/publications'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'donate'],
    },
    'Central Asia-Caucasus Institute Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications/silkroad-"]', 'a[href*="/publications/item/"]'],
        'allowed_paths': ['/publications/silkroad-', '/publications/item/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'tag', 'itemlist', 'center', 'centre', 'about'],
    },
    'Central Asia Forum Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications"]'],
        'allowed_paths': ['/publications'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'course'],
    },
    'KAS Central Asia Publications': {
        'selectors': ['article a[href]', 'a[href*="/publikationen"]', 'a[href*="/publication"]'],
        'allowed_paths': ['/publikationen', '/publication'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter'],
    },
    'Afghanistan Analysts Network': {
        'selectors': ['article a[href]', 'a[href*="/reports/"]', 'a[href*="/analysis/"]'],
        'allowed_paths': ['/reports/', '/analysis/', '/en/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'donate'],
    },
    'Human Rights Watch Central Asia': {
        'selectors': ['article a[href]', 'a[href*="/report/"]'],
        'allowed_paths': ['/report/', '/asia/central-asia'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'donate'],
    },
    'ECFR Wider Europe Central Asia': {
        'selectors': ['article a[href]', 'a[href*="/publication/"]'],
        'allowed_paths': ['/publication/', '/article/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter'],
    },
    'GMF Central Asia Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications/"]', 'a[href*="/analysis/"]'],
        'allowed_paths': ['/publications/', '/analysis/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'newsletter'],
    },
    'NHC Central Asia Reports': {
        'selectors': ['article a[href]', 'a[href*="/publications/"]', 'a[href*="/reports/"]'],
        'allowed_paths': ['/publications/', '/reports/', '/central-asia/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'donate'],
    },
    'UNESCAP Central Asia Publications': {
        'selectors': ['article a[href]', 'a[href*="/resources/"]', 'a[href*="/publications/"]'],
        'allowed_paths': ['/resources/', '/publications/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'vacancy'],
    },
    'FAO Europe Central Asia Publications': {
        'selectors': ['article a[href]', 'a[href*="/publications/"]'],
        'allowed_paths': ['/publications/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'vacancy'],
    },
    'UNICEF Europe Central Asia Reports': {
        'selectors': ['article a[href]', 'a[href*="/reports/"]', 'a[href*="/resources/"]'],
        'allowed_paths': ['/reports/', '/resources/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'vacancy'],
    },
    'ICG Central Asia Publications': {
        'selectors': ['article a[href]', 'a[href*="/content/"]', 'a[href*="/crisiswatch/"]'],
        'allowed_paths': ['/content/', '/crisiswatch/'],
        'exclude_terms': ['event', 'webinar', 'podcast', 'donate'],
    },
}

def institution_source_metadata(source_name):
    return INSTITUTION_SOURCE_REGISTRY.get(source_name, {
        'institution': source_name,
        'kind': 'report',
        'tier': 2,
    })

COUNTRY_ASSESSMENT_COUNTRIES = [
    {'country': '哈萨克斯坦', 'name': 'Kazakhstan', 'slug': 'kazakhstan', 'bti_code': 'KAZ'},
    {'country': '乌兹别克斯坦', 'name': 'Uzbekistan', 'slug': 'uzbekistan', 'bti_code': 'UZB'},
    {'country': '吉尔吉斯斯坦', 'name': 'Kyrgyzstan', 'slug': 'kyrgyzstan', 'bti_code': 'KGZ'},
    {'country': '塔吉克斯坦', 'name': 'Tajikistan', 'slug': 'tajikistan', 'bti_code': 'TJK'},
    {'country': '土库曼斯坦', 'name': 'Turkmenistan', 'slug': 'turkmenistan', 'bti_code': 'TKM'},
]

COUNTRY_ASSESSMENT_PROVIDERS = {
    'BTI Central Asia Country Reports': {
        'institution': 'Bertelsmann Stiftung Transformation Index',
        'kind': 'research_report',
        'tier': 1,
    },
    'Freedom House Central Asia Country Reports': {
        'institution': 'Freedom House',
        'kind': 'research_report',
        'tier': 1,
    },
    'Human Rights Watch Central Asia Country Chapters': {
        'institution': 'Human Rights Watch',
        'kind': 'research_report',
        'tier': 1,
        'urllib_fallback': True,
    },
}
COUNTRY_ASSESSMENT_SOURCE_NAMES = set(COUNTRY_ASSESSMENT_PROVIDERS)
DURABLE_PRESTIGE_DISCOVERY_SOURCES.update(COUNTRY_ASSESSMENT_SOURCE_NAMES)
TIER_ONE_PRESTIGE_DISCOVERY_SOURCES.update(COUNTRY_ASSESSMENT_SOURCE_NAMES)

def country_assessment_seed_records():
    """Build stable, first-party country-report entry points for all five states."""
    records = []
    for country in COUNTRY_ASSESSMENT_COUNTRIES:
        records.append({
            **country,
            'source': 'BTI Central Asia Country Reports',
            'url': 'https://bti-project.org/en/reports/country-report/' + country['bti_code'],
            'versioned_stable_url': True,
        })
        for edition_year in [TODAY.year, TODAY.year - 1]:
            records.append({
                **country,
                'source': 'Freedom House Central Asia Country Reports',
                'edition_year': edition_year,
                'url': (
                    'https://freedomhouse.org/country/' + country['slug']
                    + '/freedom-world/' + str(edition_year)
                ),
            })
            records.append({
                **country,
                'source': 'Human Rights Watch Central Asia Country Chapters',
                'edition_year': edition_year,
                'url': (
                    'https://www.hrw.org/world-report/' + str(edition_year)
                    + '/country-chapters/' + country['slug']
                ),
            })
    return records

# Research-information framework. The fields are orthogonal: one item may be
# a top-tier media investigation, a causal-analysis product, and a Kazakhstan-
# focused document at the same time.
RESEARCH_FRAMEWORK_VERSION = '2026-08'
SOURCE_TIER_DEFINITIONS = {
    1: '核心证据源：官方原始材料、顶级机构出版物、同行评议论文、顶级媒体深度报道',
    2: '专业补充源：区域专门机构、可信本地媒体、专题研究网络',
    3: '发现与线索源：聚合页、搜索结果、转载、一般新闻和待核验材料',
}
EVIDENCE_TYPE_DEFINITIONS = {
    'primary_official': '政府、议会、法律、统计和多边机构原始材料',
    'institutional_analysis': '智库、大学中心、研究机构报告与政策简报',
    'academic_paper': '同行评议论文、工作论文和学术专著章节',
    'media_investigation': '国际媒体调查、解释性长文、专题分析和数据报道',
    'local_observation': '中亚本地语种媒体、地方研究机构和社会观察',
    'data_dataset': '统计数据库、指标、调查数据和可复用数据集',
    'meeting_record': '会议纪要、联合声明、机制文件和议程材料',
    'discovery_lead': '用于发现，不直接作为最终证据的搜索或聚合线索',
}
DOCUMENT_FORM_DEFINITIONS = {
    'report': '研究报告/专题报告', 'policy_brief': '政策简报/政策备忘录',
    'working_paper': '工作论文', 'journal_article': '期刊论文',
    'deep_media': '媒体深度报道', 'dataset': '数据集/统计资料',
    'legal_text': '法律法规/判例/议会文件', 'meeting_record': '会议纪要/联合声明',
    'interview': '专家访谈', 'news': '一般新闻',
}
RESEARCH_FUNCTION_DEFINITIONS = {
    'situational_awareness': '事实与动态监测',
    'causal_analysis': '机制解释与因果分析',
    'policy_evaluation': '政策工具、执行与效果评估',
    'data_reference': '数据、指标与基线事实',
    'historical_context': '历史、制度与概念背景',
    'literature_review': '文献综述、理论与方法积累',
}
OUTPUT_CHANNEL_DEFINITIONS = {
    'daily_deep_digest': '日报深度阅读',
    'institution_library': '机构报告库',
    'academic_bibliography': '学术书目库',
    'data_watch': '数据与指标追踪',
    'primary_document_watch': '法律/政策原文追踪',
    'discovery_queue': '待核验发现队列',
}


REPORT_API_SOURCE_NAMES = {'World Bank Documents & Reports'}
WORLD_BANK_REPORT_QUERIES = [
    'Central Asia', 'Kazakhstan', 'Uzbekistan',
    'Kyrgyz Republic', 'Tajikistan', 'Turkmenistan',
    'Middle Corridor', 'Caspian', 'Aral Sea',
    'critical minerals Central Asia',
]
WORLD_BANK_ALLOWED_DOCUMENT_TYPES = {
    'brief', 'report', 'publication', 'working paper',
    'policy research working paper', 'research working paper',
    'economic & sector work', 'technical paper', 'sector report',
    'country economic memorandum', 'public expenditure review',
    'systematic country diagnostic',
}
WORLD_BANK_EXCLUDED_TITLE_TERMS = [
    'audited financial statement', 'audit report', 'procurement plan',
    'implementation status', 'disbursement', 'record of approval',
    'minutes', 'project information document', 'resettlement plan',
    'environmental and social commitment plan',
]

MEETING_MINUTES_SOURCES = {
    'SCO News': 'https://eng.sectsco.org/news/',
    'CICA Press Releases': 'https://www.s-cica.org/index.php?view=page&t=press_releases',
    'CAREC Events': 'https://carecprogram.org/?page_id=36',
    'UNRCCA Press Releases': 'https://unrcca.unmissions.org/en/press-releases',
}

ACADEMIC_QUERIES = [
    'Central Asia political economy governance state capacity',
    'Central Asia military security border Afghanistan',
    'Central Asia Russia China foreign policy relations',
    'Central Asia water climate energy transition',
    'Middle Corridor Caspian transport logistics',
    'Central Asia labor migration remittances society',
    'Kazakhstan Uzbekistan reform institutions political economy',
    'Kyrgyzstan Tajikistan Turkmenistan politics economy governance',
]

# Crossref verifies and completes metadata for a compact set of the most
# Central-Asia-specific journals. OpenAlex handles the broader thematic search.
CROSSREF_DAILY_JOURNAL_KEYS = {
    'centralasiansurvey',
    'centralasianaffairs',
    'journalofeurasianstudies',
    'eurasiangeographyandeconomics',
    'postsovietaffairs',
    'europeasiastudies',
    'innerasia',
    'nationalitiespapers',
}
ACADEMIC_DAILY_TASK_COUNT = len(ACADEMIC_QUERIES) + 2

# Used only when the normal daily edition would otherwise be empty. These are
# not a rolling recap: they search for durable, previously unseen scholarship.
DURABLE_ACADEMIC_BACKFILL_QUERIES = [
    'Central Asia political economy state capacity',
    'Central Asia governance constitutional reform',
    'Central Asia military security border governance',
    'Central Asia Russia China foreign policy',
    'Kazakhstan Uzbekistan economic reform institutions',
    'Central Asia labor migration social policy',
]

DEEP_DISCOVERY_QUERIES = [
    'Central Asia analysis report think tank',
    'Central Asia country assessment country profile political landscape',
    'Kazakhstan Uzbekistan Kyrgyzstan Tajikistan Turkmenistan country report governance society',
    'Central Asia human rights assessment media landscape state of democracy',
    'site:fpc.org.uk Kazakhstan Kyrgyzstan Uzbekistan Tajikistan Turkmenistan introduction rights governance',
    'site:bti-project.org/en/reports/country-report Central Asia',
    'site:freedomhouse.org/country Central Asia freedom world country report',
    'site:hrw.org/world-report country chapters Central Asia',
    'Central Asia political economy state capacity analysis report',
    'Central Asia governance authoritarianism elite politics analysis',
    'Kazakhstan Uzbekistan constitutional reform political analysis',
    'Central Asia public finance banking inflation debt analysis',
    'Central Asia industrial policy state-owned enterprises analysis',
    'Central Asia military defense armed forces analysis report',
    'Central Asia security sector reform border security analysis',
    'Central Asia military procurement defense policy analysis',
    'Central Asia Afghanistan border security report',
    'Kazakhstan Uzbekistan Russia China security analysis think tank',
    'site:iiss.org Central Asia military defense security',
    'site:rusi.org Central Asia security defense analysis',
    'site:sipri.org Central Asia arms military security',
    'site:carnegieendowment.org Central Asia political economy governance',
    'site:chathamhouse.org Central Asia political economy security',
    'site:csis.org Central Asia security political economy',
    'Central Asia policy brief infrastructure energy water',
    'Kazakhstan Uzbekistan Kyrgyzstan Tajikistan Turkmenistan analysis',
    'Middle Corridor Central Asia analysis report',
    'Central Asia critical minerals energy transition analysis',
    'Central Asia water climate governance report',
    'China Central Asia influence analysis think tank',
    'Russia Central Asia relations analysis report',
    'Central Asia labor migration remittances analysis',
    'Afghanistan Central Asia connectivity analysis',
    'Central Asia policy brief think tank',
    'Central Asia working paper governance migration water',
    'Central Asia special report critical minerals water transport',
    'C5+1 Central Asia analysis policy brief',
    'EU Central Asia strategy analysis policy brief',
    'China-Central Asia infrastructure policy brief',
    'Russia Central Asia sanctions migration remittances analysis',
    'Kazakhstan critical minerals analysis report',
    'Kazakhstan political economy analysis report',
    'Uzbekistan economic reform analysis report',
    'Uzbekistan water governance policy brief',
    'Kyrgyzstan remittances migration analysis report',
    'Tajikistan hydropower water security analysis',
    'Turkmenistan gas exports analysis',
    'site:fpri.org Central Asia analysis',
    'site:centralasiaprogram.org Central Asia policy brief',
    'site:osce-academy.net Central Asia policy brief',
    'site:eucentralasia.eu Central Asia policy brief',
    'site:ucentralasia.org Central Asia report',
    'site:carecinstitute.org Central Asia policy brief',
    'site:silkroadstudies.org Central Asia report',
    'site:intellinews.com Central Asia analysis',
    'site:iwpr.net Central Asia analysis',
    'site:theconversation.com Central Asia Kazakhstan Uzbekistan analysis',
    'site:ifri.org Central Asia policy brief',
    'site:carnegieendowment.org/regions/russia-eurasia Central Asia commentary analysis',
    'site:csis.org/analysis Central Asia',
    'site:osw.waw.pl/en/publikacje Central Asia analysis',
    'site:ponarseurasia.org Central Asia policy memo',
    'site:jamestown.org Central Asia analysis',
    'site:eurasianet.org Central Asia analysis',
    'site:thediplomat.com Central Asia analysis',
    'site:cabar.asia Central Asia analysis',
    'site:timesca.com Central Asia analysis',
    'site:newlinesinstitute.org Central Asia analysis',
    'site:kisi.kz Central Asia analysis',
    'site:kisi.kz Kazakhstan strategic analysis report',
    'site:capsunlock.org Central Asia analysis',
    'site:eabr.org Central Asia report',
    'site:isrs.uz/en Uzbekistan Central Asia analysis report',
    'site:nisi.gov.kg/en Kyrgyzstan analysis report',
    'site:mts.tj Tajikistan analytical report',
    'site:centralasia.fes.de Central Asia publication',
    'site:wilsoncenter.org Central Asia analysis',
    'site:daviscenter.fas.harvard.edu Central Asia analysis',
    'site:fpc.org.uk Central Asia analysis',
    'site:sipri.org Central Asia analysis',
    'site:rand.org Central Asia report',
    'site:chinaglobalsouth.com Central Asia China analysis',
    'site:cer.eu Central Asia analysis',
    'site:pism.pl Central Asia analysis',
    'site:ispionline.it Central Asia analysis',
    'site:iai.it Central Asia analysis',
    'site:osw.waw.pl Central Asia analysis report',
    'site:eucentralasia.eu Central Asia policy brief',
    'site:swp-berlin.org Central Asia analysis',
    'site:clingendael.org Central Asia report',
    'site:iss.europa.eu Central Asia policy',
    'site:crisisgroup.org Central Asia report',
    # 2026-08-09 mainstream / major-institute discovery: these publishers
    # frequently expose Central Asia through broader Eurasia, Caspian,
    # transport, energy, or China/Russia framing rather than a CA section.
    'site:reuters.com Central Asia analysis report',
    'site:reuters.com Middle Corridor Caspian Kazakhstan Uzbekistan',
    'site:ft.com Central Asia analysis report',
    'site:ft.com Middle Corridor Caspian Central Asia',
    'site:economist.com Central Asia Kazakhstan Uzbekistan analysis',
    'site:nytimes.com Central Asia Kazakhstan Uzbekistan analysis',
    'site:brookings.edu Central Asia analysis report',
    'site:chathamhouse.org Central Asia report analysis',
    'site:crisisgroup.org Europe Central Asia analysis',
    'site:csis.org Central Asia Middle Corridor analysis',
    'site:foreignpolicy.com Central Asia analysis',
    'site:foreignaffairs.com Central Asia Eurasia analysis',
    # Google News RSS often ignores site: constraints. Keep publisher names
    # in the query as a second discovery path, then rely on publisher metadata
    # plus the normal deep/public gates to reject unrelated news.
    'Reuters Central Asia',
    'Reuters Kazakhstan Uzbekistan',
    'Financial Times Central Asia',
    'Financial Times Kazakhstan Uzbekistan',
    'The Economist Central Asia Kazakhstan Uzbekistan',
    'The New York Times Central Asia Kazakhstan Uzbekistan',
    'Brookings Central Asia',
    'Chatham House Central Asia',
    'Crisis Group Central Asia',
    'Carnegie Central Asia',
    'CSIS Central Asia',
    'ADB Central Asia report',
    'OECD Eurasia Central Asia publication',
    'IOM Central Asia report migration',
    'UNODC Central Asia report governance trafficking',
    'site:cacianalyst.org Central Asia analysis',
    'site:ifri.org Central Asia paper',
    # P1 2026-07-18: Doubao gap sources + expert authors (discovery only; still depth-gated)
    'site:intellinews.com Central Asia OR Kazakhstan OR Uzbekistan analysis',
    'site:iea.org Central Asia energy OR Kazakhstan OR Uzbekistan',
    'site:frontiersin.org Central Asia political OR geopolitics',
    'Bloomberg Central Asia analysis Kazakhstan Uzbekistan',
    'Anadolu Agency Central Asia analysis Kazakhstan Uzbekistan',
    'Alexander Cooley Central Asia analysis OR author',
    'Nargis Kassenova Central Asia analysis OR Kazakhstan',
    'Joanna Lillis Kazakhstan analysis Central Asia',
    'David Trilling Central Asia analysis Eurasianet',
    'Temur Umarov Central Asia Carnegie analysis',
    'site:carnegieendowment.org Temur Umarov Central Asia',
    'The Central Asia Brief analysis',
    'Central Asia Monitor analysis report',
    # Priority deep expansion 2026-07-18
    'site:iwpr.net Central Asia investigation OR analysis',
    'site:thethirdpole.net Central Asia OR Aral OR Amu Darya OR Syr Darya',
    'site:dialogue.earth Central Asia water OR climate OR energy',
    'site:isrs.uz Central Asia OR Uzbekistan analysis',
    'site:oxussociety.org Central Asia analysis',
    'site:capsunlock.org Central Asia analysis report',
    'site:iiss.org Central Asia strategic OR security analysis',
    'site:merics.org Central Asia China OR Belt and Road',
    'site:wilsoncenter.org Kennan Cable Central Asia',
    'Erica Marat Central Asia analysis OR security',
    'Marlene Laruelle Central Asia analysis OR ideology',
    'Sebastien Peyrouse Central Asia analysis',
    'Svante Cornell Central Asia analysis OR Caucasus',
    # Dedicated institution publication queries; these complement direct
    # publication-page adapters and catch items whose title omits "Central Asia".
    'PONARS Eurasia Central Asia policy memo',
    'Carnegie Russia Eurasia Central Asia publication',
    'Brookings Central Asia report',
    'CSIS Russia Eurasia Central Asia report',
    'Chatham House Russia Eurasia Central Asia paper',
    'RUSI Russia Eurasia Central Asia analysis',
    'PISM Central Asia policy paper',
    'Wilson Center Kennan Central Asia paper',
    'Davis Center Harvard Central Asia publication',
    'FPRI Eurasia Central Asia policy brief',
]

NEIGHBOR_DEEP_DISCOVERY_GROUPS = [
    {
        'source': 'Deep Discovery: Google News RU Neighbors',
        'hl': 'ru', 'gl': 'RU', 'ceid': 'RU:ru', 'lookback_days': 365,
        'queries': [
            '"Центральная Азия" анализ исследование доклад',
            'Казахстан Узбекистан Кыргызстан Таджикистан Туркменистан анализ',
            'site:russiancouncil.ru "Центральная Азия"',
            'site:valdaiclub.com "Центральная Азия"',
            'site:imemo.ru "Центральная Азия"',
            'site:globalaffairs.ru "Центральная Азия"',
        ],
    },
    {
        'source': 'Deep Discovery: Google News TR Neighbors',
        'hl': 'tr', 'gl': 'TR', 'ceid': 'TR:tr', 'lookback_days': 365,
        'queries': [
            '"Orta Asya" analiz rapor araştırma',
            'Kazakistan Özbekistan Kırgızistan Tacikistan Türkmenistan analiz',
            'site:ankasam.org "Orta Asya"',
            'site:orsam.org.tr "Orta Asya"',
            'site:avim.org.tr "Orta Asya"',
            'site:tepav.org.tr "Orta Asya"',
        ],
    },
    {
        'source': 'Deep Discovery: Google News FA Neighbors',
        'hl': 'fa', 'gl': 'IR', 'ceid': 'IR:fa', 'lookback_days': 365,
        'queries': [
            '"آسیای مرکزی" تحلیل گزارش پژوهش',
            'قزاقستان ازبکستان قرقیزستان تاجیکستان ترکمنستان تحلیل',
            'site:ipis.ir "آسیای مرکزی"',
            'site:iras.ir "آسیای مرکزی"',
        ],
    },
    {
        'source': 'Deep Discovery: Google News South Caspian Neighbors',
        'hl': 'en-IN', 'gl': 'IN', 'ceid': 'IN:en', 'lookback_days': 365,
        'queries': [
            'site:aissonline.org Central Asia analysis report',
            'site:areu.org.af Central Asia research report',
            'site:issi.org.pk Central Asia analysis',
            'site:ipripak.org Central Asia analysis',
            'site:aircenter.az Central Asia analysis',
            'site:idd.az Central Asia analysis',
            'site:icwa.in Central Asia issue brief',
            'site:vifindia.org Central Asia analysis',
            'site:idsa.in Central Asia issue brief',
            'site:orfonline.org Central Asia analysis',
            'site:aa.com.tr/en/analysis Central Asia',
            'site:tehrantimes.com Central Asia analysis',
            'site:dawn.com Central Asia analysis',
            'site:tolonews.com Central Asia analysis',
            'site:trend.az Central Asia analysis',
        ],
    },
]

DEEP_DISCOVERY_SOURCES = {
    'Deep Discovery: Google News': [
        'https://news.google.com/rss/search?q=' + urllib.parse.quote(query + ' when:30d') + '&hl=en-US&gl=US&ceid=US:en'
        for query in DEEP_DISCOVERY_QUERIES
    ]
}
for group in NEIGHBOR_DEEP_DISCOVERY_GROUPS:
    DEEP_DISCOVERY_SOURCES[group['source']] = [
        'https://news.google.com/rss/search?q='
        + urllib.parse.quote(query + ' when:' + str(group['lookback_days']) + 'd')
        + '&hl=' + group['hl'] + '&gl=' + group['gl'] + '&ceid=' + group['ceid']
        for query in group['queries']
    ]
DEEP_DISCOVERY_TOTAL_TASKS = sum(len(urls) for urls in DEEP_DISCOVERY_SOURCES.values())

DEEP_DISCOVERY_TRUSTED_PUBLISHERS = [
    'ifri', 'csis', 'center for strategic and international studies',
    'world economic forum', 'eurasia review', 'the diplomat',
    'fpri', 'foreign policy research institute', 'bne', 'intellinews', 'bne intellinews',
    'fergana', 'fergana news', 'iea', 'international energy agency', 'frontiers',
    'bloomberg', 'anadolu', 'anadolu agency',
    'iwpr', 'institute for war and peace reporting',
    'the third pole', 'third pole', 'dialogue earth',
    'oxus society', 'oxus', 'iiss', 'merics', 'kennan cable',
    'foundation for defense of democracies', 'carnegie', 'brookings',
    'chatham house', 'rusi', 'atlantic council', 'stimson',
    'observer research foundation', 'orfonline', 'east asia forum',
    'jamestown', 'central asia-caucasus analyst', 'cabar', 'eurasianet',
    'central asia program', 'osce academy', 'eucam',
    'university of central asia', 'carec institute',
    'kaziss', 'kisi', 'kazakhstan institute for strategic studies',
    'friedrich ebert', 'central asian policy studies', 'caps unlock',
    'silk road studies', 'central asia-caucasus institute',
    'wilson center', 'kennan institute', 'davis center', 'harvard',
    'foreign policy research institute', 'foreign policy centre',
    'sipri', 'stockholm international peace research institute',
    'rand', 'china global south', 'centre for european reform',
    'pism', 'polish institute of international affairs',
    'ispi', 'istituto affari internazionali', 'iai',
    'the times of central asia', 'the astana times',
    'new lines institute', 'the conversation',
    'international crisis group', 'wilson center', 'ponars', 'osw',
    'institute for strategic and regional studies', 'isrs',
    'national institute for strategic initiatives', 'nisi',
    'center for strategic research', 'mts.tj',
    'swp', 'clingendael', 'euiss', 'ecfr', 'merics', 'idos',
    'world bank', 'adb', 'asian development bank', 'oecd', 'undp',
    'reuters', 'financial times', 'the economist', 'new york times',
    'unicef', 'iom', 'unodc', 'rfe/rl', 'radio free europe',
    # Authoritative neighboring-country institutions and strict-gated media.
    'russian international affairs council', 'russiancouncil.ru',
    'valdai discussion club', 'valdaiclub.com', 'imemo',
    'russia in global affairs', 'globalaffairs.ru',
    'ankara crisis and policy research center', 'ankasam',
    'center for middle eastern studies', 'orsam',
    'center for eurasian studies', 'avim', 'tepav',
    'institute for political and international studies', 'ipis.ir',
    'institute for iran and eurasia studies', 'iras.ir',
    'afghan institute for strategic studies', 'aissonline.org',
    'afghanistan research and evaluation unit', 'areu.org.af',
    'institute of strategic studies islamabad', 'issi.org.pk',
    'islamabad policy research institute', 'ipripak.org',
    'center of analysis of international relations', 'aircenter.az',
    'institute for development and diplomacy', 'idd.az',
    'indian council of world affairs', 'icwa.in',
    'vivekananda international foundation', 'vifindia.org',
    'manohar parrikar institute', 'idsa.in', 'gateway house',
    'anadolu agency', 'tehran times', 'islamic republic news agency',
    'dawn', 'tolonews', 'trend news agency',
]
DEEP_DISCOVERY_TRUSTED_LOWER = [term.lower() for term in DEEP_DISCOVERY_TRUSTED_PUBLISHERS]

NEIGHBOR_INSTITUTION_PUBLISHER_GROUPS = {
    '俄罗斯': [
        'russian international affairs council', 'russiancouncil.ru',
        'valdai discussion club', 'valdaiclub.com', 'imemo',
        'russia in global affairs', 'globalaffairs.ru',
    ],
    '土耳其': [
        'ankara crisis and policy research center', 'ankasam',
        'center for middle eastern studies', 'orsam',
        'center for eurasian studies', 'avim', 'tepav',
    ],
    '伊朗': [
        'institute for political and international studies', 'ipis.ir',
        'institute for iran and eurasia studies', 'iras.ir',
    ],
    '阿富汗': [
        'afghan institute for strategic studies', 'aissonline.org',
        'afghanistan research and evaluation unit', 'areu.org.af',
    ],
    '巴基斯坦': [
        'institute of strategic studies islamabad', 'issi.org.pk',
        'islamabad policy research institute', 'ipripak.org',
    ],
    '南高加索': [
        'center of analysis of international relations', 'aircenter.az',
        'institute for development and diplomacy', 'idd.az',
    ],
    '印度': [
        'indian council of world affairs', 'icwa.in',
        'vivekananda international foundation', 'vifindia.org',
        'manohar parrikar institute', 'idsa.in',
        'observer research foundation', 'orfonline', 'gateway house',
    ],
}

def neighboring_perspective_region(publisher_text):
    surface = clean_text(publisher_text).lower()
    for region, terms in NEIGHBOR_INSTITUTION_PUBLISHER_GROUPS.items():
        if any(term in surface for term in terms):
            return region
    return ''

def is_neighbor_institution_publisher_text(publisher_text):
    return bool(neighboring_perspective_region(publisher_text))

NEIGHBOR_TIER_ONE_PUBLISHER_TERMS = [
    'russian international affairs council', 'russiancouncil.ru',
    'valdai discussion club', 'valdaiclub.com', 'imemo',
    'institute for political and international studies', 'ipis.ir',
    'afghan institute for strategic studies', 'aissonline.org',
    'afghanistan research and evaluation unit', 'areu.org.af',
    'institute of strategic studies islamabad', 'issi.org.pk',
    'center of analysis of international relations', 'aircenter.az',
    'indian council of world affairs', 'icwa.in',
    'manohar parrikar institute', 'idsa.in',
    'observer research foundation', 'orfonline',
]

def neighbor_institution_tier(publisher_text):
    surface = clean_text(publisher_text).lower()
    return 1 if any(term in surface for term in NEIGHBOR_TIER_ONE_PUBLISHER_TERMS) else 2

# Publisher names alone are insufficient when Google News ignores site filters.
# Frontiers remains discoverable, but receives lower priority and stricter topic
# alignment so unrelated medicine/education papers do not pass through.
DISCOVERY_LOW_PRIORITY_PUBLISHERS = {'frontiers', 'frontiersin.org'}
FRONTIERS_RELEVANCE_TERMS = {
    'politic', 'geopolit', 'governance', 'diplomac', 'migration', 'security',
    'institution', 'policy', 'international relation', 'conflict',
    'regional cooperation', 'state', 'regime', 'reform', 'central asia',
    'kazakhstan', 'uzbekistan', 'kyrgyzstan', 'tajikistan', 'turkmenistan',
}
FRONTIERS_COUNTRY_TERMS = {
    'central asia', 'kazakhstan', 'uzbekistan', 'kyrgyzstan',
    'tajikistan', 'turkmenistan',
}

DEEP_DISCOVERY_EXCLUDED_PUBLISHERS = [
    'travel and tour world', 'nomad lawyer', 'facebook', 'kabulnow',
    'tourism', 'aviation', 'airline', 'airport', 'pr newswire',
    'globe newswire', 'business wire', 'ein presswire', 'eventbrite',
    'linkedin', 'instagram', 'youtube',
]
DEEP_DISCOVERY_EXCLUDED_LOWER = [term.lower() for term in DEEP_DISCOVERY_EXCLUDED_PUBLISHERS]

ACADEMIC_SOURCE_NAMES = {'Academic: Crossref', 'Academic: OpenAlex'}

ACADEMIC_REGIONAL_JOURNAL_WHITELIST = {
    'central asian survey', 'central asian affairs', 'post-soviet affairs',
    'europe-asia studies', 'eurasian geography and economics',
    'problems of post-communism', 'communist and post-communist studies',
    'journal of eurasian studies', 'inner asia', 'nationalities papers',
}

ACADEMIC_TOPICAL_JOURNAL_WHITELIST = {
    'Frontiers in Political Science',
    'asian survey', 'geopolitics', 'political geography', 'ethnopolitics',
    'european security', 'third world quarterly', 'international migration',
    'journal of ethnic and migration studies',
    'international journal of water resources development', 'water international',
    'climate and development', 'environmental science & policy',
    'world development', 'energy policy', 'resources policy', 'transport policy',
    # modest expansion: still quality journals, still require strong CA anchor
    'asian affairs', 'journal of contemporary asia', 'the china quarterly',
}

ACADEMIC_JOURNAL_WHITELIST = (
    ACADEMIC_REGIONAL_JOURNAL_WHITELIST | ACADEMIC_TOPICAL_JOURNAL_WHITELIST
)
ACADEMIC_QUALITY_TERMS = sorted(ACADEMIC_JOURNAL_WHITELIST)
ACADEMIC_QUALITY_LOWER = ACADEMIC_QUALITY_TERMS
ACADEMIC_MIN_ABSTRACT_CHARS = 180
ACADEMIC_TOPICAL_MIN_ABSTRACT_CHARS = 260
ACADEMIC_MIN_TITLE_CHARS = 18
ACADEMIC_REGIONAL_JOURNAL_KEYS = {
    normalize_title_key(name) for name in ACADEMIC_REGIONAL_JOURNAL_WHITELIST
}
ACADEMIC_TOPICAL_JOURNAL_KEYS = {
    normalize_title_key(name) for name in ACADEMIC_TOPICAL_JOURNAL_WHITELIST
}
ACADEMIC_JOURNAL_KEYS = ACADEMIC_REGIONAL_JOURNAL_KEYS | ACADEMIC_TOPICAL_JOURNAL_KEYS

# OpenAlex source IDs for whitelist journals (resolved 2026-07-24). Used for targeted pulls.
OPENALEX_ACADEMIC_SOURCE_IDS = {
    'centralasiansurvey': 'S87053992',
    'postsovietaffairs': 'S79712050',
    'europeasiastudies': 'S115434995',
    'eurasiangeographyandeconomics': 'S32191110',
    'problemsofpostcommunism': 'S8524894',
    'communistandpostcommuniststudies': 'S200446109',
    'journalofeurasianstudies': 'S198504937',
    'innerasia': 'S4210235700',
    'nationalitiespapers': 'S4210178631',
    'centralasianaffairs': 'S4210187618',
    'asiansurvey': 'S194011547',
    'geopolitics': 'S20737860',
    'politicalgeography': 'S202534398',
    'ethnopolitics': 'S44688107',
    'europeansecurity': 'S29557850',
    'thirdworldquarterly': 'S64122990',
    'journalofethnicandmigrationstudies': 'S149872823',
    'waterinternational': 'S201436551',
    'climateanddevelopment': 'S43379938',
    'environmentalscienceandpolicy': 'S39803240',
    'environmentalsciencepolicy': 'S39803240',
    'worlddevelopment': 'S85457386',
    'energypolicy': 'S175056054',
    'resourcespolicy': 'S194185345',
    'transportpolicy': 'S186558940',
    'frontiersinpoliticalscience': 'S4210231853',
    'internationaljournalofwaterresourcesdevelopment': 'S164882405',
    'asianaffairs': 'S162717021',
    'journalofcontemporaryasia': 'S182993579',
    'thechinaquarterly': 'S12189451',
}
ACADEMIC_JOURNAL_ISSN_L = {
    'centralasiansurvey': '0263-4937',
    'postsovietaffairs': '1060-586X',
    'europeasiastudies': '0966-8136',
    'eurasiangeographyandeconomics': '1538-7216',
    'problemsofpostcommunism': '1075-8216',
    'communistandpostcommuniststudies': '0967-067X',
    'journalofeurasianstudies': '1879-3665',
    'innerasia': '1464-8172',
    'nationalitiespapers': '0090-5992',
    'centralasianaffairs': '2214-2282',
    'asiansurvey': '0004-4687',
    'geopolitics': '1465-0045',
    'politicalgeography': '0962-6298',
    'ethnopolitics': '1744-9057',
    'europeansecurity': '0966-2839',
    'thirdworldquarterly': '0143-6597',
    'journalofethnicandmigrationstudies': '1369-183X',
    'waterinternational': '0250-8060',
    'climateanddevelopment': '1756-5529',
    'environmentalscienceandpolicy': '1462-9011',
    'environmentalsciencepolicy': '1462-9011',
    'worlddevelopment': '0305-750X',
    'energypolicy': '0301-4215',
    'resourcespolicy': '0301-4207',
    'transportpolicy': '0967-070X',
    'frontiersinpoliticalscience': '2673-3145',
    'internationaljournalofwaterresourcesdevelopment': '0790-0627',
    'asianaffairs': '0306-8374',
    'journalofcontemporaryasia': '0047-2336',
    'thechinaquarterly': '0009-4439',
}
OPENALEX_REGIONAL_SOURCE_IDS = sorted(set(
    sid for key, sid in OPENALEX_ACADEMIC_SOURCE_IDS.items()
    if key in ACADEMIC_REGIONAL_JOURNAL_KEYS
    or any(token in key for token in [
        'centralasiansurvey', 'centralasianaffairs', 'postsovietaffairs',
        'europeasiastudies', 'eurasiangeographyandeconomics',
        'problemsofpostcommunism', 'communistandpostcommuniststudies',
        'journalofeurasianstudies', 'innerasia', 'nationalitiespapers',
    ])
))
OPENALEX_TOPICAL_SOURCE_IDS = sorted(set(OPENALEX_ACADEMIC_SOURCE_IDS.values()) - set(OPENALEX_REGIONAL_SOURCE_IDS))
ACADEMIC_TOPIC_SEARCH_TERMS = [
    'Central Asia', 'Kazakhstan', 'Uzbekistan', 'Kyrgyzstan', 'Tajikistan', 'Turkmenistan',
    'Middle Corridor', 'Caspian', 'Aral',
]
ACADEMIC_FETCH_DIAG = {
    'api_results': 0,
    'pass': 0,
    'venue_not_whitelist': 0,
    'no_doi': 0,
    'no_authors': 0,
    'title_short': 0,
    'excluded_title': 0,
    'abstract_short': 0,
    'no_ca_anchor': 0,
    'no_date': 0,
    'errors': 0,
    'api_429_retry': 0,
    'api_success_after_429': 0,
    'api_final_429': 0,
}

def reset_academic_fetch_diag():
    for key in list(ACADEMIC_FETCH_DIAG.keys()):
        ACADEMIC_FETCH_DIAG[key] = 0

def note_academic_diag(reason):
    if reason in ACADEMIC_FETCH_DIAG:
        ACADEMIC_FETCH_DIAG[reason] += 1
ACADEMIC_EXCLUDED_TITLE_TERMS = [
    'book review', 'editorial', 'introduction to the special issue',
    'corrigendum', 'correction', 'erratum', 'retraction', 'obituary',
]
SPECIAL_SOURCE_WARNING_KINDS = {
    'TELEGRAM', 'PDF_REPORT', 'MEETING', 'ACADEMIC', 'DEEP_DISCOVERY',
    'COUNTRY_ASSESSMENT',
}
SPECIAL_DATE_REQUIRED_SOURCES = (
    set(TELEGRAM_SOURCES) |
    set(PDF_REPORT_SOURCES) |
    REPORT_API_SOURCE_NAMES |
    set(MEETING_MINUTES_SOURCES) |
    set(CITATION_DERIVED_WEB_SOURCES) |
    COUNTRY_ASSESSMENT_SOURCE_NAMES |
    ACADEMIC_SOURCE_NAMES
)

HEADERS = {'User-Agent': RUNTIME.user_agent}
HTTP_CLIENT = PoliteHttpClient(RUNTIME)

ACADEMIC_API_LOCKS = {
    'crossref': Lock(),
    'openalex': Lock(),
}
ACADEMIC_API_NEXT_ALLOWED = {
    'crossref': 0.0,
    'openalex': 0.0,
}
ACADEMIC_API_CACHE = {}
ACADEMIC_API_429_STREAK = {'crossref': 0, 'openalex': 0}
ACADEMIC_API_BLOCKED_UNTIL = {'crossref': 0.0, 'openalex': 0.0}


def academic_api_url(base_url, params, provider):
    """Attach provider identity/auth parameters without leaking them into source config."""
    query = dict(params)
    provider = provider.lower()
    if provider == 'openalex' and RUNTIME.openalex_api_key:
        query['api_key'] = RUNTIME.openalex_api_key
    if provider == 'crossref' and RUNTIME.crossref_mailto:
        query['mailto'] = RUNTIME.crossref_mailto
    return base_url + '?' + urllib.parse.urlencode(query)

# Sites that routinely block automated list-page access. They remain discoverable
# through Google News / targeted search, but are not treated as hard-fetch sources.
DISCOVERY_ONLY_SOURCES = {
    'ADB Central and West Asia', 'ADB Publications', 'OECD Eurasia Publications',
    'Chatham House Russia and Eurasia', 'Chatham House Search Central Asia',
    'Chatham House Russia-Eurasia Publications',
    'Clingendael Search Central Asia', 'IWPR Investigations Central Asia',
    'The Third Pole Central Asia', 'The Third Pole',
}

def request_academic_api(url, provider, timeout=30):
    """Polite provider adapter for APIs that enforce global per-client limits."""
    provider = provider.lower()
    lock = ACADEMIC_API_LOCKS.setdefault(provider, Lock())
    with lock:
        cached = ACADEMIC_API_CACHE.get(url)
        if cached is not None:
            return cached
        if time.monotonic() < ACADEMIC_API_BLOCKED_UNTIL.get(provider, 0.0):
            note_academic_diag('api_final_429')
            raise RuntimeError('academic API provider cooldown: ' + provider)
        last_response = None
        saw_429 = False
        for attempt in range(2):
            wait_for = ACADEMIC_API_NEXT_ALLOWED.get(provider, 0.0) - time.monotonic()
            if wait_for > 0:
                time.sleep(wait_for)
            try:
                response = request_url(url, timeout=timeout, retries=0)
                last_response = response
                if response.status_code != 429:
                    response.raise_for_status()
                    interval = 0.25 if provider == 'crossref' else (0.20 if RUNTIME.openalex_api_key else 0.75)
                    ACADEMIC_API_NEXT_ALLOWED[provider] = time.monotonic() + interval
                    ACADEMIC_API_429_STREAK[provider] = 0
                    ACADEMIC_API_CACHE[url] = response
                    if saw_429:
                        note_academic_diag('api_success_after_429')
                    return response
                saw_429 = True
                note_academic_diag('api_429_retry')
                retry_after = response.headers.get('Retry-After', '')
                try:
                    delay = max(1.0, min(8.0, float(retry_after)))
                except (TypeError, ValueError):
                    delay = 1.5 * (attempt + 1)
                ACADEMIC_API_429_STREAK[provider] = ACADEMIC_API_429_STREAK.get(provider, 0) + 1
                if ACADEMIC_API_429_STREAK[provider] >= 2:
                    ACADEMIC_API_BLOCKED_UNTIL[provider] = time.monotonic() + 60.0
                    break
                ACADEMIC_API_NEXT_ALLOWED[provider] = time.monotonic() + delay
            except Exception:
                if attempt >= 1:
                    raise
                ACADEMIC_API_NEXT_ALLOWED[provider] = time.monotonic() + 1.0
        if last_response is not None:
            if last_response.status_code == 429:
                note_academic_diag('api_final_429')
            last_response.raise_for_status()
        raise RuntimeError('academic API request failed: ' + provider)
seen_hashes = set()

DISABLED_FEED_SOURCES = {
    # 这些 RSS 地址长期返回 404/410/403，或实际是网页而非 RSS；对应网站仍尽量通过 WEB_SOURCES 抓取。
    'Kazinform', 'Informburo.kz', 'Tengrinews.kz', 'Zakon.kz', 'Kazpravda',
    'Nur.kz', 'Kun.uz', 'Somon.tj', 'Day.az', 'CSIS Central Asia',
    'Lowy Interpreter', 'Central Asia Program (Wilson Center)',
    'Central Asian Survey', 'Post-Soviet Affairs', 'Inner Asia',
    'RUSI Central Asia', 'Foreign Affairs', 'AP News', 'TASS',
    'Regnum Agency', 'RFE/RL Central Asia', 'Azattyq (Kazakh)',
    'Ozodi (Uzbek/Tajik)', 'Gazeta.ru', 'CGTN', 'China Daily',
}

STABLE_SKIP_FEED_SOURCES = {
    # 稳定模式：这些源在最近运行中频繁超时、403、SSL 失败或返回不稳定；先不主动抓 RSS。
    # 多数仍保留网页抓取或由其他稳定媒体覆盖，避免每日运行产生大量失败记录。
    'Aktualno.kz', 'Turbina7.kz', 'Dunyoxabarlari.uz', 'XalqSozi',
    'Zonadaily.uz', 'Report.uz', 'KgNews', 'Asiacenter.kg',
    'Kyrgyzstan Today', 'Arna.kg', 'Asia-Plus TJ', 'Nahod.tj',
    'Vazhnoe.tj', 'TDH TM', 'Tribune.TM', 'News-tm', 'Trend AZ',
    'UzA', 'Podrobno.uz', 'Business Turkmenistan', 'AKIpress',
    'Stimson Center Eurasia', 'Eurasianet', 'Chatham House Russia and Eurasia',
    'Kabar KG', 'RSIS Singapore', 'Central Asia New Strategies',
    'CABAR.asia', 'Gazeta.uz', 'Spot.uz', 'Ziyo.net',
    'Central Asia Foundation', 'Central Asia-Caucasus Analyst',
    'NDTV World', 'Reuters World', 'Al Arabiya English',
    'Eurasia Daily Monitor (Jamestown)', 'Meduza', 'Kavkaz.Realii',
    'SAIIA', 'Khovar TJ', 'Kritika', 'Middle East Eye', 'Caixin Global',
    'BBC World', 'China-US Focus', 'Novaya Gazeta', 'RIA Novosti',
    'Khronika.info', 'SCMP Asia', 'Washington Post World',
    'Al Jazeera', 'Ozodi (Uzbek/Tajik)',
    'Atlantic Council Central Asia', 'TASS',
    'International Crisis Group Central Asia', '24.kg', 'Lenta.ru',
    'Foreign Policy', 'Avesta TJ',
    'DW English',
}

STABLE_SKIP_WEB_SOURCES = {
    # 网页源中近期高频失败或被反爬的站点；保留可用 RSS 或其他替代来源。
    '24.kg', 'ADB Central and West Asia', 'Asia-Plus TJ', 'CABAR.asia',
    'Daryo.uz', 'Eurasian Development Bank', 'Gazeta.uz',
    'Human Rights Watch Central Asia', 'Kabar KG', 'Kapital.kz',
    'Kazinform', 'Khovar TJ', 'Orient TM', 'Podrobno.uz', 'Report.uz',
    'Spot.uz', 'TDH TM', 'Tengrinews', 'Trend AZ', 'UNRCCA', 'UzA',
    'World Bank ECA', 'Zakon.kz',
    'Informburo', 'XalqSozi', 'Turkmenportal', 'Eurasianet',
    'Avesta TJ',
    'OSCE News',
    'The Times of Central Asia', 'Orda.kz', 'Kazpravda',
    'Business Turkmenistan',
    'Kun.uz',
    'UNDP Eurasia',
}

RESTORED_FEED_SOURCES = {
    # 已替换为新 RSS/API，可在稳定模式中恢复尝试。
    'RUSI Central Asia', 'Foreign Affairs', 'Meduza',
    'RFE/RL Central Asia', 'Azattyq (Kazakh)',
    'SCMP Asia', 'CGTN', 'China Daily',
    'Eurasia Daily Monitor (Jamestown)',
    'Foreign Policy', 'International Crisis Group Central Asia',
    'BBC World', 'The Guardian World', 'Le Monde',
    # 2026-07-09 抽测可用：只恢复 RSS，不恢复网页抓取；仍受深度、时效、去重和公众号过滤约束。
    'CABAR.asia', 'Gazeta.uz', 'Podrobno.uz', 'Spot.uz',
    'UzA', 'AKIpress',
    # 2026-07-16 实测可用的智库/报告向 RSS：提高深度入口稳定性，不降低公开门槛。
    'OSW Central Asia', 'EUCAM Policy Briefs RSS',
    'SWP Berlin', 'Clingendael', 'EUISS',
    'Central Asia-Caucasus Analyst',
    'Caspian Policy Center RSS', 'The Diplomat Central Asia',
    # 2026-07-16 机制升级：专家媒体恢复尝试；失败会进入健康日志，不拖垮主流程。
    'CABAR.asia', 'The Times of Central Asia', 'Eurasianet',
    'The Diplomat China-Central Asia', 'Voices on Central Asia',
    # 2026-07-16 deep expansion: verified institute/specialist research RSS.
    'KISI KazISS RSS', 'Central Asia Program RSS', 'CAPS Unlock RSS',
    'NISI Kyrgyzstan', 'EUCAM Policy Briefs RSS', 'OSW Central Asia',
}

# Doubao-aligned S-tier: force daily check even in STABLE_MODE (failures logged, not auto-disabled).
S_TIER_DAILY_FEED_SOURCES = {
    'Eurasianet', 'The Times of Central Asia', 'Caspian Policy Center RSS',
    'International Crisis Group Central Asia', 'Central Asia Program RSS',
    'Central Asia Program (Wilson Center)', 'CSIS Central Asia',
    'Chatham House Russia and Eurasia', 'Reuters World',
    'TASS', 'RIA Novosti', 'Interfax', 'Regnum Agency',
    'Kazinform', 'Kabar KG', 'UzA', 'Khovar TJ', 'TDH TM',
    'Gazeta.uz', 'Kapital.kz English', 'Novastan English', 'CABAR.asia',
    'Central Asia-Caucasus Analyst', 'Azattyq (Kazakh)', 'Ozodi (Uzbek/Tajik)',
    'Fergana News English', 'bne IntelliNews Central Asia',
    'The Diplomat Central Asia', 'The Diplomat',
    'Dialogue Earth', 'The Third Pole', 'IWPR Central Asia',
    'Oxus Society RSS', 'CAPS Unlock RSS', 'ISRS Uzbekistan',
    'IISS Online Analysis',
}

S_TIER_DAILY_WEB_SOURCES = {
    'Eurasianet', 'The Times of Central Asia', 'Caspian Policy Center',
    'Gazeta.uz', 'Kapital.kz', 'Kazinform', 'Kabar KG', 'UzA',
    'Khovar TJ', 'TDH TM', 'CABAR.asia',
    'Oxus Society', 'IWPR Central Asia', 'The Third Pole',
    'ISRS Uzbekistan', 'Dialogue Earth Web',
}

# S-tier means the publisher must remain covered, not that a confirmed dead RSS
# URL must be requested forever. These publishers remain available through web,
# discovery, sitemap, search, or dedicated publication adapters.
KNOWN_DEAD_FEED_SOURCES = {
    'Eurasianet', 'CSIS Central Asia', 'Chatham House Russia and Eurasia',
    'Kabar KG', 'Kazinform', 'Reuters World', 'Oxus Society RSS',
    'The Third Pole', 'IWPR Central Asia', 'IISS Online Analysis',
    'ISRS Uzbekistan', 'Clingendael', 'bne IntelliNews Central Asia',
    'Regnum Agency',
}

RESTORED_WEB_SOURCES = set(S_TIER_DAILY_WEB_SOURCES)

def skipped_feed_sources():
    force = (set(S_TIER_DAILY_FEED_SOURCES) | set(RESTORED_FEED_SOURCES)) - KNOWN_DEAD_FEED_SOURCES
    if STABLE_MODE:
        skipped = (DISABLED_FEED_SOURCES | STABLE_SKIP_FEED_SOURCES) - force
    else:
        skipped = DISABLED_FEED_SOURCES - force
    skipped |= KNOWN_DEAD_FEED_SOURCES
    if not ENABLE_CHINA_PUBLISHER_SOURCES:
        skipped |= set(CN_SOURCES)
    return skipped

def get_active_web_sources():
    force_web = set(S_TIER_DAILY_WEB_SOURCES) | set(RESTORED_WEB_SOURCES)
    if STABLE_MODE:
        return {
            source: url for source, url in WEB_SOURCES.items()
            if source not in STABLE_SKIP_WEB_SOURCES and source not in DISCOVERY_ONLY_SOURCES
            or source in force_web and source not in DISCOVERY_ONLY_SOURCES
        }
    return {source: url for source, url in WEB_SOURCES.items() if source not in DISCOVERY_ONLY_SOURCES}

GENERIC_WEB_TITLES = {
    'central asia', 'europe and central asia', 'news', 'news centre',
    'press centre', 'publications', 'reports', 'events', 'home',
    'central asia center', 'central asia center roundup',
    'international research center', 'international research centre',
    'annual research conference', 'other imf events', 'country focus',
    'communiqués', 'communiques', 'banking laws', 'laws',
    'timeline of decisions', 'decision making schedule',
    '2015 – 2026 decision making schedule', '2015 - 2026 decision making schedule',
    'search', 'newsletter', 'subscribe', 'contact', 'about us',
    'press releases', 'read more', 'more',
}

GENERIC_TITLE_PATTERNS = [
    r'^\s*[-–—]\s*[\w\s]+laws?\s*$',
    r'^\s*\d{4}\s*[-–—]\s*\d{4}\s+decision making schedule\s*$',
    r'^\s*(all|latest)\s+(news|events|publications|reports)\s*$',
    r'^\s*(news|events|publications|reports|press releases)\s*$',
    r'^\s*(?:central asia[- ]caucasus institute|joint center|joint centre)\s*$',
    r'^\s*central\s*%?20asia\.html\s*$',
    r'^\s*(?:fellowship|scholarship|training|programme|program)\b',
]

GENERIC_URL_PARTS = [
    '/about', '/contact', '/privacy', '/cookies', '/terms',
    '/newsletter', '/subscribe', '/search', '/sitemap',
    '/news/sprolls/', '/news/seminars', '/news/country-focus',
    '/news/searchnews', '/laws/', '/rubrics/',
    '/category/central-asia-center/', '/workstream/cac-round-up/',
    '/category/international-research-center/',
    '/fellowships/', '/scholarships/', '/training/', '/courses/',
    '/itemlist/tag/', '/tag/',
]

def clean_web_title(text):
    text = clean_text(text)
    text = re.sub(r'\s*•\s*[A-Za-z][A-Za-z0-9 .,&/\-]{2,80}$', '', text).strip()
    text = re.sub(r'\bRead more\b\s*$', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'^(?:Article|Report|Policy Brief|Working Paper)\s+', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(
        r'^(Press Releases|News|Events|Reports|Publications)\s+(?=[A-ZА-ЯЁ0-9])',
        '',
        text,
        flags=re.IGNORECASE,
    ).strip()
    return text

def is_generic_title(title):
    normalized = clean_text(title).lower().strip(' -–—|:;')
    if not normalized:
        return True
    if normalized in GENERIC_WEB_TITLES:
        return True
    if len(normalized) < 8:
        return True
    if len(normalized.split()) <= 2 and not re.search(r'\d', normalized):
        return True
    return any(re.search(pattern, normalized, re.IGNORECASE) for pattern in GENERIC_TITLE_PATTERNS)

def is_generic_url(link):
    lowered = (link or '').lower()
    try:
        if urllib.parse.urlparse(link).path.rstrip('/') == '':
            return True
    except Exception:
        pass
    return any(part in lowered for part in GENERIC_URL_PARTS)

def is_generic_item(item):
    return is_generic_title(item.get('title', '')) or is_generic_url(item.get('link', ''))

LOCAL_KZ = {
    'Kazinform', 'Informburo', 'Informburo.kz', 'Tengrinews', 'Tengrinews.kz',
    'Zakon.kz', 'Aktualno.kz', 'Nur.kz', 'Kazpravda', 'Turbina7.kz',
    'The Astana Times', 'Vlast.kz', 'Orda.kz', 'Kapital.kz',
    'Kursiv Kazakhstan English',
    'Akorda', 'Kazakhstan Government', 'Kazakhstan MFA',
    'National Bank of Kazakhstan', 'Forbes Kazakhstan',
    'Vlast.kz Telegram', 'Orda.kz Telegram',
}

LOCAL_UZ = {
    'UzA', 'Gazeta.uz', 'Kun.uz', 'Report.uz', 'Dunyoxabarlari.uz',
    'XalqSozi', 'Zonadaily.uz', 'Daryo.uz', 'Podrobno.uz', 'Spot.uz',
    'President of Uzbekistan', 'Uzbekistan Government',
    'Central Bank of Uzbekistan', 'Statistics Agency Uzbekistan',
    'Gazeta.uz Telegram', 'Kun.uz Telegram', 'Daryo Telegram',
}

LOCAL_KG = {
    'Kabar KG', '24.kg', 'KgNews', 'Asiacenter.kg', 'Arna.kg',
    'Kyrgyzstan Today', 'AKIpress', 'Kloop',
    'President of Kyrgyzstan', 'Kyrgyz Cabinet', 'Kyrgyz MFA',
    'National Bank Kyrgyzstan', 'Kyrgyz Statistics', 'Kaktus.media',
    'NISI Kyrgyzstan',
    'AKIpress Telegram', 'Kloop Telegram',
}

LOCAL_TJ = {
    'Khovar TJ', 'Asia-Plus TJ', 'Somon.tj', 'Ziyo.net', 'Nahod.tj',
    'Vazhnoe.tj', 'Avesta TJ', 'Tajik MFA', 'National Bank Tajikistan',
    'Your.tj', 'Tajik CSR Analytical Articles',
    'Asia-Plus TJ Telegram', 'Your.tj Telegram',
}

LOCAL_TM = {
    'TDH TM', 'Tribune.TM', 'News-tm', 'Business Turkmenistan',
    'Turkmenportal', 'Orient TM', 'Turkmenistan Official',
    'Turkmenistan MFA', 'Orient TM Telegram',
}

REGIONAL_LOCAL = {'Trend AZ', 'Day.az', 'Fergana Agency Telegram'}

LOCAL_POLICY_INSTITUTE_SOURCES = {
    'NISI Kyrgyzstan', 'Tajik CSR Analytical Articles',
    # KISI RSS is mixed (analysis + institutional events). Keep analytics page as institute marker;
    # RSS itself stays in HIGH_SIGNAL / PRESTIGE without per-item page metadata fetch.
    'KISI KazISS Analytics',
}

THINK_TANK_SOURCES = {
    'CABAR.asia',
    'Carnegie Endowment Central Asia', 'Eurasianet',
    'Central Asia-Caucasus Analyst', 'Central Asia New Strategies',
    'Carnegie Endowment', 'CSIS Central Asia', 'Atlantic Council Central Asia',
    'Brookings Russia and Eurasia', 'Stimson Center Eurasia',
    'Chatham House Russia and Eurasia', 'RUSI Central Asia', 'ISW',
    'Lowy Interpreter', 'Eurasia Daily Monitor (Jamestown)',
    'Central Asia Program (Wilson Center)', 'Central Asia Foundation',
    'Open Society Foundations (Central Asia)', 'SAIIA', 'RSIS Singapore',
    'Central Asian Survey', 'Post-Soviet Affairs', 'Inner Asia', 'Kritika',
    'Caspian Policy Center', 'Oxus Society',
    'International Crisis Group Central Asia', 'Human Rights Watch Central Asia',
    'Eurasian Development Bank', 'UNRCCA', 'OSCE News',
    'ADB Central and West Asia', 'World Bank ECA', 'UNDP Eurasia',
    'New Lines Central Asia', 'SpecialEurasia Central Asia', 'CAREC',
    'IMF Central Asia',
    'KISI KazISS Analytics', 'KISI KazISS RSS', 'CAREC Institute Publications',
    'Central Asia Program Policy Briefs', 'Central Asia Program RSS', 'OSCE Academy Policy Briefs',
    'EUCAM Policy Briefs', 'EUCAM Policy Briefs RSS', 'EUCAM Research Publications', 'University of Central Asia Publications',
    'FES Central Asia Publications', 'CAPS Unlock Publications', 'CAPS Unlock RSS',
    'IAI Publications',
    'Silk Road Studies Publications',
    'Wilson Center Search Central Asia', 'Kennan Institute Search Central Asia',
    'Davis Center Harvard Central Asia', 'FPRI Search Central Asia',
    'Foreign Policy Centre Search Central Asia', 'SIPRI Search Central Asia',
    'RAND Search Central Asia', 'China Global South Central Asia',
    'CER Search Central Asia',
    'PONARS Eurasia', 'NISI Kyrgyzstan', 'Tajik CSR Analytical Articles', 'Voices on Central Asia', 'Novastan English',
    'Eurasian Research Institute', 'Global Voices Central Asia',
    'Caspian Policy Center RSS', 'The Diplomat Central Asia',
    'The Diplomat China-Central Asia', 'Riddle Russia',
    'OSW Central Asia', 'SWP Berlin', 'Clingendael',
    'EUISS', 'ECFR', 'Dialogue Earth', 'MERICS',
    'German Institute for Development and Sustainability',
    'The Loop ECPR', 'E-International Relations', 'LSE International Development',
    'Carnegie Search Central Asia', 'Brookings Search Central Asia',
    'CSIS Search Central Asia', 'Atlantic Council Search Central Asia',
    'Stimson Search Central Asia', 'Chatham House Search Central Asia',
    'RUSI Search Central Asia', 'SWP Search Central Asia',
    'Clingendael Search Central Asia', 'EUISS Search Central Asia',
    'ECFR Search Central Asia', 'ORF Search Central Asia',
    'Observer Research Foundation Central Asia', 'Manohar Parrikar IDSA Central Asia',
    'Ankasam Central Asia', 'Valdai Search Central Asia',
    'RIAC Search Central Asia', 'Ifri Central Asia',
    'IISS Search Central Asia', 'MERICS Central Asia Search', 'IDOS Central Asia Search',
    'Deep Discovery: Google News',
    'CABAR.asia Telegram', 'EDB Reports', 'EBRD Publications',
    'World Bank ECA Publications', 'CAREC Publications', 'ADB Publications',
    'OECD Eurasia Publications', 'UNDP Europe and Central Asia Publications',
    'IOM Central Asia Publications', 'UNODC Central Asia Publications',
    'Novastan Telegram', 'Academic: Crossref', 'Academic: arXiv',
}

DEEP_ANALYSIS_SOURCES = THINK_TANK_SOURCES | {
    'SIPRI Publications',
    'Ifri Papers Central Asia',
    'EUCAM Research Publications',
    'EUCAM Policy Briefs RSS',
    'KISI KazISS RSS', 'Central Asia Program RSS', 'CAPS Unlock RSS',
    'IAI Publications',
    'The Diplomat', 'Foreign Affairs', 'Foreign Policy', 'The Times of Central Asia',
    'Financial Times World', 'Financial Times Asia',
    'The Economist Asia', 'The Economist Europe', 'New York Times World',
    'Nikkei Asia',
    'Eurasianet', 'RFE/RL Central Asia', 'Azattyq (Kazakh)',
    'RUSI Central Asia', 'Carnegie Endowment', 'Carnegie Endowment Central Asia',
    'Brookings Russia and Eurasia', 'Chatham House Russia and Eurasia',
    'Eurasia Daily Monitor (Jamestown)', 'Central Asia-Caucasus Analyst',
    'New Lines Central Asia', 'SpecialEurasia Central Asia', 'Caspian Policy Center',
    'Oxus Society', 'International Crisis Group Central Asia',
    'PONARS Eurasia', 'NISI Kyrgyzstan', 'Tajik CSR Analytical Articles', 'Voices on Central Asia', 'Novastan English',
    'Eurasian Research Institute', 'Global Voices Central Asia',
    'Caspian Policy Center RSS', 'The Diplomat Central Asia',
    'The Diplomat China-Central Asia', 'Riddle Russia',
    'OSW Central Asia', 'SWP Berlin', 'Clingendael',
    'EUISS', 'ECFR', 'Dialogue Earth', 'MERICS',
    'German Institute for Development and Sustainability',
    'The Loop ECPR', 'E-International Relations', 'LSE International Development',
    'Carnegie Search Central Asia', 'Brookings Search Central Asia',
    'CSIS Search Central Asia', 'Atlantic Council Search Central Asia',
    'Stimson Search Central Asia', 'Chatham House Search Central Asia',
    'RUSI Search Central Asia', 'SWP Search Central Asia',
    'Clingendael Search Central Asia', 'EUISS Search Central Asia',
    'ECFR Search Central Asia', 'ORF Search Central Asia',
    'Observer Research Foundation Central Asia', 'Manohar Parrikar IDSA Central Asia',
    'Ankasam Central Asia', 'Valdai Search Central Asia',
    'RIAC Search Central Asia', 'Ifri Central Asia',
    'IISS Search Central Asia', 'MERICS Central Asia Search', 'IDOS Central Asia Search',
    'Deep Discovery: Google News',
    'War on the Rocks', 'German Marshall Fund',
    'CABAR.asia Telegram', 'EDB Reports', 'EBRD Publications',
    'World Bank ECA Publications', 'CAREC Publications', 'ADB Publications',
    'OECD Eurasia Publications', 'UNDP Europe and Central Asia Publications',
    'IOM Central Asia Publications', 'UNODC Central Asia Publications',
    'Novastan Telegram', 'Academic: Crossref', 'Academic: arXiv',
}

OFFICIAL_POLICY_SOURCES = {
    'Akorda', 'Kazakhstan Government', 'Kazakhstan MFA',
    'National Bank of Kazakhstan', 'President of Uzbekistan',
    'Uzbekistan Government', 'Central Bank of Uzbekistan',
    'Statistics Agency Uzbekistan', 'President of Kyrgyzstan',
    'Kyrgyz Cabinet', 'Kyrgyz MFA', 'National Bank Kyrgyzstan',
    'Kyrgyz Statistics', 'Tajik MFA', 'National Bank Tajikistan',
    'Turkmenistan Official', 'Turkmenistan MFA', 'UNDP Eurasia',
    'UNRCCA', 'OSCE News', 'Eurasian Development Bank', 'CAREC',
    'IMF Central Asia',
    'SCO News', 'CICA Press Releases', 'CAREC Events',
    'UNRCCA Press Releases',
    'EDB Reports', 'EBRD Publications', 'World Bank ECA Publications',
    'CAREC Publications', 'ADB Publications', 'OECD Eurasia Publications',
    'UNDP Europe and Central Asia Publications', 'IOM Central Asia Publications',
    'UNODC Central Asia Publications',
}

RU_SOURCES = {
    'Meduza', 'TASS', 'RIA Novosti', 'Regnum Agency', 'Kavkaz.Realii',
    'Lenta.ru', 'Gazeta.ru', 'Novaya Gazeta', 'Interfax',
    'Azattyq (Kazakh)', 'Ozodi (Uzbek/Tajik)', 'Khronika.info',
    'RFE/RL Central Asia', 'Riddle Russia', 'Fergana Agency Telegram',
}

CN_SOURCES = {'SCMP Asia', 'Caixin Global', 'CGTN', 'China Daily', 'China-US Focus'}

REGIONAL_POLICY_SOURCES = {
    'UNDP Eurasia', 'OSCE News', 'Eurasian Development Bank',
    'ADB Central and West Asia', 'World Bank ECA', 'IMF Central Asia',
}

CENTRAL_ASIA_POLICY_SOURCES = {'CAREC', 'UNRCCA', 'UNRCCA Press Releases'}

NATIONAL_OFFICIAL_SOURCES = OFFICIAL_POLICY_SOURCES - REGIONAL_POLICY_SOURCES - CENTRAL_ASIA_POLICY_SOURCES

CENTRAL_ASIA_SPECIALIST_SOURCES = {
    'CABAR.asia', 'Eurasianet',
    'Central Asia-Caucasus Analyst', 'Central Asia New Strategies',
    'Caspian Policy Center', 'Oxus Society',
    'International Crisis Group Central Asia', 'Human Rights Watch Central Asia',
    'Central Asia Program (Wilson Center)', 'Central Asia Foundation',
    'Open Society Foundations (Central Asia)',
    'Voices on Central Asia', 'Novastan English', 'Global Voices Central Asia',
    'Caspian Policy Center RSS', 'The Diplomat Central Asia',
    'The Diplomat China-Central Asia', 'OSW Central Asia',
    'Novastan Telegram', 'Fergana Agency Telegram',
    'New Lines Central Asia', 'SpecialEurasia Central Asia',
    'CAREC', 'UNRCCA', 'UNRCCA Press Releases',
    'KISI KazISS Analytics', 'KISI KazISS RSS', 'CAREC Institute Publications',
    'Central Asia Program Policy Briefs', 'Central Asia Program RSS', 'OSCE Academy Policy Briefs',
    'EUCAM Policy Briefs', 'University of Central Asia Publications',
    'FES Central Asia Publications', 'CAPS Unlock Publications', 'CAPS Unlock RSS',
    'Silk Road Studies Publications',
}

HIGH_SIGNAL_DEEP_SOURCES = {
    'SIPRI Publications',
    'Ifri Papers Central Asia',
    'EUCAM Research Publications',
    'EUCAM Policy Briefs RSS',
    'Carnegie Endowment Central Asia', 'Eurasianet', 'CABAR.asia',
    'Central Asia-Caucasus Analyst', 'Central Asia New Strategies',
    'New Lines Central Asia', 'SpecialEurasia Central Asia',
    'Caspian Policy Center', 'Oxus Society',
    'International Crisis Group Central Asia', 'PONARS Eurasia',
    'NISI Kyrgyzstan', 'Tajik CSR Analytical Articles',
    'Voices on Central Asia', 'Novastan English',
    'Eurasian Research Institute', 'Carnegie Endowment',
    'Caspian Policy Center RSS', 'The Diplomat Central Asia',
    'The Diplomat China-Central Asia', 'Riddle Russia',
    'OSW Central Asia',
    'Carnegie Search Central Asia', 'Brookings Search Central Asia',
    'CSIS Search Central Asia', 'Atlantic Council Search Central Asia',
    'Stimson Search Central Asia', 'Chatham House Search Central Asia',
    'RUSI Search Central Asia', 'SWP Search Central Asia',
    'Clingendael Search Central Asia', 'EUISS Search Central Asia',
    'ECFR Search Central Asia', 'ORF Search Central Asia',
    'Observer Research Foundation Central Asia', 'Manohar Parrikar IDSA Central Asia',
    'Ankasam Central Asia', 'Valdai Search Central Asia',
    'RIAC Search Central Asia', 'Ifri Central Asia',
    'IISS Search Central Asia', 'MERICS Central Asia Search', 'IDOS Central Asia Search',
    'Deep Discovery: Google News',
    'KISI KazISS Analytics', 'KISI KazISS RSS', 'CAREC Institute Publications',
    'Central Asia Program Policy Briefs', 'Central Asia Program RSS', 'OSCE Academy Policy Briefs',
    'EUCAM Policy Briefs', 'University of Central Asia Publications',
    'FES Central Asia Publications', 'CAPS Unlock Publications', 'CAPS Unlock RSS',
    'IAI Publications',
    'Silk Road Studies Publications',
    'Wilson Center Search Central Asia', 'Kennan Institute Search Central Asia',
    'Davis Center Harvard Central Asia', 'FPRI Search Central Asia',
    'Foreign Policy Centre Search Central Asia', 'SIPRI Search Central Asia',
    'RAND Search Central Asia', 'China Global South Central Asia',
    'CER Search Central Asia',
    'Foreign Affairs', 'Foreign Policy',
    'Financial Times World', 'Financial Times Asia',
    'The Economist Asia', 'The Economist Europe', 'New York Times World',
    'Nikkei Asia', 'War on the Rocks', 'German Marshall Fund',
}

PRESTIGE_LONGFORM_SOURCES = {
    'Dialogue Earth', 'The Third Pole', 'IWPR Central Asia',
    'Oxus Society RSS', 'Oxus Society', 'ISRS Uzbekistan',
    'IISS Online Analysis',
    'EUCAM Policy Briefs RSS',
    'KISI KazISS RSS',
    'Central Asia Program RSS',
    'CAPS Unlock RSS',
    'Caspian Policy Center RSS', 'Caspian Policy Center',
    'Central Asia-Caucasus Analyst', 'The Times of Central Asia',
    'Novastan English', 'Eurasianet', 'CABAR.asia',
    'bne IntelliNews Central Asia', 'Voices on Central Asia',
    'Ifri Papers Central Asia',
    'Carnegie Endowment Central Asia', 'Carnegie Endowment',
    'Carnegie Search Central Asia', 'CSIS Search Central Asia',
    'Ifri Central Asia', 'OSW Central Asia',
    'PONARS Eurasia', 'NISI Kyrgyzstan', 'Tajik CSR Analytical Articles',
    'FPRI Search Central Asia', 'Wilson Center Search Central Asia',
    'Kennan Institute Search Central Asia', 'Davis Center Harvard Central Asia',
    'RUSI Central Asia', 'RUSI Search Central Asia',
    'Chatham House Russia and Eurasia', 'Chatham House Search Central Asia',
    'SWP Berlin', 'SWP Search Central Asia', 'EUISS', 'EUISS Search Central Asia',
    'IISS Search Central Asia', 'SIPRI Search Central Asia',
    'RAND Search Central Asia', 'MERICS Central Asia Search',
    'IDOS Central Asia Search', 'Clingendael', 'Clingendael Search Central Asia',
    'ECFR', 'ECFR Search Central Asia',
    'Central Asia Program Policy Briefs', 'OSCE Academy Policy Briefs',
    'EUCAM Policy Briefs', 'University of Central Asia Publications',
}

PRESTIGE_DEEP_METADATA_TERMS = [
    'analysis', 'commentary', 'research', 'report', 'policy brief',
    'working paper', 'study', 'assessment', 'investigation', 'long read',
    'аналитика', 'анализ', 'исследование', 'доклад',
    'analiz', 'analizi', 'araştırma', 'araştırması', 'rapor', 'raporu', 'değerlendirme', 'inceleme',
    'تحلیل', 'پژوهش', 'گزارش', 'ارزیابی', 'بررسی',
]
PRESTIGE_DEEP_METADATA_LOWER = [term.lower() for term in PRESTIGE_DEEP_METADATA_TERMS]

BROAD_REGIONAL_DEEP_SOURCES = {
    'Caspian Policy Center RSS', 'Caspian Policy Center',
    'Riddle Russia', 'OSW Central Asia',
    'Financial Times World', 'Financial Times Asia',
    'The Economist Asia', 'The Economist Europe', 'New York Times World',
    'Nikkei Asia', 'War on the Rocks', 'German Marshall Fund',
    'SWP Berlin', 'Clingendael', 'EUISS', 'ECFR', 'Dialogue Earth',
    'Carnegie Search Central Asia', 'Brookings Search Central Asia',
    'CSIS Search Central Asia', 'Atlantic Council Search Central Asia',
    'Stimson Search Central Asia', 'Chatham House Search Central Asia',
    'RUSI Search Central Asia', 'SWP Search Central Asia',
    'Clingendael Search Central Asia', 'EUISS Search Central Asia',
    'ECFR Search Central Asia', 'ORF Search Central Asia',
    'Observer Research Foundation Central Asia', 'Manohar Parrikar IDSA Central Asia',
    'Ankasam Central Asia', 'Valdai Search Central Asia',
    'RIAC Search Central Asia', 'Ifri Central Asia',
    'IISS Search Central Asia', 'MERICS Central Asia Search', 'IDOS Central Asia Search',
    'Deep Discovery: Google News',
    'Silk Road Studies Publications', 'FES Central Asia Publications',
    'Wilson Center Search Central Asia', 'Kennan Institute Search Central Asia',
    'Davis Center Harvard Central Asia', 'FPRI Search Central Asia',
    'Foreign Policy Centre Search Central Asia', 'SIPRI Search Central Asia',
    'RAND Search Central Asia', 'China Global South Central Asia',
    'CER Search Central Asia',
}

THINK_TANK_SOURCES.update(NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES)
DEEP_ANALYSIS_SOURCES.update(NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES)
HIGH_SIGNAL_DEEP_SOURCES.update(NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES)
PRESTIGE_LONGFORM_SOURCES.update(NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES)
BROAD_REGIONAL_DEEP_SOURCES.update(NEIGHBORING_COUNTRY_PERSPECTIVE_SOURCES)

LOCAL_AND_OFFICIAL_SOURCES = (
    LOCAL_KZ | LOCAL_UZ | LOCAL_KG | LOCAL_TJ | LOCAL_TM |
    REGIONAL_LOCAL | OFFICIAL_POLICY_SOURCES
)

LOCAL_NEWS_CONTEXT_SOURCES = (
    LOCAL_KZ | LOCAL_UZ | LOCAL_KG | LOCAL_TJ | LOCAL_TM |
    REGIONAL_LOCAL | NATIONAL_OFFICIAL_SOURCES | CENTRAL_ASIA_POLICY_SOURCES
)

SOURCE_CONTEXT_SOURCES = LOCAL_AND_OFFICIAL_SOURCES | CENTRAL_ASIA_SPECIALIST_SOURCES

POLICY_DATA_TERMS = [
    'policy', 'strategy', 'decree', 'law', 'regulation', 'agreement',
    'statistics', 'data', 'indicator', 'survey', 'report', 'outlook',
    'forecast', 'budget', 'inflation', 'base rate', 'monetary',
    'central bank', 'gdp', 'trade', 'export', 'import', 'investment',
    'tariff', 'corridor', 'infrastructure', 'program', 'project',
    'approved', 'adopted', 'signed', 'meeting', 'summit', 'forum',
    '政策', '战略', '法令', '法律', '法规', '协议', '统计', '数据',
    '指标', '调查', '报告', '展望', '预测', '预算', '通胀', '基准利率',
    '央行', '贸易', '投资', '走廊', '基础设施', '项目', '批准', '签署',
    '会议', '峰会', '论坛',
    'политик', 'стратег', 'закон', 'статист', 'данн', 'отчет',
    'инфляц', 'ставк', 'инвестиц', 'торгов', 'соглаш',
]
POLICY_DATA_LOWER = [term.lower() for term in POLICY_DATA_TERMS]

EVENT_SIGNAL_TERMS = [
    'announced', 'launched', 'opened', 'met', 'held', 'discussed',
    'visited', 'signed', 'approved', 'adopted', 'presented', 'published',
    'briefing', 'statement', 'decision', 'consultation', 'roundtable',
    '宣布', '启动', '举行', '会见', '访问', '签署', '批准', '发布',
    '简报', '声明', '决定', '磋商', '圆桌',
]
EVENT_SIGNAL_LOWER = [term.lower() for term in EVENT_SIGNAL_TERMS]

def short_error(exc):
    text = str(exc)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:300]

FETCH_DIAG = {
    'requests': 0, 'ok': 0, 'status_403': 0, 'status_404': 0,
    'status_429': 0, 'status_5xx': 0, 'timeouts': 0, 'other_errors': 0,
}

def request_url(url, timeout=12, retries=1):
    try:
        FETCH_DIAG['requests'] += 1
        headers = dict(HEADERS)
        headers.setdefault('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
        headers.setdefault('Accept-Language', 'en-US,en;q=0.8,zh-CN;q=0.5')
        response = HTTP_CLIENT.get(url, headers=headers, timeout=timeout, retries=retries)
        # Several institution sites omit or misstate UTF-8 in Content-Type.
        # Correct only the requests default/unknown case; explicit valid
        # charsets remain untouched.
        if not response.encoding or response.encoding.lower() in {'iso-8859-1', 'latin-1'}:
            apparent = (response.apparent_encoding or '').lower()
            if apparent in {'utf-8', 'utf_8'}:
                response.encoding = 'utf-8'
        if 200 <= response.status_code < 400:
            FETCH_DIAG['ok'] += 1
        elif response.status_code == 403:
            FETCH_DIAG['status_403'] += 1
        elif response.status_code == 404:
            FETCH_DIAG['status_404'] += 1
        elif response.status_code == 429:
            FETCH_DIAG['status_429'] += 1
        elif response.status_code >= 500:
            FETCH_DIAG['status_5xx'] += 1
        return response
    except Exception as exc:
        text = str(exc).lower()
        if 'timeout' in text or 'timed out' in text:
            FETCH_DIAG['timeouts'] += 1
        else:
            FETCH_DIAG['other_errors'] += 1
        raise

def request_url_with_urllib_fallback(url, timeout=12, retries=1):
    """Dedicated fallback for high-value pages that block requests but allow plain HTTP clients."""
    response = request_url(url, timeout=timeout, retries=retries)
    if response.status_code not in {403, 429}:
        return response
    request = urllib.request.Request(url, headers={
        'User-Agent': HEADERS['User-Agent'],
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.8',
    })
    with urllib.request.urlopen(request, timeout=timeout) as handle:
        body = handle.read()
        fallback = requests.Response()
        fallback.status_code = getattr(handle, 'status', 200)
        fallback.url = handle.geturl()
        fallback.headers = dict(handle.headers.items())
        fallback._content = body
        fallback.encoding = handle.headers.get_content_charset() or 'utf-8'
        if 200 <= fallback.status_code < 400:
            FETCH_DIAG['ok'] += 1
        return fallback

GOOGLE_NEWS_RESOLVE_CACHE = {}
GOOGLE_NEWS_RESOLVE_LOADED = False

def load_google_news_resolve_cache():
    global GOOGLE_NEWS_RESOLVE_CACHE, GOOGLE_NEWS_RESOLVE_LOADED
    if GOOGLE_NEWS_RESOLVE_LOADED:
        return
    GOOGLE_NEWS_RESOLVE_LOADED = True
    try:
        if GOOGLE_NEWS_RESOLVE_CACHE_FILE.exists():
            data = json.loads(GOOGLE_NEWS_RESOLVE_CACHE_FILE.read_text(encoding='utf-8'))
            if isinstance(data, dict):
                GOOGLE_NEWS_RESOLVE_CACHE.update({
                    str(k): str(v) for k, v in data.items()
                    if isinstance(k, str) and isinstance(v, str) and v.startswith('http')
                })
    except Exception:
        pass

def save_google_news_resolve_cache():
    try:
        # Keep cache bounded.
        items = list(GOOGLE_NEWS_RESOLVE_CACHE.items())
        if len(items) > 2000:
            items = items[-2000:]
        atomic_write_json(GOOGLE_NEWS_RESOLVE_CACHE_FILE, dict(items))
    except Exception:
        pass

def is_google_news_url(url):
    try:
        parsed = urllib.parse.urlparse(url or '')
        host = (parsed.hostname or '').lower()
        path = parsed.path or ''
        return host == 'news.google.com' and ('/articles/' in path or '/read/' in path)
    except Exception:
        return False

def google_news_article_id(url):
    try:
        path = urllib.parse.urlparse(url or '').path.rstrip('/')
        parts = path.split('/')
        if len(parts) >= 2 and parts[-2] in {'articles', 'read'}:
            return parts[-1]
    except Exception:
        return ''
    return ''

def resolve_google_news_url(url, timeout=12):
    """Resolve Google News article redirect URLs to publisher originals."""
    load_google_news_resolve_cache()
    url = clean_text(url or '')
    if not url:
        return ''
    if not is_google_news_url(url):
        return url
    if url in GOOGLE_NEWS_RESOLVE_CACHE:
        return GOOGLE_NEWS_RESOLVE_CACHE[url]
    article_id = google_news_article_id(url)
    if not article_id:
        GOOGLE_NEWS_RESOLVE_CACHE[url] = url
        return url
    resolved = ''
    try:
        # Prefer /articles/ page which exposes signature/timestamp attributes.
        page_urls = [
            'https://news.google.com/articles/' + article_id,
            'https://news.google.com/rss/articles/' + article_id,
        ]
        signature = ''
        timestamp = ''
        for page_url in page_urls:
            try:
                resp = request_url(page_url, timeout=timeout, retries=0)
                resp.raise_for_status()
                match_sg = re.search(r'data-n-a-sg="([^"]+)"', resp.text)
                match_ts = re.search(r'data-n-a-ts="([^"]+)"', resp.text)
                if match_sg and match_ts:
                    signature = match_sg.group(1)
                    timestamp = match_ts.group(1)
                    break
            except Exception:
                continue
        if signature and timestamp:
            payload = [
                'Fbv4je',
                (
                    '["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,null,null,null,null,null,0,1],'
                    '"X","X",1,[1,1,1],1,1,null,0,0,null,0],"'
                    + article_id + '",' + str(timestamp) + ',"' + signature + '"]'
                ),
            ]
            post_headers = dict(HEADERS)
            post_headers['Content-Type'] = 'application/x-www-form-urlencoded;charset=UTF-8'
            post_headers['Referer'] = 'https://news.google.com/'
            post_resp = HTTP_CLIENT.request(
                'POST',
                'https://news.google.com/_/DotsSplashUi/data/batchexecute',
                headers=post_headers,
                data='f.req=' + urllib.parse.quote(json.dumps([[payload]])),
                timeout=timeout,
                retries=1,
            )
            post_resp.raise_for_status()
            body = post_resp.text
            # Primary parse path used by googlenewsdecoder.
            try:
                parsed = json.loads(body.split('\n\n', 1)[1])[:-2]
                candidate = json.loads(parsed[0][2])[1]
                if isinstance(candidate, str) and candidate.startswith('http') and 'news.google.com' not in candidate:
                    resolved = candidate
            except Exception:
                match = re.search(r'garturlres\",\"(https?:\\/\\/[^\\"]+)\"', body)
                if not match:
                    match = re.search(r'garturlres","(https?://[^"]+)"', body)
                if match:
                    candidate = match.group(1).replace('\\/', '/')
                    if candidate.startswith('http') and 'news.google.com' not in candidate:
                        resolved = candidate
    except Exception:
        resolved = ''
    final_url = resolved or url
    GOOGLE_NEWS_RESOLVE_CACHE[url] = final_url
    return final_url

def resolve_item_link(item):
    """In-place resolve Google News links; keep original for fallback display."""
    if not item:
        return item
    link = clean_text(item.get('link', ''))
    if not is_google_news_url(link):
        return item
    if item.get('link_resolved'):
        return item
    resolved = resolve_google_news_url(link)
    item['google_news_link'] = link
    if resolved and resolved != link and not is_google_news_url(resolved):
        item['link'] = resolved
        item['link_resolved'] = True
    else:
        item['link_resolved'] = False
        # Fallback search on publisher homepage domain when available.
        home = clean_text(item.get('publisher_home', ''))
        if home:
            host = urllib.parse.urlparse(home).netloc
            if host:
                query = clean_title(item.get('title', ''))
                item['publisher_search_link'] = (
                    'https://www.google.com/search?q='
                    + urllib.parse.quote(query + ' site:' + host)
                )
    return item

def resolve_item_links(items, label='links'):
    items = list(items or [])
    if not items:
        return items
    pending = [item for item in items if is_google_news_url(item.get('link', '')) and not item.get('link_resolved')]
    if not pending:
        return items
    print('  Resolving Google News ' + label + ': ' + str(len(pending)) + ' url(s)...')
    # Parallel but small pool to avoid rate limits.
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(resolve_item_link, pending))
    resolved_count = sum(1 for item in pending if item.get('link_resolved'))
    print('  Resolved ' + str(resolved_count) + '/' + str(len(pending)) + ' Google News links.')
    save_google_news_resolve_cache()
    return items


def clean_rss_summary_html(summary):
    text = clean_text(summary or '')
    if not text:
        return ''
    # Strip residual HTML and Google News boilerplate.
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^(View Full Coverage on Google News|Read more|Continue reading)[:\s-]*', '', text, flags=re.I)
    text = re.sub(r'\s*(?:Read more|Continue reading|阅读更多)\s*[»›].*$', '', text, flags=re.I)
    return clean_text(text)

def enrich_item_from_article(item):
    """Fetch original-page metadata after link resolution to improve clues and depth signals."""
    if not item or item.get('enriched'):
        return item
    link = clean_text(item.get('link', ''))
    if not link or is_google_news_url(link):
        item['enriched'] = False
        return item
    # Skip non-http and known low-value endpoints.
    if not link.startswith('http'):
        item['enriched'] = False
        return item
    try:
        metadata = fetch_article_metadata(link)
        # Publishers such as Frontiers expose a PDF URL in feeds while the
        # article landing page carries the usable title, abstract and date.
        if not metadata.get('summary') and (
            link.lower().endswith('/pdf') or link.lower().endswith('.pdf')
        ):
            landing_link = re.sub(r'/pdf/?$', '', link, flags=re.I)
            landing_link = re.sub(r'\.pdf(?:\?.*)?$', '', landing_link, flags=re.I)
            if landing_link and landing_link != link:
                landing_metadata = fetch_article_metadata(landing_link)
                if landing_metadata.get('summary') or landing_metadata.get('published'):
                    item['link'] = landing_link
                    link = landing_link
                    metadata = landing_metadata
        item['enriched'] = True
        old_summary = clean_rss_summary_html(item.get('summary', ''))
        new_summary = clean_text(metadata.get('summary', ''))
        if new_summary and '%pdf-' not in new_summary.lower() and len(new_summary) >= max(80, len(old_summary) + 20):
            item['summary'] = new_summary[:500]
            item['summary_enriched'] = True
        elif old_summary:
            item['summary'] = old_summary[:500]
        if metadata.get('content_type'):
            item['content_type'] = metadata.get('content_type')
        if metadata.get('word_count'):
            item['word_count'] = metadata.get('word_count')
        if metadata.get('access_status') and metadata.get('access_status') != 'unknown':
            item['access_status'] = metadata.get('access_status')
        # Light re-score using enriched text so display/reasoning improve.
        content = item_content_text(item)
        item['research_score'] = max(item.get('research_score', 0), count_terms(content, RESEARCH_LOWER))
        item['depth_term_score'] = max(item.get('depth_term_score', 0), count_terms(content, DEPTH_LOWER))
        topic_matches = research_topic_matches(item)
        if topic_matches:
            item['priority_topics'] = [match['label'] for match in topic_matches[:3]]
            item['priority_score'] = max(item.get('priority_score', 0), research_topic_score(item))
    except Exception:
        item['enriched'] = False
    return item

def enrich_items_for_output(items, label='candidates'):
    items = list(items or [])
    if not items:
        return items
    pending = [
        item for item in items
        if not item.get('enriched')
        and clean_text(item.get('link', '')).startswith('http')
        and not is_google_news_url(item.get('link', ''))
    ]
    if not pending:
        return items
    print('  Enriching article metadata for ' + label + ': ' + str(len(pending)) + ' item(s)...')
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(enrich_item_from_article, pending))
    ok = sum(1 for item in pending if item.get('summary_enriched'))
    print('  Enriched summaries: ' + str(ok) + '/' + str(len(pending)))
    return items

def summarize_near_misses(low_research_public_items, limit=8):
    """Explain why recent high-signal items did not enter the public digest."""
    rows = []
    ranked = sorted(
        low_research_public_items or [],
        key=lambda item: (
            -item.get('priority_score', 0),
            -item.get('research_score', 0),
            -item.get('depth_term_score', 0),
            -item.get('core_score', 0),
        )
    )
    for item in ranked[:40]:
        if not has_strong_central_asia_anchor(item):
            reason = '中亚强相关不足'
        elif is_thin_analytical_news(item):
            reason = '分析性新闻/快讯体裁'
        elif is_event_or_conference_announcement(item):
            reason = '会议/活动预告'
        elif is_official_activity_news(item):
            reason = '官方活动动态'
        elif is_news_aggregation_item(item):
            reason = '新闻聚合/综述'
        elif not has_verifiable_publication_time(item):
            reason = '无法确认发布时间'
        elif not is_recent_item(item):
            reason = '超出时效窗口'
        elif not is_strict_deep_public_item(item) and not is_substantive_policy_document(item):
            reason = '缺少深度体裁证据'
        else:
            reason = '未达公开深读综合门槛'
        rows.append({
            'title': clean_title(item.get('title', ''))[:120],
            'source': item.get('source', ''),
            'reason': reason,
            'date': clean_text(item.get('published', ''))[:32],
        })
        if len(rows) >= limit:
            break
    return rows

def write_near_miss_section(lines, near_misses):
    if not near_misses:
        lines.append('公开版近失样本：本期没有可展示的高信号近失条目。')
        return
    lines.append('公开版近失样本（有研究信号但未进公开版，便于判断门槛是否过严/过松）：')
    for index, row in enumerate(near_misses, start=1):
        lines.append(
            str(index) + '. [' + row['reason'] + '] '
            + row['source'] + '｜'
            + row['title']
            + (('｜' + row['date']) if row['date'] else '')
        )

def record_source_warning(kind, source_name, url, exc):
    SOURCE_WARNINGS.append({
        'kind': kind,
        'source': source_name,
        'url': url,
        'error': short_error(exc),
    })

def classify_access_error(error):
    text = clean_text(error).lower()
    if '403' in text or 'forbidden' in text or 'cloudflare' in text:
        return '403/反爬'
    if '404' in text or 'not found' in text or '410' in text:
        return '404/死链'
    if '429' in text or 'too many requests' in text:
        return '429/限流'
    if 'timeout' in text or 'timed out' in text:
        return '超时'
    if 'ssl' in text or 'ssleof' in text or 'connection' in text:
        return '连接/SSL'
    return '其他'

def warning_count(kinds):
    return sum(1 for warning in SOURCE_WARNINGS if warning['kind'] in kinds)

def update_candidate_history(candidate_web_jobs):
    if not TEST_CANDIDATE_SOURCES:
        return
    try:
        if CANDIDATE_HEALTH_FILE.exists():
            history = json.loads(CANDIDATE_HEALTH_FILE.read_text(encoding='utf-8'))
        else:
            history = {}
    except Exception:
        history = {}
    counts = dict(candidate_web_jobs)
    warning_sources = {
        warning['source']
        for warning in SOURCE_WARNINGS
        if warning['kind'] == 'CANDIDATE_WEB'
    }
    today = str(TODAY)
    for source in CANDIDATE_WEB_SOURCES:
        records = [
            record for record in history.get(source, [])
            if record.get('date') != today
        ]
        records.append({
            'date': today,
            'items': counts.get(source, 0),
            'warning': source in warning_sources,
        })
        history[source] = records[-7:]
    atomic_write_json(CANDIDATE_HEALTH_FILE, history)

def jobs_with_items(jobs):
    return sum(1 for _, count in jobs if count > 0)

def sources_with_items(jobs):
    return len({source for source, count in jobs if count > 0})

def job_item_total(jobs):
    return sum(count for _, count in jobs)

def extra_source_scope_text(extra_source_jobs):
    parts = []
    for group in extra_source_jobs or []:
        unit = '查询任务' if group.get('kind') in {'DEEP_DISCOVERY', 'ACADEMIC'} else '来源'
        parts.append(
            group['label'] + ' ' + str(jobs_with_items(group['jobs'])) + '/' + str(group['total'])
            + ' 个' + unit + '返回非空'
        )
    if not parts:
        return ''
    return '；' + '；'.join(parts)

def write_source_health_log(feed_jobs, web_jobs, active_feed_count, active_web_count, candidate_web_jobs=None, extra_source_jobs=None, near_misses=None):
    candidate_web_jobs = candidate_web_jobs or []
    extra_source_jobs = extra_source_jobs or []
    skipped_feeds = skipped_feed_sources()
    active_web_sources = set(get_active_web_sources())
    skipped_web_sources = set(WEB_SOURCES) - active_web_sources
    lines = []
    lines.append('中亚研究每日简报 - 数据源健康日志')
    lines.append('日期：' + str(TODAY))
    lines.append('')
    lines.append('稳定模式：' + ('开启' if STABLE_MODE else '关闭'))
    lines.append('S级出版方覆盖：' + str(len(S_TIER_DAILY_FEED_SOURCES)) + ' 个 RSS 配置 / '
                 + str(len(S_TIER_DAILY_WEB_SOURCES)) + ' 个网页配置；已知死 RSS '
                 + str(len(KNOWN_DEAD_FEED_SOURCES)) + ' 个改走网页、发现或专用适配器。')
    lines.append('RSS 源：启用 ' + str(active_feed_count) + ' / 总计 ' + str(len(FEEDS)) + '；跳过 ' + str(len(skipped_feeds)))
    lines.append('网页源：启用 ' + str(active_web_count) + ' / 总计 ' + str(len(WEB_SOURCES)) + '；跳过 ' + str(len(WEB_SOURCES) - active_web_count))
    lines.append('HTTP 访问诊断：请求 ' + str(FETCH_DIAG.get('requests', 0))
                 + '；成功 ' + str(FETCH_DIAG.get('ok', 0))
                 + '；403=' + str(FETCH_DIAG.get('status_403', 0))
                 + '；404=' + str(FETCH_DIAG.get('status_404', 0))
                 + '；429=' + str(FETCH_DIAG.get('status_429', 0))
                 + '；5xx=' + str(FETCH_DIAG.get('status_5xx', 0))
                 + '；超时=' + str(FETCH_DIAG.get('timeouts', 0)))
    lines.append('候选网页源：' + ('启用 ' + str(len(CANDIDATE_WEB_SOURCES)) if TEST_CANDIDATE_SOURCES else '未启用'))
    if extra_source_jobs:
        lines.append('专项补强源：' + '；'.join(group['label'] + ' 启用 ' + str(group['total']) for group in extra_source_jobs))
    lines.append('')
    lines.append(
        'RSS 返回非空：' + str(sources_with_items(feed_jobs)) + ' 个真实来源 / '
        + str(jobs_with_items(feed_jobs)) + ' 个 URL 任务；原始条目 ' + str(job_item_total(feed_jobs)) + ' 条（尚未过滤）'
    )
    lines.append(
        '网页返回非空：' + str(sources_with_items(web_jobs)) + ' 个来源；原始条目 '
        + str(job_item_total(web_jobs)) + ' 条（尚未过滤）'
    )
    if TEST_CANDIDATE_SOURCES:
        lines.append(
            '候选网页返回非空：' + str(sources_with_items(candidate_web_jobs)) + ' 个来源；原始条目 '
            + str(job_item_total(candidate_web_jobs)) + ' 条（含首页旧链接，尚未过滤）'
        )
    for group in extra_source_jobs:
        unit = '查询任务' if group.get('kind') in {'DEEP_DISCOVERY', 'ACADEMIC'} else '来源'
        suffix = '；任务数不等于出版方数量' if unit == '查询任务' else ''
        lines.append(
            group['label'] + ' 返回非空' + unit + '：' + str(jobs_with_items(group['jobs']))
            + ' / ' + str(group['total']) + '；原始条目 ' + str(job_item_total(group['jobs'])) + ' 条' + suffix
        )
    lines.append('主源失败记录数：' + str(warning_count({'RSS', 'WEB'})))
    if ENABLE_ACADEMIC_SOURCES and any(ACADEMIC_FETCH_DIAG.values()):
        lines.append('学术 API 身份配置：OpenAlex API key=' + ('已配置' if RUNTIME.openalex_api_key else '未配置')
                     + '；Crossref mailto=' + ('已配置' if RUNTIME.crossref_mailto else '未配置') + '。')
        lines.append('学术门禁诊断（OpenAlex/Crossref 抓取阶段）：'
                     + ' api_results=' + str(ACADEMIC_FETCH_DIAG.get('api_results', 0))
                     + ' pass=' + str(ACADEMIC_FETCH_DIAG.get('pass', 0))
                     + ' venue_not_whitelist=' + str(ACADEMIC_FETCH_DIAG.get('venue_not_whitelist', 0))
                     + ' abstract_short=' + str(ACADEMIC_FETCH_DIAG.get('abstract_short', 0))
                     + ' no_ca_anchor=' + str(ACADEMIC_FETCH_DIAG.get('no_ca_anchor', 0))
                     + ' no_doi=' + str(ACADEMIC_FETCH_DIAG.get('no_doi', 0))
                     + ' errors=' + str(ACADEMIC_FETCH_DIAG.get('errors', 0))
                     + ' api_429_retry=' + str(ACADEMIC_FETCH_DIAG.get('api_429_retry', 0))
                     + ' api_success_after_429=' + str(ACADEMIC_FETCH_DIAG.get('api_success_after_429', 0))
                     + ' api_final_429=' + str(ACADEMIC_FETCH_DIAG.get('api_final_429', 0)))
        lines.append('学术时效窗口：近 ' + str(ACADEMIC_LOOKBACK_DAYS) + ' 天；白名单刊定向拉取已启用。')
    if TEST_CANDIDATE_SOURCES:
        lines.append('候选源失败记录数：' + str(warning_count({'CANDIDATE_WEB'})))
    if extra_source_jobs:
        lines.append('专项补强源失败记录数：' + str(warning_count(SPECIAL_SOURCE_WARNING_KINDS)))
    lines.append('')
    if skipped_feeds:
        lines.append('已跳过的 RSS 源：')
        for source in sorted(skipped_feeds):
            lines.append('- ' + source)
        lines.append('')
    if skipped_web_sources:
        lines.append('稳定模式已跳过的网页源：')
        for source in sorted(skipped_web_sources):
            lines.append('- ' + source)
        lines.append('')
    discovery_only = sorted(set(DISCOVERY_ONLY_SOURCES) & (set(WEB_SOURCES) | set(PDF_REPORT_SOURCES) | set(CANDIDATE_WEB_SOURCES)))
    if discovery_only:
        lines.append('反爬适配：以下来源改为 discovery-only，仅通过定向发现查询，不硬抓列表页：')
        for source in discovery_only:
            lines.append('- ' + source)
        lines.append('')
    if TEST_CANDIDATE_SOURCES and candidate_web_jobs:
        lines.append('候选网页源本次返回详情：')
        for source, count in sorted(candidate_web_jobs):
            lines.append('- ' + source + ': ' + str(count))
        lines.append('')
    for group in extra_source_jobs:
        if not group['jobs']:
            continue
        lines.append(group['label'] + '本次返回详情：')
        for source, count in sorted(group['jobs']):
            lines.append('- ' + source + ': ' + str(count))
        lines.append('')
    main_warnings = [warning for warning in SOURCE_WARNINGS if warning['kind'] in {'RSS', 'WEB'}]
    candidate_warnings = [warning for warning in SOURCE_WARNINGS if warning['kind'] == 'CANDIDATE_WEB']
    extra_warnings = [warning for warning in SOURCE_WARNINGS if warning['kind'] in SPECIAL_SOURCE_WARNING_KINDS]
    if main_warnings:
        lines.append('主源失败详情：')
        for warning in main_warnings:
            lines.append('- [' + warning['kind'] + '] ' + warning['source'] + ' | ' + warning['url'] + ' | ' + warning['error'])
        lines.append('')
    if candidate_warnings:
        lines.append('候选源失败详情：')
        for warning in candidate_warnings:
            lines.append('- [' + warning['kind'] + '] ' + warning['source'] + ' | ' + warning['url'] + ' | ' + warning['error'])
        lines.append('')
    if extra_warnings:
        lines.append('专项补强源失败详情：')
        for warning in extra_warnings:
            lines.append('- [' + warning['kind'] + '] ' + warning['source'] + ' | ' + warning['url'] + ' | ' + warning['error'])
    all_warnings = main_warnings + candidate_warnings + extra_warnings
    if all_warnings:
        error_counts = {}
        for warning in all_warnings:
            category = classify_access_error(warning.get('error', ''))
            error_counts[category] = error_counts.get(category, 0) + 1
        lines.append('访问失败分类：' + '；'.join(
            category + '=' + str(count)
            for category, count in sorted(error_counts.items(), key=lambda pair: (-pair[1], pair[0]))
        ))
    atomic_write_text(SOURCE_HEALTH_LOG, '\n'.join(lines))


def unique_item_count(items):
    return len({normalize_key(item) for item in items or [] if normalize_key(item)})


def distinct_source_count(items):
    return len({public_source_key(item) for item in items or []})


def canonical_source_distribution(items, limit=12):
    counts = {}
    for item in items or []:
        key = public_source_key(item)
        counts[key] = counts.get(key, 0) + 1
    ranked = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))[:limit]
    return '；'.join(key.split(':', 1)[-1] + '=' + str(count) for key, count in ranked)


def append_selection_funnel_to_health_log(
    all_items,
    relevant_items,
    history_deduped_items,
    public_eligible_items,
    internal_review_items,
    published_items,
    cross_day_skipped,
    same_run_skipped,
    low_value_count,
    low_research_count,
):
    try:
        lines = SOURCE_HEALTH_LOG.read_text(encoding='utf-8').splitlines() if SOURCE_HEALTH_LOG.exists() else []
        lines.append('')
        lines.append('筛选漏斗（以下才可用于解释“抓取很多、采用很少”）：')
        lines.append(
            '- 原始抓取：' + str(len(all_items)) + ' 条记录 / ' + str(unique_item_count(all_items))
            + ' 个链接或标题 / ' + str(distinct_source_count(all_items)) + ' 个规范化出版方'
        )
        lines.append(
            '- 中亚相关与基础信号通过：' + str(len(relevant_items)) + ' 条 / '
            + str(distinct_source_count(relevant_items)) + ' 个规范化出版方'
        )
        lines.append(
            '- 去重后研究候选：' + str(len(history_deduped_items)) + ' 条 / '
            + str(distinct_source_count(history_deduped_items)) + ' 个规范化出版方'
            + '（跨日排除 ' + str(cross_day_skipped) + '，同轮排除 ' + str(same_run_skipped) + '）'
        )
        lines.append(
            '- 公开门禁通过：' + str(len(public_eligible_items)) + ' 条 / '
            + str(distinct_source_count(public_eligible_items)) + ' 个规范化出版方'
        )
        lines.append(
            '- 内部备查：' + str(len(internal_review_items)) + ' 条；低价值排除 '
            + str(low_value_count) + ' 条；研究性或深度不足排除 ' + str(low_research_count) + ' 条'
        )
        lines.append(
            '- 最终公开采用：' + str(published_item_count(published_items)) + ' 条 / '
            + str(distinct_source_count(published_items)) + ' 个规范化出版方'
        )
        distribution = canonical_source_distribution(published_items)
        if distribution:
            lines.append('- 最终来源分布：' + distribution)
        atomic_write_text(SOURCE_HEALTH_LOG, chr(10).join(lines) + chr(10))
    except Exception:
        pass

def fetch_feed(url, source_name, warning_kind='RSS'):
    results = []
    try:
        resp = request_url(url, timeout=12, retries=1)
        resp.raise_for_status()
        d = feedparser.parse(resp.text)
        for e in d.entries[:100]:
            link = e.get('link', '')
            link = link.replace('https://24.kg./', 'https://24.kg/')
            title = clean_text(e.get('title', ''))
            summary = clean_text(e.get('summary', e.get('description', '')))
            published = e.get('published', e.get('updated', ''))
            if not title or not link:
                continue
            item_source = source_name
            publisher_home = ''
            publisher_text = ''
            publisher = ''
            neighbor_region = ''
            if warning_kind == 'DEEP_DISCOVERY':
                source_meta = e.get('source') or {}
                publisher = clean_text(source_meta.get('title', ''))
                publisher_home = clean_text(source_meta.get('href', ''))
                publisher_text = publisher.lower()
                item_text = (publisher + ' ' + title + ' ' + summary).lower()
                if any(term in item_text for term in DEEP_DISCOVERY_EXCLUDED_LOWER):
                    continue
                if not any(term in publisher_text for term in DEEP_DISCOVERY_TRUSTED_LOWER):
                    continue
                if publisher:
                    item_source = source_name + '｜' + publisher
                neighbor_region = neighboring_perspective_region(publisher_text)
            metadata = {'content_type': '', 'word_count': 0}
            if source_name in LOCAL_POLICY_INSTITUTE_SOURCES:
                metadata = fetch_article_metadata(link)
                if metadata.get('published'):
                    published = metadata['published'].isoformat()
                if metadata.get('summary') and len(metadata['summary']) > len(summary):
                    summary = metadata['summary']
            results.append({
                'source': item_source, 'title': title, 'link': link,
                'summary': summary[:500], 'published': published,
                'content_type': metadata.get('content_type', ''),
                'word_count': metadata.get('word_count', 0),
                'source_type': (
                    'institution_publication'
                    if warning_kind == 'DEEP_DISCOVERY' and neighbor_region
                    else (
                        'top_tier_media_discovery'
                        if warning_kind == 'DEEP_DISCOVERY'
                        and any(name in publisher_text for name in TOP_TIER_MEDIA_PUBLISHERS)
                        else ('discovery' if warning_kind == 'DEEP_DISCOVERY' else 'feed')
                    )
                ),
                'source_tier': (
                    neighbor_institution_tier(publisher_text)
                    if warning_kind == 'DEEP_DISCOVERY' and neighbor_region
                    else (
                        1 if warning_kind == 'DEEP_DISCOVERY'
                        and any(name in publisher_text for name in TOP_TIER_MEDIA_PUBLISHERS)
                        else (2 if warning_kind == 'DEEP_DISCOVERY' else 3)
                    )
                ),
                'publisher': publisher,
                'publisher_home': publisher_home,
                'institution': publisher if neighbor_region else '',
                'institution_publication_kind': 'research_analysis' if neighbor_region else '',
                'perspective_region': neighbor_region,
                'id': e.get('id', link + title),
            })
    except Exception as exc:
        record_source_warning(warning_kind, source_name, url, exc)
    return results

def fetch_article_metadata(link, response_fetcher=None):
    metadata = {
        'title': '', 'summary': '', 'body_summary': '', 'published': None,
        'content_type': '', 'word_count': 0, 'access_status': 'unknown',
    }
    try:
        fetcher = response_fetcher or request_url
        resp = fetcher(link, timeout=10, retries=0)
        resp.raise_for_status()
        content_type_header = (resp.headers.get('content-type', '') or '').lower()
        if 'application/pdf' in content_type_header or resp.content[:4] == b'%PDF':
            pdf_text = clean_text(extract_pdf_text(resp.content))
            if pdf_text:
                metadata['summary'] = excerpt(pdf_text, 500)
                metadata['body_summary'] = metadata['summary']
                metadata['content_type'] = 'PDF report'
                metadata['word_count'] = len(re.findall(r'\b[\w-]+\b', pdf_text, re.UNICODE))
                metadata['access_status'] = 'open'
            return metadata
        soup = BeautifulSoup(resp.text, 'lxml')
        page_surface = clean_text(soup.get_text(' ', strip=True)).lower()
        paywall_terms = [
            'subscribe to read', 'subscribe to continue', 'sign in to continue',
            'log in to continue', 'members only', 'full access', 'premium content',
            'continue reading', '订阅后阅读', '登录后阅读', '付费阅读',
        ]
        metadata['access_status'] = 'paywalled' if any(term in page_surface for term in paywall_terms) else 'open'
        date_value = None
        for selector in [
            'meta[property="article:published_time"]', 'meta[name="date"]',
            'meta[name="pubdate"]', 'meta[name="citation_publication_date"]',
            'meta[itemprop="datePublished"]', 'time[datetime]'
        ]:
            el = soup.select_one(selector)
            if not el:
                continue
            raw_date = el.get('content') or el.get('datetime') or el.get_text(' ', strip=True)
            date_value = parse_date_text(raw_date)
            if date_value:
                break
        if not date_value:
            for script in soup.select('script[type="application/ld+json"]'):
                try:
                    payload = json.loads(script.get_text(strip=True))
                    payloads = payload if isinstance(payload, list) else [payload]
                    for record in payloads:
                        if not isinstance(record, dict):
                            continue
                        raw_date = record.get('datePublished') or record.get('dateCreated') or record.get('dateModified')
                        date_value = parse_date_text(raw_date or '')
                        if date_value:
                            break
                    if date_value:
                        break
                except Exception:
                    continue
        page_title = ''
        if soup.title and soup.title.get_text():
            page_title = clean_text(soup.title.get_text(' ', strip=True))
        og_title_el = soup.select_one('meta[property="og:title"]')
        if og_title_el and og_title_el.get('content'):
            page_title = clean_text(og_title_el.get('content')) or page_title
        metadata['title'] = page_title[:240]
        meta_summary = ''
        for selector in ['meta[name="description"]', 'meta[property="og:description"]']:
            el = soup.select_one(selector)
            if el and el.get('content'):
                meta_summary = clean_text(el.get('content'))
                break
        content_type_parts = []
        for selector in [
            'meta[property="article:section"]', 'meta[name="parsely-section"]',
            'meta[name="type"]', 'meta[property="og:type"]',
            'meta[name="citation_keywords"]', '.content-type',
            '.publication-type', '.article-type', '.field--name-field-content-type',
        ]:
            el = soup.select_one(selector)
            if not el:
                continue
            value = el.get('content') or el.get_text(' ', strip=True)
            value = clean_text(value)
            if value and value.lower() not in [part.lower() for part in content_type_parts]:
                content_type_parts.append(value)
        paragraphs = [
            clean_text(p.get_text(' ', strip=True))
            for p in soup.select('article p, main p, .entry-content p, .post-content p, p')[:80]
        ]
        paragraph_boilerplate = [
            'share this via', 'more sharing options', 'sign up for updates',
            'subscribe to our newsletter', 'follow us on',
        ]
        paragraphs = [
            p for p in paragraphs
            if len(p) >= 60
            and not any(term in p.lower() for term in paragraph_boilerplate)
            and not (
                len(p) < 220
                and any(term in p.lower() for term in [
                    ' pictured ', ' photo of ', ' holding her ', ' holding his ',
                    ' seen in ', ' speaks during ', ' appears ', 'appears onscreen',
                    ' stands ', ' sits ', ' walks ', 'people walk', ' walk past ',
                    '©',
                ])
            )
        ]
        body_summary = ' '.join(paragraphs[:2])
        body_text = ' '.join(paragraphs)
        extracted_text = clean_text(extract_main_text(resp.text, link))
        if len(extracted_text) >= max(600, len(body_text) + 120):
            body_text = extracted_text
            body_summary = excerpt(extracted_text, 500)
        metadata['body_summary'] = clean_text(body_summary)[:500]
        summary = meta_summary
        # Prefer body excerpt when meta description is missing, too short, or off-topic site boilerplate.
        if body_summary:
            caption_like_summary = (
                len(summary) < 220
                and any(term in summary.lower() for term in [
                    ' pictured ', ' photo of ', ' holding her ', ' holding his ',
                    ' seen in ', ' speaks during ', ' appears ', 'appears onscreen',
                    ' stands ', ' sits ', ' walks ', 'people walk', ' walk past ',
                    '©',
                ])
            )
            if not summary or len(summary) < 80 or caption_like_summary:
                summary = body_summary
            else:
                title_tokens = set(re.findall(r'[a-zA-Z\u4e00-\u9fff]{4,}', (page_title or '').lower()))
                summary_tokens = set(re.findall(r'[a-zA-Z\u4e00-\u9fff]{4,}', summary.lower()))
                overlap = len(title_tokens & summary_tokens)
                if title_tokens and overlap == 0:
                    summary = body_summary
        metadata['summary'] = clean_text(summary)[:500]
        if not date_value:
            date_patterns = [
                r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b',
                r'\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b',
            ]
            for pattern in date_patterns:
                match = re.search(pattern, page_surface, flags=re.I)
                if match:
                    date_value = parse_date_text(match.group(0))
                    if date_value:
                        break
        metadata['published'] = date_value
        metadata['content_type'] = ' | '.join(content_type_parts)[:240]
        metadata['word_count'] = len(re.findall(r'\b[\w-]+\b', body_text, re.UNICODE))
        if metadata['access_status'] == 'paywalled' and metadata['word_count'] >= 500:
            metadata['access_status'] = 'open'
    except Exception:
        pass
    return metadata

SOURCE_WEB_SELECTORS = {
    'CSIS Search Central Asia': ['a[href*="/analysis/"]'],
    'Ifri Central Asia': ['a[href*="/papers/"]'],
}

SOURCE_WEB_REQUIRED_PATHS = {
    'CSIS Search Central Asia': ['/analysis/'],
    'Ifri Central Asia': ['/papers/'],
}

def fetch_web(source_name, url, warning_kind='WEB'):
    results = []
    try:
        resp = request_url(url, timeout=12, retries=1)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')
        selectors = SOURCE_WEB_SELECTORS.get(source_name, [
            'article a', '.post a', '.entry a', '.views-row a', '.card a',
            '.teaser a', '.news a', 'h1 a', 'h2 a', 'h3 a',
            'a[href*="/news/"]', 'a[href*="/articles/"]', 'a[href*="/analysis/"]',
        ])
        seen_links = set()
        for sel in selectors:
            for el in soup.select(sel)[:12]:
                href = el.get('href', '')
                txt = clean_web_title(el.get_text(' ', strip=True))
                if not href or not txt or len(txt) < 8:
                    continue
                if is_generic_title(txt):
                    continue
                if not href.startswith('http'):
                    href = urljoin(url, href)
                required_paths = SOURCE_WEB_REQUIRED_PATHS.get(source_name, [])
                if required_paths and not any(path in href.lower() for path in required_paths):
                    continue
                if is_generic_url(href):
                    continue
                normalized = href.split('#')[0]
                if normalized in seen_links:
                    continue
                seen_links.add(normalized)
                parent_text = clean_text(el.parent.get_text(' ', strip=True) if el.parent else '')
                item_date = infer_date_from_context(txt, href, parent_text[:400])
                summary = ''
                if parent_text and parent_text != txt:
                    summary = parent_text.replace(txt, ' ', 1)
                    summary = clean_text(summary)[:500]
                metadata = {'content_type': '', 'word_count': 0, 'access_status': 'unknown'}
                title_lowered = txt.lower()
                needs_prestige_metadata = (
                    source_name in PRESTIGE_LONGFORM_SOURCES
                    and not any(term in title_lowered for term in STRICT_DEEP_FORMAT_LOWER)
                )
                needs_official_metadata = (
                    source_name in OFFICIAL_POLICY_SOURCES
                    and any(term in title_lowered for term in SUBSTANTIVE_POLICY_DOCUMENT_LOWER)
                )
                metadata_source = (
                    source_name in (DEEP_ANALYSIS_SOURCES | set(PDF_REPORT_SOURCES) | set(MEETING_MINUTES_SOURCES))
                    or source_name in DURABLE_PRESTIGE_DISCOVERY_SOURCES
                    or needs_official_metadata
                )
                if metadata_source and (
                    not item_date or len(summary) < 120 or needs_prestige_metadata
                ):
                    metadata = fetch_article_metadata(href)
                    metadata_title = clean_web_title(metadata.get('title', ''))
                    if metadata_title and not is_generic_title(metadata_title):
                        txt = metadata_title
                    if metadata.get('published'):
                        item_date = metadata['published']
                    if metadata.get('summary') and len(metadata['summary']) > len(summary):
                        summary = metadata['summary']
                results.append({
                    'source': source_name, 'title': txt[:150], 'link': href,
                    'summary': summary, 'published': item_date.isoformat() if item_date else '',
                    'content_type': metadata.get('content_type', ''),
                    'word_count': metadata.get('word_count', 0),
                    'access_status': metadata.get('access_status', 'unknown'),
                    'source_type': (
                        'institution_publication'
                        if source_name in DURABLE_PRESTIGE_DISCOVERY_SOURCES
                        else 'web_discovery'
                    ),
                    'institution': source_name.replace(' Search Central Asia', '').replace(' Central Asia', ''),
                    'institution_publication_kind': 'research_analysis',
                    'source_tier': 1 if source_name in TIER_ONE_PRESTIGE_DISCOVERY_SOURCES else 2,
                    'id': source_name + ':' + href,
                })
                if len(results) >= 8:
                    break
            if len(results) >= 8:
                break
    except Exception as exc:
        record_source_warning(warning_kind, source_name, url, exc)
    return results

def fetch_telegram(source_name, url, warning_kind='TELEGRAM'):
    results = []
    try:
        resp = request_url(url, timeout=15, retries=1)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')
        messages = soup.select('.tgme_widget_message_wrap, .tgme_widget_message')
        seen_links = set()
        for msg in messages[-18:]:
            text_el = msg.select_one('.tgme_widget_message_text')
            text = clean_text(text_el.get_text(' ', strip=True) if text_el else '')
            if not text or len(text) < 20:
                continue
            date_el = msg.select_one('a.tgme_widget_message_date')
            link = date_el.get('href', '') if date_el else ''
            time_el = msg.select_one('time')
            published = ''
            if time_el:
                published = time_el.get('datetime') or time_el.get('title') or ''
            if not link:
                link = url
            normalized = normalize_history_link(link)
            if normalized in seen_links:
                continue
            seen_links.add(normalized)
            title = text.split('。')[0].split('.')[0].split('\n')[0]
            title = trim_text(title, 140)
            if is_generic_title(title):
                title = trim_text(text, 140)
            results.append({
                'source': source_name,
                'title': title,
                'link': link,
                'summary': text[:500],
                'published': published,
                'id': source_name + ':' + link + ':' + title,
            })
            if len(results) >= 10:
                break
    except Exception as exc:
        record_source_warning(warning_kind, source_name, url, exc)
    return results

REPORT_LINK_TERMS = [
    'report', 'publication', 'outlook', 'brief', 'working paper',
    'policy', 'analysis', 'study', 'pdf', 'download',
    '报告', '出版物', '展望', '简报', '研究', '白皮书',
    'rapor', 'raporu', 'yayın', 'araştırma', 'araştırması', 'analiz', 'analizi', 'inceleme',
    'گزارش', 'پژوهش', 'تحلیل', 'بررسی',
]
REPORT_LINK_LOWER = [term.lower() for term in REPORT_LINK_TERMS]

REPORT_CLASSIFICATION_TERMS = [
    'report', 'special report', 'policy brief', 'working paper',
    'white paper', 'discussion paper', 'research note', 'research brief',
    'briefing paper', 'study', 'publication', 'monograph',
    '报告', '专项报告', '政策简报', '工作论文', '白皮书', '讨论稿', '研究简报', '研究报告',
    'rapor', 'raporu', 'politika notu', 'araştırma', 'araştırması', 'inceleme', 'yayın',
    'گزارش', 'یادداشت سیاستی', 'پژوهش', 'بررسی',
]
REPORT_CLASSIFICATION_LOWER = [term.lower() for term in REPORT_CLASSIFICATION_TERMS]

STRICT_DEEP_FORMAT_TERMS = [
    'analysis', 'news analysis', 'expert views', 'expert view', 'commentary',
    'in-depth', 'deep dive', 'long read', 'longread',
    'investigation', 'investigative', 'policy brief', 'working paper',
    'research report', 'special report', 'white paper', 'discussion paper',
    'research note', 'briefing paper', 'study', 'assessment', 'outlook',
    '分析', '评论', '深度', '调查报道', '政策简报', '工作论文',
    '研究报告', '专题报告', '白皮书', '讨论报告', '评估报告',
    'анализ', 'исследование', 'доклад', 'аналитический обзор',
    'analiz', 'analizi', 'araştırma', 'araştırması', 'rapor', 'raporu', 'değerlendirme', 'politika notu', 'inceleme',
    'تحلیل', 'پژوهش', 'گزارش', 'ارزیابی', 'یادداشت سیاستی', 'بررسی',
]
STRICT_DEEP_FORMAT_LOWER = [term.lower() for term in STRICT_DEEP_FORMAT_TERMS]

THIN_ANALYTICAL_NEWS_TITLE_TERMS = [
    'makes progress', 'holds dialogue', 'held dialogue', 'calls for',
    'signed an agreement', 'signs deal', 'signs agreement', 'signs protocol',
    'officials hold', 'hold talks', 'holds talks', 'discusses closer',
    'tour marks', 'marks the', 'marks an emerging', 'marks a new',
    'will launch', 'to launch satellite', 'launches satellite',
    'enterprise advances', 'joint venture', 'ambassador urges',
    'ambassador calls', 'urges us-', 'urges u.s.',
    'round of talks', 'bilateral meeting', 'press release',
    '取得进展', '举行对话', '签署协议', '签署谅解备忘录', '呼吁',
    '巡演', '巡回访问', '企业取得进展', '卫星星座',
]
THIN_ANALYTICAL_NEWS_TITLE_LOWER = [term.lower() for term in THIN_ANALYTICAL_NEWS_TITLE_TERMS]

# Media-style publishers discovered via Google News need explicit deep framing.
MEDIA_STYLE_DEEP_PUBLISHERS = [
    'the times of central asia', 'times of central asia', 'eurasianet',
    'the diplomat', 'bne', 'intellinews', 'the astana times',
    'cabar', 'novastan', 'radio free europe', 'rfe/rl',
    'reuters', 'financial times', 'the economist', 'new york times',
    'foreign affairs', 'foreign policy', 'nikkei asia', 'bloomberg',
    'the guardian', 'washington post', 'wall street journal', 'al jazeera',
    'anadolu', 'tehran times', 'irna', 'dawn', 'tolonews',
    'trend news agency', 'azernews',
]
MEDIA_STYLE_DEEP_PUBLISHERS_LOWER = [term.lower() for term in MEDIA_STYLE_DEEP_PUBLISHERS]

TOP_TIER_MEDIA_PUBLISHERS = {
    'reuters', 'financial times', 'the economist', 'new york times',
    'foreign affairs', 'foreign policy', 'nikkei asia', 'bloomberg',
    'the guardian', 'washington post', 'wall street journal', 'al jazeera',
}

def is_top_tier_media_item(item):
    if item.get('source_type') == 'institution_publication':
        return False
    source = clean_text(item.get('source', '')).lower()
    publisher = clean_text(item.get('publisher', '')).lower()
    if is_deep_discovery_source(item.get('source', '')):
        publisher = deep_discovery_publisher(item.get('source', ''))
    surface = source + ' ' + publisher
    return any(name in surface for name in TOP_TIER_MEDIA_PUBLISHERS)

SUBSTANTIVE_POLICY_DOCUMENT_TERMS = [
    'strategy', 'concept', 'law', 'code', 'regulation', 'decree',
    'national report', 'annual report', 'statistical bulletin', 'dataset',
    'survey results', 'development plan', 'action plan', 'communiqué',
    'communique', 'joint statement', 'official data',
    '战略', '规划', '法典', '法律', '条例', '法令', '国家报告',
    '年度报告', '统计公报', '数据集', '调查结果', '行动计划', '联合声明',
    'стратегия', 'концепция', 'закон', 'кодекс', 'доклад',
]
SUBSTANTIVE_POLICY_DOCUMENT_LOWER = [term.lower() for term in SUBSTANTIVE_POLICY_DOCUMENT_TERMS]

OFFICIAL_ACTIVITY_TITLE_TERMS = [
    'meeting', 'met with', 'receives', 'received', 'visit', 'visited',
    'consultations', 'participated', 'participates', 'held talks',
    'forum', 'conference', 'delegation', 'congratulated',
    '会见', '接见', '访问', '出访', '磋商', '参加', '出席', '举行会议',
    '代表团', '论坛', '会议', '祝贺',
]
OFFICIAL_ACTIVITY_TITLE_LOWER = [term.lower() for term in OFFICIAL_ACTIVITY_TITLE_TERMS]
def fetch_pdf_reports(source_name, url, warning_kind='PDF_REPORT'):
    results = []
    try:
        seen_links = set()
        adapter = INSTITUTION_ADAPTER_CONFIG.get(source_name, {})
        adapter_selectors = adapter.get('selectors') or ['a[href]']
        adapter_paths = adapter.get('allowed_paths') or []
        adapter_excludes = [term.lower() for term in adapter.get('exclude_terms', [])]
        page_urls = [url] + [u for u in adapter.get('alternate_urls', []) if u and u != url]
        # Publication sitemaps often remain available when the visual archive
        # is JS-rendered or protected by a bot challenge. Treat them as a
        # discovery index, then verify each candidate through its landing page.
        sitemap_candidates = []
        for sitemap_url in adapter.get('sitemap_urls', []):
            try:
                sm_resp = request_url(sitemap_url, timeout=15, retries=0)
                if sm_resp.status_code >= 400:
                    continue
                content_type = (sm_resp.headers.get('content-type', '') or '').lower()
                if 'xml' not in content_type and '<urlset' not in sm_resp.text[:500].lower() and '<sitemapindex' not in sm_resp.text[:500].lower():
                    continue
                sm_soup = BeautifulSoup(sm_resp.text, features='xml')
                for loc in sm_soup.select('url > loc, sitemap > loc')[:400]:
                    candidate = clean_text(loc.get_text(' ', strip=True))
                    if candidate.startswith('http'):
                        sitemap_candidates.append(candidate)
            except Exception:
                continue
        if sitemap_candidates:
            page_urls.extend(sitemap_candidates[:12])
        visited_pages = set()
        exclude_path_terms = [
            'event', 'events', 'agenda', 'calendar', 'webinar', 'newsletter',
            'subscribe', 'contact', 'about', 'donation', 'career', 'job',
            'search', 'login', 'account', 'support-us', 'tags',
            'nuclear-sharing', 'press-release',
        ]
        required_path_terms = {
            'Ifri Papers Central Asia': ['/papers/'],
        }.get(source_name, [])
        archive_mode = source_name in INSTITUTION_SOURCE_REGISTRY or adapter.get('durable_archive')
        max_pages = (6 if archive_mode else 3) + min(12, len(sitemap_candidates))
        while page_urls and len(visited_pages) < max_pages and len(results) < 10:
            page_url = page_urls.pop(0)
            normalized_page = normalize_history_link(page_url)
            if normalized_page in visited_pages:
                continue
            visited_pages.add(normalized_page)
            resp = request_url(page_url, timeout=18, retries=1)
            resp.raise_for_status()
            page_content_type = (resp.headers.get('content-type', '') or '').lower()
            page_parser = 'xml' if 'xml' in page_content_type or resp.text.lstrip().startswith('<?xml') else 'lxml'
            soup = BeautifulSoup(resp.text, features=page_parser)

            # Follow only a small number of same-site pagination links so a
            # publication archive cannot turn one source into an unbounded crawl.
            for next_el in soup.select('a[rel~="next"], a.next, .pagination a[href], a[href*="page="]'):
                next_href = next_el.get('href', '')
                if not next_href:
                    continue
                next_url = urljoin(page_url, next_href)
                if urllib.parse.urlparse(next_url).netloc != urllib.parse.urlparse(url).netloc:
                    continue
                if normalize_history_link(next_url) not in visited_pages and next_url not in page_urls:
                    page_urls.append(next_url)
                if len(page_urls) + len(visited_pages) >= max_pages:
                    break

            link_elements = []
            for selector in adapter_selectors:
                link_elements.extend(soup.select(selector)[:80])
            seen_elements = set()
            for el in link_elements[:400]:
                if id(el) in seen_elements:
                    continue
                seen_elements.add(id(el))
                href = el.get('href', '')
                title = clean_web_title(el.get_text(' ', strip=True))
                title = re.sub(r'^(?:read more about|learn more about)\s+', '', title, flags=re.I).strip()
                if not href:
                    continue
                link = urljoin(page_url, href)
                path = urllib.parse.urlparse(link).path.lower()
                if adapter_paths and not any(term in path for term in adapter_paths):
                    continue
                if any(term in path for term in exclude_path_terms):
                    continue
                if required_path_terms and not any(term in path for term in required_path_terms):
                    continue
                parent_text = clean_text(el.parent.get_text(' ', strip=True) if el.parent else '')
                card = el.find_parent(['article', 'li'])
                card_text = clean_text(card.get_text(' ', strip=True)) if card else ''
                # Institution pages often have large navigation blocks in the
                # parent node. Prefer the article card to avoid false CA hits.
                context_parts = [card_text] if source_name in INSTITUTION_SOURCE_REGISTRY else [parent_text, card_text]
                context_text = trim_text(' '.join(part for part in context_parts if part), 700)
                lowered = (title + ' ' + link + ' ' + context_text).lower()
                if adapter_excludes and any(term in lowered for term in adapter_excludes):
                    continue
                is_pdf = '.pdf' in link.lower()
                looks_report = is_pdf or any(term in lowered for term in REPORT_LINK_LOWER)
                if (
                    source_name in INSTITUTION_SOURCE_REGISTRY
                    and len(title) >= 20
                    and not is_generic_title(title)
                    and any(path in path for path in [
                        '/article', '/articles/', '/analysis/', '/research/', '/publication',
                        '/policy-', '/policy_memo', '/policy-memos/', '/insights/', '/politika/',
                    ])
                ):
                    looks_report = True
                if not looks_report:
                    continue
                if not title or is_generic_title(title):
                    title = Path(urllib.parse.urlparse(link).path).name.replace('-', ' ').replace('_', ' ')
                    title = clean_web_title(title)
                if not title or len(title) < 10 or is_generic_title(title):
                    continue
                # Reject pure calendar/event titles with clock-like fragments.
                if re.search(r'\b\d{1,2}:\d{2}\b', title) or 'contested policy endures' in title.lower():
                    continue
                # Require Central Asia relevance in title/link/context, never via source-name alone.
                relevance = (title + ' ' + link + ' ' + context_text).lower()
                if source_name == 'Davis Center Central Asia Publications':
                    davis_title_surface = (title + ' ' + link).lower()
                    if not any(term in davis_title_surface for term in STRONG_CA_ANCHOR_LOWER):
                        continue
                    if any(term in davis_title_surface for term in [
                        'book review', 'book-review', 'review of', 'journal issue',
                        'spring 2026', 'fall 2026', 'winter 2026', 'summer 2026',
                        'newsletter', 'podcast', 'event', 'webinar', 'course',
                        'memoir', 'obituary',
                    ]):
                        continue
                if not any(term in relevance for term in STRONG_CA_ANCHOR_LOWER):
                    continue
                item_date = infer_date_from_context(title, link, context_text)
                metadata = {'title': '', 'summary': '', 'published': None, 'content_type': '', 'word_count': 0, 'access_status': 'unknown'}
                needs_title_cleanup = len(title) > 140 or any(term in title.lower() for term in [' michael ', ' wim ', ' author:', 'article '])
                if not is_pdf and (not item_date or needs_title_cleanup):
                    metadata = fetch_article_metadata(link)
                    if metadata.get('published'):
                        item_date = metadata['published']
                    metadata_title = clean_web_title(metadata.get('title', ''))
                    if metadata_title and not is_generic_title(metadata_title):
                        title = metadata_title
                if not item_date:
                    continue
                age_days = (TODAY - item_date).days
                # Future-dated calendar leftovers are not research reports.
                max_age = DURABLE_RESEARCH_MAX_AGE_DAYS if archive_mode else MAX_SLOW_PUBLICATION_AGE_DAYS
                if age_days < -1 or age_days > max_age:
                    continue
                normalized = normalize_history_link(link)
                if normalized in seen_links:
                    continue
                seen_links.add(normalized)
                # Do not put source names containing "Central Asia" into summary,
                # or has_strong_central_asia_anchor will false-positive later.
                snippet = trim_text(context_text, 280) if context_text else ''
                if metadata.get('summary') and len(metadata['summary']) > len(snippet):
                    snippet = metadata['summary']
                if not snippet or len(snippet) < 40:
                    snippet = 'Institutional report/publication candidate with identifiable date.'
                registry = institution_source_metadata(source_name)
                results.append({
                    'source': source_name,
                    'title': title[:180],
                    'link': link,
                    'summary': snippet,
                    'published': item_date.isoformat(),
                    'content_type': metadata.get('content_type', ''),
                    'word_count': metadata.get('word_count', 0),
                    'access_status': metadata.get('access_status', 'unknown'),
                    'source_type': 'institution_publication',
                    'institution': registry['institution'],
                    'institution_publication_kind': registry['kind'],
                    'source_tier': registry['tier'],
                    'access_status': metadata.get('access_status', 'unknown'),
                    'id': source_name + ':' + link,
                })
                if len(results) >= 10:
                    break
    except Exception as exc:
        record_source_warning(warning_kind, source_name, url, exc)
    return results

def explicit_country_assessment_year(record, metadata):
    configured_year = record.get('edition_year')
    surface = ' '.join([
        clean_text(metadata.get('title', '')),
        clean_text(metadata.get('summary', ''))[:220],
        clean_text(record.get('url', '')),
    ])
    years = [
        int(value) for value in re.findall(r'(?<!\d)(20\d{2})(?!\d)', surface)
        if int(value) <= TODAY.year + 1
    ]
    if configured_year and int(configured_year) in years:
        return int(configured_year)
    if years:
        return max(years)
    return int(configured_year) if configured_year else None

def fetch_country_assessment(record, warning_kind='COUNTRY_ASSESSMENT'):
    """Fetch one first-party national assessment page with edition-aware metadata."""
    source_name = record['source']
    url = record['url']
    provider = COUNTRY_ASSESSMENT_PROVIDERS[source_name]
    try:
        fetcher = request_url_with_urllib_fallback if provider.get('urllib_fallback') else request_url
        metadata = fetch_article_metadata(url, response_fetcher=fetcher)
        title = clean_web_title(metadata.get('title', ''))
        title = re.sub(r'\s*\|\s*Freedom House\s*$', '', title, flags=re.I).strip()
        if not title:
            record_year = record.get('edition_year') or ''
            title = (
                (str(record_year) + ' ' if record_year else '')
                + record['name'] + ' Country Assessment'
            ).strip()
        summary = clean_text(metadata.get('summary', '')).replace('\ufffd', '').strip()
        body_summary = clean_text(metadata.get('body_summary', '')).replace('\ufffd', '').strip()
        if len(body_summary) >= 160:
            summary = body_summary
        if source_name == 'Human Rights Watch Central Asia Country Chapters':
            sentences = re.split(r'(?<=[.!?])\s+', summary)
            if (
                len(sentences) >= 2
                and len(sentences[0]) < 190
                and any(term in sentences[0].lower() for term in [
                    ' appears ', ' appears onscreen', ' holding ', ' pictured ',
                    ' stands ', ' sits ', ' speaks ', ' walks ', 'people walk',
                ])
            ):
                summary = ' '.join(sentences[1:])
        if len(summary) < 60:
            raise ValueError('country assessment page has no usable summary')
        publication_year = explicit_country_assessment_year(record, metadata)
        metadata_date = metadata.get('published')
        if not publication_year and metadata_date:
            publication_year = metadata_date.year
        if not publication_year:
            raise ValueError('country assessment edition year unavailable')

        # Stable report pages sometimes retain the launch date of an older site
        # record after the report edition itself has changed. Preserve the
        # verified edition year and never display an invented day/month.
        exact_date = (
            metadata_date
            if metadata_date
            and metadata_date.year == publication_year
            and metadata_date <= TODAY + datetime.timedelta(days=1)
            else None
        )
        published = exact_date.isoformat() if exact_date else ''
        edition_id = str(publication_year) + ':' + record['bti_code']
        return [{
            'source': source_name,
            'title': title[:180],
            'link': url,
            'summary': summary[:500],
            'published': published,
            'publication_year': publication_year,
            'date_precision': 'day' if exact_date else 'year',
            'edition_id': edition_id,
            'versioned_stable_url': record.get('versioned_stable_url') is True,
            'content_type': metadata.get('content_type', '') or 'country assessment',
            'word_count': int(metadata.get('word_count', 0) or 0),
            'access_status': metadata.get('access_status', 'open') or 'open',
            'source_type': 'institution_publication',
            'institution': provider['institution'],
            'institution_publication_kind': provider['kind'],
            'source_tier': provider['tier'],
            'country_assessment': True,
            'country_scope': record['country'],
            'id': source_name + ':' + edition_id + ':' + url,
        }]
    except Exception as exc:
        record_source_warning(warning_kind, source_name, url, exc)
        return []

def world_bank_abstract(document):
    abstract = document.get('abstracts', '')
    if isinstance(abstract, dict):
        abstract = abstract.get('cdata!', '') or abstract.get('abstract', '')
    return clean_text(abstract)

def fetch_world_bank_reports(warning_kind='PDF_REPORT'):
    source_name = 'World Bank Documents & Reports'
    results = []
    seen_links = set()
    try:
        from_date = TODAY - datetime.timedelta(days=MAX_SLOW_PUBLICATION_AGE_DAYS)
        for query in WORLD_BANK_REPORT_QUERIES:
            params = {
                'format': 'json',
                'qterm': query,
                'rows': 30,
                'os': 0,
                'strdate': str(from_date),
                'enddate': str(TODAY),
                'sort': 'docdt',
                'order': 'desc',
            }
            url = 'https://search.worldbank.org/api/v3/wds?' + urllib.parse.urlencode(params)
            resp = request_url(url, timeout=20, retries=1)
            resp.raise_for_status()
            documents = resp.json().get('documents', {})
            for document in documents.values():
                title = clean_text(document.get('display_title', ''))
                document_type = clean_text(document.get('docty', '')).lower()
                abstract = world_bank_abstract(document)
                published = clean_text(document.get('docdt', ''))
                item_date = parse_date_text(published)
                language = clean_text(document.get('lang', '')).lower()
                if not title or not item_date or language not in {'english', 'en'}:
                    continue
                if document_type not in WORLD_BANK_ALLOWED_DOCUMENT_TYPES:
                    continue
                title_lowered = title.lower()
                if any(term in title_lowered for term in WORLD_BANK_EXCLUDED_TITLE_TERMS):
                    continue
                if len(abstract) < 200:
                    continue
                relevance_text = (title + ' ' + abstract).lower()
                if not any(term in relevance_text for term in STRONG_CA_ANCHOR_LOWER):
                    continue
                guid = clean_text(document.get('guid', ''))
                link = clean_text(document.get('url', ''))
                if link.startswith('http://'):
                    link = 'https://' + link[len('http://'):]
                if not link and guid:
                    link = 'https://documents.worldbank.org/curated/en/' + guid
                if not link:
                    link = clean_text(document.get('pdfurl', ''))
                normalized = normalize_history_link(link)
                if not normalized or normalized in seen_links:
                    continue
                seen_links.add(normalized)
                results.append({
                    'source': source_name,
                    'title': title[:220],
                    'link': link,
                    'summary': ' '.join(re.split(r'(?<=[.!?])\\s+', abstract)[:2])[:380],
                    'published': item_date.isoformat(),
                    'report_type': document_type,
                    'id': source_name + ':' + (guid or normalized),
                })
        results.sort(key=lambda item: parse_item_published_date(item) or datetime.date.min, reverse=True)
        return results[:12]
    except Exception as exc:
        record_source_warning(warning_kind, source_name, 'World Bank Documents API', exc)
    return results
MEETING_LINK_TERMS = [
    'meeting', 'session', 'summit', 'forum', 'council',
    'consultation', 'roundtable', 'ministerial', 'statement',
    'communiqué', 'communique', 'press release', 'conference',
    '会议', '峰会', '论坛', '部长', '理事会', '磋商', '声明', '公报',
]
MEETING_LINK_LOWER = [term.lower() for term in MEETING_LINK_TERMS]

def fetch_meeting_minutes(source_name, url, warning_kind='MEETING'):
    results = []
    try:
        resp = request_url(url, timeout=18, retries=1)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')
        seen_links = set()
        for el in soup.select('article a, .news a, .views-row a, .item a, h2 a, h3 a, a[href]')[:220]:
            href = el.get('href', '')
            title = clean_web_title(el.get_text(' ', strip=True))
            if not href or not title or len(title) < 10:
                continue
            link = urljoin(url, href)
            lowered = (title + ' ' + link).lower()
            if not any(term in lowered for term in MEETING_LINK_LOWER):
                continue
            if is_generic_title(title) or is_generic_url(link):
                continue
            parent_text = clean_text(el.parent.get_text(' ', strip=True) if el.parent else '')
            item_date = infer_date_from_context(title, link, parent_text[:400])
            metadata = {'summary': '', 'published': None, 'content_type': '', 'word_count': 0}
            if not item_date:
                metadata = fetch_article_metadata(link)
                if metadata.get('published'):
                    item_date = metadata['published']
            if not item_date:
                continue
            age_days = (TODAY - item_date).days
            if age_days < -1 or age_days > MAX_ITEM_AGE_DAYS:
                continue
            normalized = normalize_history_link(link)
            if normalized in seen_links:
                continue
            seen_links.add(normalized)
            summary = metadata.get('summary') or trim_text(parent_text, 300)
            if not summary:
                summary = 'Meeting/minutes/statement item from ' + source_name
            results.append({
                'source': source_name,
                'title': title[:180],
                'link': link,
                'summary': summary,
                'published': item_date.isoformat(),
                'content_type': metadata.get('content_type', ''),
                'word_count': metadata.get('word_count', 0),
                'id': source_name + ':' + link,
            })
            if len(results) >= 10:
                break
    except Exception as exc:
        record_source_warning(warning_kind, source_name, url, exc)
    return results

def crossref_date(item):
    for key in ['published-print', 'published-online', 'published', 'issued']:
        parts = item.get(key, {}).get('date-parts', [])
        if parts and parts[0]:
            values = parts[0]
            year = values[0]
            month = values[1] if len(values) > 1 else 1
            day = values[2] if len(values) > 2 else 1
            return f'{year:04d}-{month:02d}-{day:02d}'
    return ''

def academic_author_names(authors, limit=4):
    names = []
    for author in authors or []:
        if isinstance(author, str):
            name = clean_text(author)
        else:
            given = clean_text(author.get('given', ''))
            family = clean_text(author.get('family', ''))
            name = clean_text(given + ' ' + family)
        if name and name not in names:
            names.append(name)
        if len(names) >= limit:
            break
    return names

def openalex_author_names(authorships, limit=4):
    names = []
    for authorship in authorships or []:
        name = clean_text((authorship.get('author') or {}).get('display_name', ''))
        if name and name not in names:
            names.append(name)
        if len(names) >= limit:
            break
    return names

def openalex_abstract(inverted_index):
    if not inverted_index:
        return ''
    positioned_words = []
    for word, positions in inverted_index.items():
        for position in positions or []:
            positioned_words.append((position, word))
    positioned_words.sort(key=lambda pair: pair[0])
    return clean_text(' '.join(word for _, word in positioned_words))

def academic_quality_gate(title, abstract, venue, doi, authors, diagnose=False):
    """Whitelist journal + DOI/authors + abstract length + strong CA anchor.

    Topical journals: title OR abstract may carry the CA anchor (not title-only).
    Regional journals: title+abstract together must show CA relevance.
    """
    title = clean_text(title)
    abstract = clean_text(abstract)
    venue_key = normalize_title_key(venue)
    title_lowered = title.lower()
    if venue_key not in ACADEMIC_JOURNAL_KEYS:
        if diagnose:
            note_academic_diag('venue_not_whitelist')
        return False
    if not doi:
        if diagnose:
            note_academic_diag('no_doi')
        return False
    if not authors:
        if diagnose:
            note_academic_diag('no_authors')
        return False
    if len(title) < ACADEMIC_MIN_TITLE_CHARS:
        if diagnose:
            note_academic_diag('title_short')
        return False
    if any(term in title_lowered for term in ACADEMIC_EXCLUDED_TITLE_TERMS):
        if diagnose:
            note_academic_diag('excluded_title')
        return False
    minimum_abstract = (
        ACADEMIC_TOPICAL_MIN_ABSTRACT_CHARS
        if venue_key in ACADEMIC_TOPICAL_JOURNAL_KEYS
        else ACADEMIC_MIN_ABSTRACT_CHARS
    )
    if len(abstract) < minimum_abstract:
        if diagnose:
            note_academic_diag('abstract_short')
        return False
    full_text = (title + ' ' + abstract).lower()
    full_text_has_anchor = any(term in full_text for term in STRONG_CA_ANCHOR_LOWER)
    # Topical and regional both accept title-or-abstract anchors; topical still needs
    # longer abstracts (above) so generic World Development papers do not flood in.
    if not full_text_has_anchor:
        if diagnose:
            note_academic_diag('no_ca_anchor')
        return False
    if diagnose:
        note_academic_diag('pass')
    return True

def academic_summary(venue, authors, abstract):
    parts = [venue]
    if authors:
        parts.append('Authors: ' + ', '.join(authors))
    parts.append(abstract)
    return clean_text(' | '.join(part for part in parts if part))[:500]

def _academic_item_from_crossref(work, source_name='Academic: Crossref'):
    titles = work.get('title') or []
    title = clean_text(titles[0] if titles else '')
    doi = clean_text(work.get('DOI', ''))
    link = 'https://doi.org/' + doi if doi else ''
    venue = clean_text((work.get('container-title') or [''])[0])
    abstract = clean_text(work.get('abstract', ''))
    # Crossref abstracts often include JATS tags.
    if abstract:
        abstract = BeautifulSoup(abstract, 'lxml').get_text(' ')
        abstract = clean_text(abstract)
    authors = academic_author_names(work.get('author', []))
    published_date = crossref_date(work)
    item_date = parse_date_text(published_date)
    if not item_date:
        note_academic_diag('no_date')
        return None
    age_days = (TODAY - item_date).days
    if age_days < -1 or age_days > ACADEMIC_LOOKBACK_DAYS:
        note_academic_diag('no_date')
        return None
    note_academic_diag('api_results')
    if not academic_quality_gate(title, abstract, venue, doi, authors, diagnose=True):
        return None
    return {
        'source': source_name,
        'title': title[:220],
        'link': link,
        'summary': academic_summary(venue, authors, abstract),
        'published': published_date,
        'academic_quality': True,
        'source_type': 'academic_paper',
        'source_tier': 1,
        'access_status': 'unknown',
        'academic_venue': venue,
        'academic_authors': authors,
        'id': source_name + ':' + doi,
    }

def _academic_item_from_openalex(work, source_name='Academic: OpenAlex'):
    title = clean_text(work.get('display_name', ''))
    doi_url = clean_text(work.get('doi', ''))
    doi = doi_url.replace('https://doi.org/', '').replace('http://doi.org/', '')
    primary_location = work.get('primary_location') or {}
    source = primary_location.get('source') or {}
    venue = clean_text(source.get('display_name', ''))
    abstract = openalex_abstract(work.get('abstract_inverted_index'))
    authors = openalex_author_names(work.get('authorships', []))
    published_date = clean_text(work.get('publication_date', ''))
    item_date = parse_date_text(published_date)
    if not item_date:
        note_academic_diag('no_date')
        return None
    note_academic_diag('api_results')
    if not academic_quality_gate(title, abstract, venue, doi, authors, diagnose=True):
        return None
    link = doi_url or clean_text(primary_location.get('landing_page_url', ''))
    if not link:
        return None
    return {
        'source': source_name,
        'title': title[:220],
        'link': link,
        'summary': academic_summary(venue, authors, abstract),
        'published': published_date,
        'academic_quality': True,
        'source_type': 'academic_paper',
        'source_tier': 1,
        'access_status': 'unknown',
        'academic_venue': venue,
        'academic_authors': authors,
        'cited_by_count': int(work.get('cited_by_count') or 0),
        'id': source_name + ':' + (doi or clean_text(work.get('id', ''))),
    }

def fetch_crossref(query, warning_kind='ACADEMIC'):
    source_name = 'Academic: Crossref'
    results = []
    try:
        from_date = TODAY - datetime.timedelta(days=ACADEMIC_LOOKBACK_DAYS)
        params = {
            'query.bibliographic': query,
            'rows': 25,
            'sort': 'published',
            'order': 'desc',
            'filter': ','.join([
                'from-pub-date:' + str(from_date),
                'until-pub-date:' + str(TODAY),
                'type:journal-article',
            ]),
        }
        url = academic_api_url('https://api.crossref.org/works', params, 'crossref')
        resp = request_academic_api(url, 'crossref', timeout=20)
        data = resp.json()
        for work in data.get('message', {}).get('items', []):
            item = _academic_item_from_crossref(work, source_name=source_name)
            if not item:
                continue
            results.append(item)
            if len(results) >= 5:
                break
    except Exception as exc:
        note_academic_diag('errors')
        record_source_warning(warning_kind, source_name, query, exc)
    return results

def fetch_crossref_whitelist_issns(warning_kind='ACADEMIC'):
    """Target Crossref by whitelist ISSN instead of broad topical search."""
    source_name = 'Academic: Crossref'
    results = []
    seen = set()
    from_date = TODAY - datetime.timedelta(days=ACADEMIC_LOOKBACK_DAYS)
    issns = sorted({
        ACADEMIC_JOURNAL_ISSN_L[key]
        for key in CROSSREF_DAILY_JOURNAL_KEYS
        if key in ACADEMIC_JOURNAL_ISSN_L
    })
    for issn in issns:
        try:
            params = {
                'rows': 20,
                'sort': 'published',
                'order': 'desc',
                'filter': ','.join([
                    'issn:' + issn,
                    'from-pub-date:' + str(from_date),
                    'until-pub-date:' + str(TODAY),
                    'type:journal-article',
                ]),
            }
            url = academic_api_url('https://api.crossref.org/works', params, 'crossref')
            resp = request_academic_api(url, 'crossref', timeout=20)
            data = resp.json()
            for work in data.get('message', {}).get('items', []):
                item = _academic_item_from_crossref(work, source_name=source_name)
                if not item:
                    continue
                key = item.get('id') or item.get('link')
                if key in seen:
                    continue
                seen.add(key)
                results.append(item)
        except Exception as exc:
            note_academic_diag('errors')
            record_source_warning(warning_kind, source_name, 'issn:' + issn, exc)
    return results

def fetch_openalex(query, warning_kind='ACADEMIC'):
    source_name = 'Academic: OpenAlex'
    results = []
    try:
        from_date = TODAY - datetime.timedelta(days=ACADEMIC_LOOKBACK_DAYS)
        # Prefer whitelist venues inside the broad topical query.
        source_ids = sorted(set(OPENALEX_ACADEMIC_SOURCE_IDS.values()))
        source_filter = ''
        if source_ids:
            source_filter = 'primary_location.source.id:' + '|'.join(source_ids) + ','
        params = {
            'search': query,
            'filter': ''.join([
                source_filter,
                'from_publication_date:' + str(from_date) + ',',
                'to_publication_date:' + str(TODAY) + ',',
                'type:article,',
                'is_retracted:false',
            ]),
            'sort': 'publication_date:desc',
            'per-page': 25,
        }
        url = academic_api_url('https://api.openalex.org/works', params, 'openalex')
        resp = request_academic_api(url, 'openalex', timeout=20)
        data = resp.json()
        for work in data.get('results', []):
            item = _academic_item_from_openalex(work, source_name=source_name)
            if not item:
                continue
            results.append(item)
            if len(results) >= 5:
                break
    except Exception as exc:
        note_academic_diag('errors')
        record_source_warning(warning_kind, source_name, query, exc)
    return results

def fetch_durable_openalex_backfill(warning_kind='ACADEMIC_BACKFILL'):
    """Find durable, unread scholarship only after a zero-result daily pass."""
    source_name = 'Academic: OpenAlex'
    results = []
    seen = set()
    try:
        from_date = TODAY - datetime.timedelta(days=DURABLE_ACADEMIC_BACKFILL_DAYS)
        until_date = TODAY - datetime.timedelta(days=ACADEMIC_LOOKBACK_DAYS + 1)
        source_ids = sorted(set(OPENALEX_ACADEMIC_SOURCE_IDS.values()))
        source_filter = (
            'primary_location.source.id:' + '|'.join(source_ids) + ','
            if source_ids else ''
        )
        for query in DURABLE_ACADEMIC_BACKFILL_QUERIES:
            params = {
                'search': query,
                'filter': ''.join([
                    source_filter,
                    'from_publication_date:' + str(from_date) + ',',
                    'to_publication_date:' + str(until_date) + ',',
                    'type:article,',
                    'is_retracted:false',
                ]),
                'sort': 'cited_by_count:desc',
                'per-page': 25,
            }
            url = academic_api_url('https://api.openalex.org/works', params, 'openalex')
            resp = request_academic_api(url, 'openalex', timeout=20)
            for work in resp.json().get('results', []):
                item = _academic_item_from_openalex(work, source_name=source_name)
                if not item:
                    continue
                key = item.get('id') or item.get('link')
                if key in seen:
                    continue
                seen.add(key)
                results.append(item)
                if len(results) >= 24:
                    return results
    except Exception as exc:
        note_academic_diag('errors')
        record_source_warning(warning_kind, source_name, 'durable-unread-backfill', exc)
    return results

def fetch_openalex_whitelist_sources(warning_kind='ACADEMIC'):
    """Pull recent works from regional whitelist journals in one bounded request."""
    source_name = 'Academic: OpenAlex'
    results = []
    seen = set()
    from_date = TODAY - datetime.timedelta(days=ACADEMIC_LOOKBACK_DAYS)

    def pull(filter_parts, search=None, limit=20):
        nonlocal results
        params = {
            'filter': ','.join(filter_parts),
            'sort': 'publication_date:desc',
            'per-page': limit,
        }
        if search:
            params['search'] = search
        url = academic_api_url('https://api.openalex.org/works', params, 'openalex')
        resp = request_academic_api(url, 'openalex', timeout=20)
        data = resp.json()
        for work in data.get('results', []):
            item = _academic_item_from_openalex(work, source_name=source_name)
            if not item:
                continue
            key = item.get('id') or item.get('link')
            if key in seen:
                continue
            seen.add(key)
            results.append(item)

    # Regional journals are already CA-focused. Topical whitelist journals are
    # covered by the eight thematic OpenAlex searches below, so repeating six
    # extra searches here only burns provider budget and increases 429 risk.
    regional_ids = OPENALEX_REGIONAL_SOURCE_IDS or sorted(set(OPENALEX_ACADEMIC_SOURCE_IDS.values()))
    if regional_ids:
        try:
            pull([
                'primary_location.source.id:' + '|'.join(regional_ids),
                'from_publication_date:' + str(from_date),
                'to_publication_date:' + str(TODAY),
                'type:article',
                'is_retracted:false',
            ], search=None, limit=50)
        except Exception as exc:
            note_academic_diag('errors')
            record_source_warning(warning_kind, source_name, 'regional-whitelist', exc)
    return results
# ================================================================
#  关键词体系
# ================================================================
KEYWORDS = [
    # 五国名称
    'kazakhstan', 'kazakh', 'qazaq', 'qazaqstan',
    'kyrgyzstan', 'kyrgyz', 'kirghiz',
    'uzbekistan', 'uzbek', 'ozbekiston',
    'tajikistan', 'tajik', 'tojikiston',
    'turkmenistan', 'turkmen',
    # 地理
    'central asia', 'mid asia', 'caspian', 'aral sea', 'fergana valley', 'pamir', 'tian shan',
    'xinjiang', 'western china', 'altai',
    # 政治
    'tokayev', 'mirziyoyev', 'rahmon', 'berdimuhamedow', 'jeparov',
    'nazarbayev', 'akayev', 'kulmukhamotov',
    'parliamentary election', 'presidential election', 'constitution', 'opposition', 'reform',
    'security council', 'prime minister', 'akim', 'president', 'government', 'cabinet', 'minister',
    'corruption', 'judiciary', 'transparency',
    # 安全
    'terrorism', 'extremism', 'islamic state', 'hizb ut-tahrir',
    'border dispute', 'military exercise', 'arms deal', 'drug trafficking', 'narcotics',
    'cybersecurity', 'data localization',
    # 经济能源
    'gas export', 'oil production', 'pipeline', 'natural gas', 'mining', 'uranium', 'gold', 'lithium', 'copper',
    'rare earth', 'critical minerals', 'middle corridor', 'trans-caspian', 'titr', 'lapis lazuli corridor',
    'water dispute', 'irrigation', 'dam project', 'water crisis', 'drought', 'climate change',
    'labor migration', 'remittance', 'china investment', 'foreign direct investment',
    'gdp growth', 'inflation', 'currency', 'som', 'tenge', 'somoni', 'sum', 'manat',
    'banking', 'ebrd', 'world bank', 'aisdb',
    'trade', 'export', 'import', 'sanction',
    # 区域组织
    'shanghai cooperation', 'sco', 'cica', 'turkic council', 'oic',
    'eurasian economic union', 'eeu', 'eaeu', 'csto', 'collective security',
    # 阿富汗
    'afghanistan', 'taliban', 'herat', 'mazar', 'refugee', 'border closure', 'visa policy',
    # 大国关系
    'russia central asia', 'moscow', 'putin', 'post-soviet', 'eurasian',
    'eu central asia', 'strategy', 'partnership', 'us aid',
    'china central asia', 'beijing',
    # 社会文化
    'language policy', 'russian language', 'religion', 'mosque', 'hajj',
    'civil society', 'ngo', 'journalism', 'press freedom', 'women rights',
    # 俄语
    'средняя азия', 'казахстан', 'киргизия', 'кыргызстан', 'узбекистан', 'таджикистан', 'туркменистан',
    'каспий', 'араль', 'синьцзян', 'памир', 'фергана', 'шаоск',
    'афганистан', 'талибан', 'трудовая миграция',
    # 本地语种常见写法
    'markaziy osiyo', 'oʻzbekiston', 'ozbekiston', 'qozogʻiston', 'qozogiston',
    'qirgʻiziston', 'qirgiziston', 'tojikiston', 'turkmaniston',
    'suv', 'energiya', 'iqtisodiyot', 'investitsiya', 'savdo', 'transport',
    'saylov', 'parlament', 'hukumat', 'prezident', 'konstitutsiya', 'migratsiya',
    'қазақстан', 'қырғызстан', 'өзбекстан', 'тәжікстан', 'түрікменстан',
    'су ресурстары', 'энергетика', 'инвестиция', 'сайлау', 'парламент',
    'тоҷикистон', 'ӯзбекистон', 'қирғизистон', 'муҳоҷират',
    # 中文
    '中亚', '哈萨克斯坦', '吉尔吉斯斯坦', '乌兹别克斯坦', '塔吉克斯坦', '土库曼斯坦',
    '新疆', '上海合作组织', '一带一路', '阿富汗', '塔利班', '跨境水资源',
    '中间走廊', '跨里海', '关键矿产', '稀土', '欧亚经济联盟', '集安组织',
]
KW_LOWER = [k.lower() for k in KEYWORDS]

CORE_CA_TERMS = [
    'kazakhstan', 'kazakh', 'qazaq', 'qazaqstan', 'astana', 'almaty',
    'kyrgyzstan', 'kyrgyz republic', 'kyrgyz', 'kirghiz', 'bishkek',
    'uzbekistan', 'uzbek', 'tashkent', 'samarkand',
    'tajikistan', 'tajik', 'dushanbe',
    'turkmenistan', 'turkmen', 'ashgabat',
    'central asia', 'fergana', 'pamir', 'caspian', 'aral sea', 'tian shan',
    'middle corridor', 'trans-caspian',
    'tokayev', 'mirziyoyev', 'rahmon', 'jeparov', 'berdimuhamedow',
    'шымкент', 'алматы', 'астана', 'казахстан', 'кыргызстан', 'киргизия',
    'узбекистан', 'таджикистан', 'туркменистан', 'средняя азия',
    'markaziy osiyo', 'oʻzbekiston', 'ozbekiston', 'qozogʻiston', 'qozogiston',
    'qirgʻiziston', 'qirgiziston', 'tojikiston', 'turkmaniston',
    'қазақстан', 'қырғызстан', 'өзбекстан', 'тәжікстан', 'түрікменстан',
    'тоҷикистон', 'ӯзбекистон', 'қирғизистон',
    'orta asya', 'kazakistan', 'kırgızistan', 'özbekistan', 'tacikistan', 'türkmenistan',
    'آسیای مرکزی', 'قزاقستان', 'قرقیزستان', 'ازبکستان', 'تاجیکستان', 'ترکمنستان',
    '中亚', '哈萨克斯坦', '吉尔吉斯斯坦', '乌兹别克斯坦', '塔吉克斯坦', '土库曼斯坦',
]
CORE_CA_LOWER = [k.lower() for k in CORE_CA_TERMS]

RESEARCH_TERMS = [
    'water', 'energy', 'pipeline', 'gas', 'oil', 'uranium', 'mining',
    'rare earth', 'critical minerals', 'middle corridor', 'trans-caspian',
    'migration', 'remittance', 'sanction', 'transport corridor', 'trade',
    'security', 'border', 'taliban', 'afghanistan', 'sco', 'csto', 'eaeu',
    'election', 'constitution', 'reform', 'corruption', 'civil society',
    'democracy', 'human rights', 'political rights', 'civil liberties',
    'rule of law', 'media freedom', 'press freedom', 'political system',
    'state capacity', 'political landscape', 'media landscape',
    'climate', 'language policy', 'religion', 'china', 'russia', 'eu',
    'culture', 'education', 'archaeological', 'health', 'national bank',
    'business council', 'investment needs', 'transport', 'infrastructure',
    '中间走廊', '跨里海', '关键矿产', '稀土', '跨境水资源',
    'suv', 'energiya', 'iqtisodiyot', 'investitsiya', 'savdo', 'saylov',
    'parlament', 'hukumat', 'konstitutsiya', 'migratsiya',
    'инвестиция', 'сайлау', 'парламент', 'энергетика',
    'иқтисод', 'савдо', 'муҳоҷират',
    'güvenlik', 'ekonomi', 'dış politika', 'göç', 'enerji', 'sınır',
    'yönetişim', 'insan hakları', 'reform',
    'امنیت', 'اقتصاد', 'سیاست خارجی', 'مهاجرت', 'انرژی', 'مرز',
    'حکمرانی', 'حقوق بشر', 'اصلاحات',
]
RESEARCH_LOWER = [k.lower() for k in RESEARCH_TERMS]

DEPTH_TERMS = [
    'analysis', 'commentary', 'opinion', 'interview', 'long read',
    'report', 'brief', 'policy brief', 'white paper', 'working paper',
    'research', 'survey', 'outlook', 'forecast', 'assessment',
    'country report', 'country assessment', 'country profile', 'country overview',
    'freedom in the world', 'nations in transit', 'transformation index',
    'world report', 'political landscape', 'media landscape',
    'strategy', 'implications', 'why', 'how', 'explainer',
    '分析', '评论', '专访', '报告', '简报', '政策简报', '白皮书',
    '研究', '调查', '展望', '预测', '评估', '战略', '影响',
    'анализ', 'комментарий', 'доклад', 'исследование', 'прогноз',
    'analiz', 'analizi', 'yorum', 'rapor', 'raporu', 'araştırma', 'araştırması', 'değerlendirme', 'inceleme',
    'تحلیل', 'تفسیر', 'گزارش', 'پژوهش', 'ارزیابی', 'بررسی', 'مصاحبه',
]
DEPTH_LOWER = [term.lower() for term in DEPTH_TERMS]

ALWAYS_INCLUDE_SOURCES = (
    LOCAL_KZ | LOCAL_UZ | LOCAL_KG | LOCAL_TJ | LOCAL_TM | REGIONAL_LOCAL |
    {
        'Eurasianet', 'CABAR.asia', 'The Times of Central Asia',
        'Central Asia-Caucasus Analyst', 'Central Asia New Strategies',
        'Caspian Policy Center', 'Oxus Society',
        'Central Asian Survey', 'Post-Soviet Affairs',
        'International Crisis Group Central Asia', 'Human Rights Watch Central Asia',
        'Eurasian Development Bank', 'UNRCCA', 'UNDP Eurasia',
    }
)

COUNTRY_TAGS = {
    '哈萨克斯坦': ['kazakhstan', 'kazakh', 'qazaq', 'astana', 'almaty', 'tokayev', 'tenge', '哈萨克斯坦', 'казахстан', 'kazakistan', 'قزاقستان'],
    '乌兹别克斯坦': ['uzbekistan', 'uzbek', 'tashkent', 'samarkand', 'mirziyoyev', 'sum', '乌兹别克斯坦', 'узбекистан', 'özbekistan', 'ازبکستان'],
    '吉尔吉斯斯坦': ['kyrgyzstan', 'kyrgyz', 'kirghiz', 'bishkek', 'jeparov', 'som', '吉尔吉斯斯坦', 'кыргызстан', 'киргизия', 'kırgızistan', 'قرقیزستان'],
    '塔吉克斯坦': ['tajikistan', 'tajik', 'dushanbe', 'rahmon', 'somoni', '塔吉克斯坦', 'таджикистан', 'tacikistan', 'تاجیکستان'],
    '土库曼斯坦': ['turkmenistan', 'turkmen', 'ashgabat', 'berdimuhamedow', 'manat', '土库曼斯坦', 'туркменистан', 'türkmenistan', 'ترکمنستان'],
    '阿富汗关联': ['afghanistan', 'taliban', 'herat', 'mazar', '阿富汗', '塔利班', 'афганистан', 'талибан'],
}

TOPIC_TAGS = {
    '政治治理': [
        'election', 'constitution', 'reform', 'government', 'minister',
        'president', 'akim', 'corruption', 'human rights', 'political rights',
        'civil liberties', 'rule of law', 'media freedom', 'press freedom',
        'democracy', 'political system', 'political landscape', 'media landscape',
    ],
    '安全防务': ['security', 'terrorism', 'extremism', 'border', 'military', 'csto', 'collective security'],
    '经济能源': ['gas', 'oil', 'pipeline', 'uranium', 'mining', 'trade', 'investment', 'banking', 'national bank', 'ebrd', 'sanction'],
    '水资源气候': ['water', 'irrigation', 'dam', 'drought', 'climate', 'aral'],
    '外交关系': ['summit', 'partnership', 'sco', 'china', 'russia', 'eu ', 'beijing', 'moscow'],
    '社会文化': ['migration', 'remittance', 'language', 'religion', 'civil society', 'journalism', 'education', 'culture', 'archaeological'],
}

PUBLIC_TAG_LABELS = {
    '政治治理': '治理动态',
    '安全防务': '区域稳定',
    '外交关系': '对外关系',
    '阿富汗关联': '南向邻近地区',
}

def format_public_tags(tags):
    return [PUBLIC_TAG_LABELS.get(tag, tag) for tag in tags]

PUBLIC_RESEARCH_TOPIC_LABELS = {
    '阿富汗关联与边境风险': '南向邻近地区与边境治理',
}

def public_research_topic_label(label):
    return PUBLIC_RESEARCH_TOPIC_LABELS.get(label, label)

def public_research_topic_labels(labels):
    return [public_research_topic_label(label) for label in labels]

def tag_item(item):
    text = clean_text(item.get('title', '') + ' ' + item.get('summary', '') + ' ' + item.get('source', '')).lower()
    tags = []
    for label, terms in COUNTRY_TAGS.items():
        if count_terms(text, [term.lower() for term in terms]) > 0:
            tags.append(label)
    for label, terms in TOPIC_TAGS.items():
        if count_terms(text, [term.lower() for term in terms]) == 0:
            continue
        if label == '安全防务':
            benign_security_contexts = ['food security', 'energy security', 'water security', 'economic security']
            hard_security_terms = [
                'terrorism', 'extremism', 'border', 'military', 'csto',
                'collective security', 'conflict', 'war', 'attack', 'unrest',
                'drug trafficking', 'security role', 'security policy',
            ]
            if (
                any(term in text for term in benign_security_contexts)
                and not any(term in text for term in hard_security_terms)
            ):
                continue
        tags.append(label)
    return tags[:6]

NEGATIVE_KEYWORDS = [
    'israel', 'palestinian', 'west bank', 'jerusalem', 'tel aviv',
    'portugal', 'ronaldo', 'world cup', 'football', 'soccer', 'nba', 'tennis', 'olympic',
    'ebola', 'kenya', 'montreal', 'australia', 'one nation', 'dettol', 'fuel rebate',
    'australian politics', 'melbourne', 'toronto', 'canada', 'usa election',
    'trump', 'biden', 'zelenskyy', 'putin ukraine', 'ukraine war',
    'bangladesh', 'nepal', 'sri lanka',
    'boxing academy', 'world cup', 'horror film', 'celebrity',
    'storm approaching', 'magnitude 3 earthquake',
    'chocolate', 'raffaello', 'family park', 'no electricity',
    'golden globes', 'tribute gala', 'film and television',
    'vacancy', 'job opening', 'job vacancies', 'project evaluator',
    'happy new year', 'new year greeting', 'horse racing championship',
    'horse beauty contest',
    'не будет света', 'конфет', 'шоколада', 'вакансия', 'вакансии',
]
NEG_KW_LOWER = [k.lower() for k in NEGATIVE_KEYWORDS]

PUBLICATION_RISK_TERMS = [
    'attack', 'attacks', 'killed', 'violence', 'violent', 'war',
    'prisoner of war', 'prisoners of wars', 'ukraine', 'russia-ukraine',
    'taliban', 'terrorism', 'terrorist', 'extremism', 'extremist',
    'afghanistan', 'afghan',
    'islamic state', 'hizb ut-tahrir', 'drug trafficking', 'narcotics',
    'money laundering', 'sanction', 'sanctions', 'arms deal',
    'military exercise', 'border dispute', 'border clash', 'unrest',
    'protest', 'opposition', 'detention', 'prisoner', 'prison',
    'court', 'trial', 'threat', 'pressure', 'abuse', 'xinjiang',
    'corruption', 'fraud', 'crime', 'overthrown',
    'islam', 'religion', 'religious', 'mosque', 'hajj',
    '袭击', '遇袭', '被杀', '死亡', '暴力', '战争', '战俘', '乌克兰',
    '阿富汗', '塔利班', '恐怖', '极端主义', '贩毒', '毒品', '洗钱', '制裁',
    '军演', '军售', '边境冲突', '抗议', '骚乱', '反对派', '拘留',
    '冲突', '虐待', '新疆', '腐败', '犯罪', '囚犯', '法院', '审判',
    '威胁', '施压', '伊斯兰', '宗教', '清真寺', '朝觐',
    'напад', 'убит', 'войн', 'украин', 'талиб', 'террор',
    'экстрем', 'наркот', 'санкц', 'коррупц', 'протест',
    'оппози', 'заключ', 'суде', 'суд', 'угроз', 'давлен',
    'фаталь', 'сверг', 'ислам', 'религи', 'мечет',
]
PUBLICATION_RISK_LOWER = [term.lower() for term in PUBLICATION_RISK_TERMS]

# Hard risk: keep out of public body almost always (WeChat + scholarly prudence).
# Soft risk: war/afghanistan/sanctions etc. may appear in legitimate deep analysis;
# those pieces can still enter public if they pass research-grade deep gates.
HARD_PUBLICATION_RISK_TERMS = [
    'attack', 'attacks', 'killed', 'violence', 'violent',
    'terrorism', 'terrorist', 'extremism', 'extremist',
    'islamic state', 'hizb ut-tahrir',
    'drug trafficking', 'narcotics', 'money laundering',
    'unrest', 'protest', 'opposition',
    'detention', 'prisoner', 'prison', 'court', 'trial',
    'abuse', 'xinjiang', 'corruption', 'fraud', 'crime', 'overthrown',
    'islam', 'religion', 'religious', 'mosque', 'hajj',
    '袭击', '攻击', '杀害', '暴力', '恐怖', '极端', '毒品', '洗钱',
    '抗议', '反对派', '拘押', '监狱', '法庭', '审判', '腐败', '欺诈',
    '新疆', '政变', '宗教', '清真寺', '朝觐',
]
HARD_PUBLICATION_RISK_LOWER = [term.lower() for term in HARD_PUBLICATION_RISK_TERMS]


PUBLICATION_RISK_SOURCES = {
    'Kloop', 'RFE/RL Central Asia', 'Azattyq (Kazakh)',
    'Ozodi (Uzbek/Tajik)', 'Khronika.info',
}

PUBLIC_LOW_VALUE_TERMS = [
    'weather', 'forecast', 'weekend', 'rain', 'snow',
    'dollar traded', 'exchange rate', 'stock exchange',
    'average prices', 'tomatoes', 'cucumbers',
    'concert', 'birthday', 'anniversary celebration',
    # thin transport / personnel / deal blurbs never belong in public deep digest
    'direct flight', 'launch direct flight', 'launches flight', 'airline',
    'airport', 'air route', 'air link', 'flights between',
    'appointed', 'appointment of', 'named akim', 'as akim',
    'keeps building', 'expands ties', 'expand ties with',
    'signed a series of', 'signs a series of', 'signed agreements',
    'humanitarian aid', 'medical cooperation and trade talks',
    '航班', '直飞', '通航', '航线', '任命', '出任', '州长',
    'egypt', 'byzantine', 'grocery delivery', 'delivery service',
    'grandmother', 'chose to fly', 'comic-con', 'comic con',
    'netflix', 'one piece', 'actor', 'actress', 'anime',
    'akhal-teke', 'filly', 'horse', 'opera legend',
    '100 years of opera', 'new book', 'ten words one journey',
    'weekly media highlights', 'headlines from the caspian',
    'quick primer', 'headline news', 'latest headlines',
    'discuss closer economic ties', 'closer economic ties',
    'does not rule out visa-free travel', 'visa-free travel to schengen',
    'please contact us', 'contact us for more details',
    'love sushi', 'sushi', 'hidden risks', 'health experts issue warning',
    'empire state building',
    'annual security conference', 'hosted annual', 'hosted the annual',
    'launches institute', 'launches g-index', 'research base for women',
    'commemorative coin', 'commemorative coins', 'donkey meat',
    'banana', 'bananas', 'tropical fruit', 'sign language proposal',
    '天气', '预报', '周末', '降雨', '降雪',
    '美元交易', '汇率', '证券交易所', '平均价格',
    '西红柿', '黄瓜', '埃及', '拜占庭', '杂货配送',
    '配送服务', '祖母', '动漫展', 'Netflix', '海贼王',
    '演员', '马匹', '小马', '歌剧传奇', '诞辰100周年',
    '新书', '每周媒体', '头条新闻', '新闻集锦',
    '寿司', '联系我们', '更多详情', '帝国大厦',
    '年度安全会议', '举办年度', '成立研究所', '女性创业研究基地',
    '纪念币', '驴肉', '香蕉', '热带水果', '手语求婚',
    'погода', 'выходные', 'доллар', 'бирж', 'помидор',
    'огурц', 'египт', 'византий', 'доставк', 'бабушк',
]
PUBLIC_LOW_VALUE_LOWER = [term.lower() for term in PUBLIC_LOW_VALUE_TERMS]

# Admissions, degree and course-promotion pages are useful as source
# discovery signals but are not research outputs for the public digest.
# Keep this separate from the general low-value list so a substantive paper
# about higher education is not rejected merely for mentioning a master's
# programme in its body.
EDUCATION_PROMOTION_TITLE_TERMS = [
    'master programme', 'master program', "master's program", "master's programme",
    'masters program', 'masters programme', 'msc in ', 'ma in ', 'mba in ',
    'graduate studies', 'graduate programme', 'graduate program',
    'academic program', 'academic programmes', 'degree program', 'degree programme',
    'admissions', 'admission open', 'apply now', 'enrol', 'enrollment',
    '硕士课程', '硕士项目', '硕士学位', '研究生课程', '研究生项目', '招生', '申请入学',
    'scholarship', 'scholarships', 'fellowship', 'fellowships', 'grant application',
    'research grant', 'research grants', 'grant programme', 'grant program',
    'fundraising', 'fund-raising', 'case summit',
    '奖学金', '奖助学金', '研究资助', '补助金', '筹款', ' fellowship计划',
]
EDUCATION_PROMOTION_URL_TERMS = [
    '/admissions', '/admission', '/masters', '/master-', '/graduate-studies',
    '/graduate-program', '/academic-program', '/degree-program', '/apply',
    '/programs/masters', '/programmes/masters', '/programs/graduate',
    '/programmes/graduate',
    '/scholarship', '/scholarships', '/fellowship', '/fellowships',
    '/grants', '/grant-application', '/events/', '/event/',
]
EDUCATION_PROMOTION_BODY_TERMS = [
    'tuition fee', 'application deadline', 'how to apply', 'entry requirements',
    '120 ects', 'two-year programme', 'two-year program', 'study programme',
    'study program', '学费', '申请截止', '入学要求', '招生简章', '两年制',
    'application deadline', 'eligibility criteria', 'funding opportunity',
    '奖学金申请', '申请资格', '资助机会',
]

def is_education_promotion(item):
    """Return True for admissions/course marketing pages, not research."""
    title = item_title_text(item)
    summary = clean_text(item.get('summary', '')).lower()
    link_path = urllib.parse.urlparse(item.get('link', '') or '').path.lower()
    research_frame = any(term in title for term in STRICT_DEEP_FORMAT_LOWER)
    # URL evidence is decisive for dedicated admissions/programme pages.
    if any(term in link_path for term in EDUCATION_PROMOTION_URL_TERMS) and not research_frame:
        return True
    if any(term in title for term in EDUCATION_PROMOTION_TITLE_TERMS):
        # A title explicitly framed as a report/study/analysis remains eligible.
        if not research_frame:
            return True
    body_hits = sum(1 for term in EDUCATION_PROMOTION_BODY_TERMS if term in summary)
    return body_hits >= 2 and not any(term in title for term in STRICT_DEEP_FORMAT_LOWER)

PUBLIC_NEWS_AGGREGATION_TERMS = [
    'news digest', 'weekly digest', 'daily digest', 'roundup', 'media roundup',
    'foreign media on', 'top stories', 'latest news', 'in brief', 'briefly',
    '新闻摘要', '新闻综述', '媒体摘要', '一周新闻', '简讯', '快讯',
]
PUBLIC_NEWS_AGGREGATION_LOWER = [term.lower() for term in PUBLIC_NEWS_AGGREGATION_TERMS]

PUBLIC_DEEP_SIGNAL_TERMS = [
    'analysis', 'analyst', 'commentary', 'opinion', 'interview', 'investigation',
    'long read', 'deep dive', 'report', 'study', 'policy brief', 'working paper',
    'explainer', 'how ', 'why ', 'what lies beneath', 'implications', 'strategy',
    '分析', '评论', '访谈', '调查', '深度', '报告', '研究', '政策简报',
    '解释', '为什么', '如何', '影响', '战略',
]
PUBLIC_DEEP_SIGNAL_LOWER = [term.lower() for term in PUBLIC_DEEP_SIGNAL_TERMS]

INTERNAL_LOW_VALUE_NEWS_TERMS = [
    'detained in', 'drug trafficking detained', 'suspects in', 'viral',
    'marriage proposal', 'social media', 'video of', 'heartfelt',
    'regular meeting', 'met with', 'receives the minister',
    'consul', 'consulate', 'commemorative coin', 'commemorative coins',
    'donkey meat', 'banana', 'bananas', 'tropical fruit',
    'narcotic', 'narcotics', 'psychotropic', 'prison sentence',
    'courtesy call', 'courtesy meeting', 'paid a visit',
    'heat record', 'temperature record', 'record heat', 'record high',
    'heatwave', 'heat wave', 'weather forecast', 'air temperature',
    'working visit', 'state visit', 'official visit', 'arrived in',
    'pays a visit', 'on a visit to', 'visit to china', 'visit to russia',
    'is visiting', 'left for', 'departed for', 'holds talks with',
    '被拘留', '嫌疑人', '贩毒', '走红', '求婚', '社交媒体', '例会', '会见',
    '领事', '总领事', '纪念币', '驴肉', '香蕉', '热带水果',
    '麻醉药品', '精神药物', '礼节性', '拜会', '获刑', '入狱',
    '高温纪录', '气温纪录', '创纪录高温', '热浪', '天气预报', '气温达到',
    '工作访问', '国事访问', '正式访问', '抵达', '出访', '正在访问',
    'консул', 'генконсул', 'соотечественник', 'памятные монеты',
    'банан', 'наркотичес', 'психотроп', 'осужден',
    'температура', 'жара', 'рекорд', 'рабочий визит', 'государственный визит',
    'аптап', 'ауа температурасы', 'жұмыс сапары', 'мемлекеттік сапар',
]
INTERNAL_LOW_VALUE_NEWS_LOWER = [term.lower() for term in INTERNAL_LOW_VALUE_NEWS_TERMS]

STRICT_INTERNAL_LOW_VALUE_TERMS = [
    'consul', 'consulate', 'commemorative coin', 'commemorative coins',
    'donkey meat', 'banana', 'bananas', 'tropical fruit',
    'marriage proposal', 'sign language proposal', 'proposal went viral',
    'goes viral', 'viral on social media',
    'narcotic', 'narcotics', 'psychotropic',
    'regular meeting', 'receives the minister', 'met with the minister',
    'hosted the regular meeting', 'meeting of the minister',
    'heat record', 'temperature record', 'record heat', 'record high',
    'heatwave', 'heat wave', 'weather forecast', 'air temperature',
    'working visit', 'state visit', 'official visit',
    'esek', 'esektin', 'etin satqandar', 'есек', 'есект',
    '领事', '总领事', '纪念币', '驴肉', '香蕉', '热带水果',
    '手语求婚', '求婚', '风靡一时', '走红', '麻醉药品', '精神药物',
    '例会', '会见部长', '部长会晤',
    '高温纪录', '气温纪录', '创纪录高温', '热浪', '天气预报',
    '工作访问', '国事访问', '正式访问', '出访',
    'консул', 'генконсул', 'памятные монеты', 'банан',
    'наркотичес', 'психотроп',
    'температура', 'жара', 'рабочий визит', 'государственный визит',
    'аптап', 'ауа температурасы', 'жұмыс сапары', 'мемлекеттік сапар',
]
STRICT_INTERNAL_LOW_VALUE_LOWER = [term.lower() for term in STRICT_INTERNAL_LOW_VALUE_TERMS]

PUBLIC_CONVERSION_EXCLUDE_TERMS = [
    'flight', 'flights', 'airline', 'airlines', 'airport', 'direct flight',
    'xinjiang', 'urumqi',
    'attack', 'killed', 'violence', 'war', 'prison', 'jail', 'detention',
    'detained', 'convicted', 'coup', 'corruption', 'fraud', 'drug',
    'narcotics', 'terror', 'extremism', 'foreign agent', 'spying', 'spy',
    'surveillance', 'protest', 'opposition', 'religion', 'islam',
    'domestic violence', 'abuse', 'blocked', 'hacked', 'instagram',
    '袭击', '被杀', '暴力', '战争', '监禁', '拘留', '判刑', '政变',
    '腐败', '欺诈', '毒品', '恐怖', '极端', '外国代理人', '间谍',
    '监控', '抗议', '反对派', '宗教', '伊斯兰', '家暴', '封锁',
]
PUBLIC_CONVERSION_EXCLUDE_LOWER = [term.lower() for term in PUBLIC_CONVERSION_EXCLUDE_TERMS]

PUBLIC_CONVERTIBLE_INTERNAL_SOURCES = {
    'Kursiv Kazakhstan English', 'The Diplomat', 'Vlast.kz',
    'The Times of Central Asia', 'The Astana Times',
    'Caspian Policy Center RSS', 'Eurasian Research Institute',
}

NORMALIZED_PUBLIC_CONVERSION_TAGS = {'经济能源', '水资源气候'}
NORMALIZED_PUBLIC_CONVERSION_TOPICS = {
    '中间走廊与互联互通',
    '关键矿产与能源转型',
}
NORMALIZED_PUBLIC_CONVERSION_TERMS = [
    'trade', 'investment', 'deal pipeline', 'banking cooperation',
    'transport', 'corridor', 'logistics', 'rail', 'railway', 'port',
    'energy', 'renewable', 'green power', 'critical minerals', 'mining',
    '贸易', '投资', '银行合作', '交通', '走廊', '物流', '铁路', '港口',
    '能源', '可再生能源', '绿色电力', '关键矿产', '矿产',
]
NORMALIZED_PUBLIC_CONVERSION_LOWER = [term.lower() for term in NORMALIZED_PUBLIC_CONVERSION_TERMS]

def is_durable_research_grade(item):
    """Stricter gate for older material admitted through the long horizon."""
    age_days = item_age_days(item)
    if age_days is None:
        return False
    if age_days <= MAX_SLOW_PUBLICATION_AGE_DAYS:
        return True
    if item.get('source_tier', 3) > 2:
        return False
    if item.get('access_status') in {'paywalled', 'blocked'}:
        return False
    if is_public_simple_news(item) or is_institute_soft_content(item) or is_public_low_value(item):
        return False
    if not has_strong_central_asia_anchor(item):
        return False
    source_type = clean_text(item.get('source_type', '')).lower()
    content_type = clean_text(item.get('content_type', '')).lower()
    durable_forms = (
        'institution' in source_type or 'report' in source_type or
        'academic' in source_type or 'working paper' in content_type or
        'research' in content_type or 'policy' in content_type or
        'report' in content_type or 'journal' in content_type
    )
    if not durable_forms:
        return False
    summary = clean_text(item.get('summary', ''))
    word_count = int(item.get('word_count', 0) or 0)
    return len(summary) >= 120 or word_count >= 500

def is_recent_item(item):
    source = item.get('source', '')
    age_days = item_age_days(item)
    if age_days is None:
        return source not in SPECIAL_DATE_REQUIRED_SOURCES
    if age_days < -1:
        return False
    if source in ACADEMIC_SOURCE_NAMES:
        # Daily academic discovery stays recent, but on an otherwise empty day
        # a previously unseen, peer-reviewed article may enter through the
        # durable backfill path below.
        max_age = DURABLE_ACADEMIC_BACKFILL_DAYS
    elif source in DURABLE_PRESTIGE_DISCOVERY_SOURCES or item.get('source_type') == 'institution_publication':
        # Major think-tank and university archives supply unread, durable
        # research rather than only the last month of web updates.
        max_age = DURABLE_RESEARCH_MAX_AGE_DAYS
    elif source in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES):
        # Institutional reports remain useful beyond the daily news cycle;
        # history/strategy/governance studies may be surfaced until they have
        # appeared in the digest or reached the durable-research horizon.
        max_age = DURABLE_RESEARCH_MAX_AGE_DAYS
    elif source in HIGH_SIGNAL_DEEP_SOURCES or is_deep_discovery_source(source):
        max_age = MAX_DEEP_ANALYSIS_AGE_DAYS
    else:
        max_age = MAX_ITEM_AGE_DAYS
    if age_days > max_age:
        return False
    if age_days > MAX_SLOW_PUBLICATION_AGE_DAYS and not is_durable_research_grade(item):
        return False
    return True

def is_new_discovery_item(item):
    """Material newly discovered in the rolling window, not necessarily published today."""
    if clean_text(item.get('date_precision', '')).lower() == 'year':
        return False
    item_date = parse_item_published_date(item)
    if not item_date:
        return False
    age_days = (TODAY - item_date).days
    return -1 <= age_days <= NEW_DISCOVERY_LOOKBACK_DAYS

def item_content_text(item):
    return strip_source_boilerplate(
        item.get('title', '') + ' ' +
        item.get('summary', '') + ' ' +
        link_relevance_text(item.get('link', ''))
    ).lower()

STRONG_CA_ANCHOR_TERMS = [
    'central asia', 'kazakhstan', 'uzbekistan', 'kyrgyzstan', 'kyrgyz republic', 'tajikistan',
    'turkmenistan', 'astana', 'almaty', 'tashkent', 'bishkek', 'dushanbe',
    'ashgabat', 'tokayev', 'mirziyoyev', 'rahmon', 'japarov',
    'middle corridor', 'trans-caspian', 'transcaspian', 'caspian corridor', 'caspian sea',
    'titr', 'fergana', 'aral sea', 'amu darya', 'syr darya',
    '中亚', '哈萨克斯坦', '乌兹别克斯坦', '吉尔吉斯斯坦', '塔吉克斯坦', '土库曼斯坦',
    '中间走廊', '跨里海',
    'казахстан', 'узбекистан', 'кыргызстан', 'киргизия', 'таджикистан', 'туркменистан',
    'средний коридор', 'транскаспий',
]
STRONG_CA_ANCHOR_LOWER = [term.lower() for term in STRONG_CA_ANCHOR_TERMS]

def has_strong_central_asia_anchor(item):
    text = item_content_text(item)
    ca_substance_terms = [
        'kazakhstan', 'uzbekistan', 'kyrgyzstan', 'kyrgyz republic', 'tajikistan', 'turkmenistan',
        'astana', 'almaty', 'tashkent', 'bishkek', 'dushanbe', 'ashgabat',
        'tokayev', 'mirziyoyev', 'rahmon', 'japarov',
        'middle corridor', 'trans-caspian', 'transcaspian', 'caspian corridor', 'caspian sea',
        'titr', 'fergana', 'aral sea', 'amu darya', 'syr darya',
        '中亚五国', '哈萨克斯坦', '乌兹别克斯坦', '吉尔吉斯斯坦', '塔吉克斯坦', '土库曼斯坦',
        '中间走廊', '跨里海',
        'казахстан', 'узбекистан', 'кыргызстан', 'киргизия', 'таджикистан', 'туркменистан',
        'средний коридор', 'транскаспий',
    ]
    has_substance = any(term in text for term in ca_substance_terms)
    has_generic = 'central asia' in text or '中亚' in text
    # Georgia/Caucasus-only pieces without CA substance are not CA anchors.
    if re.search(r'\b(georgia|georgian|tbilisi)\b', text) and not (has_substance or has_generic):
        return False
    # Broad "Eurasia + Iran/Afghanistan" framing is not enough. This avoids
    # admitting Caucasus- or Middle East-centered corridor studies merely
    # because their route could indirectly affect Central Asia.
    return has_substance or has_generic

HIGH_VALUE_LOCAL_ECON_TERMS = [
    'critical mineral', 'rare earth', 'tungsten', 'uranium', 'copper', 'lithium',
    'gold', 'mining', 'mine', 'deposit', 'processing plant', 'supply chain',
    'middle corridor', 'trans-caspian', 'caspian', 'transport corridor',
    'railway', 'pipeline', 'port', 'logistics', 'hydropower', 'water',
    'wind farm', 'renewable energy', 'solar', 'oil', 'gas', 'exports to',
    'sanctions', 'remittance', 'labor migration', 'eaeu', 'sco',
    '关键矿产', '稀土', '钨', '铀', '铜', '锂', '黄金', '矿', '供应链',
    '中间走廊', '跨里海', '运输走廊', '铁路', '管道', '港口', '水电',
    '风电', '可再生能源', '石油', '天然气', '劳务移民', '侨汇',
]
HIGH_VALUE_LOCAL_ECON_LOWER = [term.lower() for term in HIGH_VALUE_LOCAL_ECON_TERMS]

def has_high_value_local_econ_signal(item):
    text = item_content_text(item)
    for term in HIGH_VALUE_LOCAL_ECON_LOWER:
        if re.fullmatch(r'[a-z0-9]+', term):
            if re.search(r'\b' + re.escape(term) + r'\b', text):
                return True
        elif term in text:
            return True
    return False

def count_terms(text, terms):
    count = 0
    for term in terms:
        term = str(term or '').strip().lower()
        if not term:
            continue
        if re.fullmatch(r'[a-z0-9][a-z0-9 -]*', term):
            if re.search(r'(?<![a-z0-9])' + re.escape(term) + r'(?![a-z0-9])', text):
                count += 1
        elif term in text:
            count += 1
    return count

COUNTRY_ASSESSMENT_EXPLICIT_TERMS = [
    'country assessment', 'country report', 'country profile', 'country overview',
    'national assessment', 'political landscape', 'media landscape',
    'human rights assessment', 'state of democracy', 'state of human rights',
    'freedom in the world', 'nations in transit', 'transformation index',
    'world report', 'retreating rights', 'rights in retreat',
    '综合国情', '国家形势', '国情评估', '国家评估', '国家概况',
    '政治图景', '媒体生态', '人权评估', '民主状况', '权利状况',
    'страновой доклад', 'обзор страны', 'политический ландшафт',
]
COUNTRY_ASSESSMENT_BREADTH_TERMS = [
    'political system', 'political change', 'governance', 'democracy',
    'human rights', 'civil liberties', 'rule of law', 'media freedom',
    'civil society', 'state capacity', 'economy', 'society', 'transformation',
    'current situation', '政治制度', '政治变迁', '治理', '民主', '人权',
    '公民自由', '法治', '媒体自由', '公民社会', '国家能力', '经济', '社会',
]

def item_country_labels(item):
    text = item_content_text(item)
    labels = []
    for label, terms in COUNTRY_TAGS.items():
        if label == '阿富汗关联':
            continue
        if count_terms(text, [term.lower() for term in terms]) > 0:
            labels.append(label)
    return labels

def is_country_assessment_item(item):
    """Recognize single-country panoramic research beyond title format labels."""
    if not has_strong_central_asia_anchor(item):
        return False
    if item.get('country_assessment') is True or item.get('source') in COUNTRY_ASSESSMENT_SOURCE_NAMES:
        return True
    countries = item_country_labels(item)
    if len(countries) != 1:
        return False
    text = item_content_text(item)
    title = item_title_text(item)
    if count_terms(text, COUNTRY_ASSESSMENT_EXPLICIT_TERMS) > 0:
        return True
    introductory_title = (
        'introduction' in title or 'introducing ' in title
        or 'spotlight on' in title or '聚焦' in title or '导论' in title
    )
    breadth = count_terms(text, COUNTRY_ASSESSMENT_BREADTH_TERMS)
    return introductory_title and breadth >= 2

def research_topic_matches(item):
    text = item_content_text(item)
    matches = []
    for topic in RESEARCH_TOPIC_TERMS_LOWER:
        hit_count = count_terms(text, topic['terms'])
        if hit_count > 0:
            matches.append({
                'label': topic['label'],
                'weight': topic['weight'],
                'hits': hit_count,
            })
    matches.sort(key=lambda match: (-match['weight'], -match['hits'], match['label']))
    return matches

def research_topic_score(item):
    matches = research_topic_matches(item)
    if not matches:
        return 0
    score = 0
    for index, match in enumerate(matches[:3]):
        score += max(0, match['weight'] - index * 12) + min(match['hits'], 4) * 4
    return score

def text_contains_risk_term(text, term):
    if re.fullmatch(r'[a-z0-9][a-z0-9 -]*', term):
        return bool(re.search(r'(?<![a-z0-9])' + re.escape(term) + r'(?![a-z0-9])', text))
    return term in text

def publication_risk_terms(item):
    text = item_content_text(item)
    return [term for term in PUBLICATION_RISK_LOWER if text_contains_risk_term(text, term)]

def is_high_grade_risk_research(item):
    """Allow substantive political/security research through the public gate.

    Risk vocabulary describes a subject, not its scholarly value. This keeps
    formal reports, peer-reviewed work and verified long-form analysis while
    continuing to reject incident reporting and sensational event copy.
    """
    if is_public_low_value(item) or is_public_simple_news(item):
        return False
    if is_thin_analytical_news(item) or is_event_or_conference_announcement(item):
        return False
    source = item.get('source', '')
    source_tier = int(item.get('source_tier', 3) or 3)
    credible_form = (
        source in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES | ACADEMIC_SOURCE_NAMES)
        or item.get('source_type') in {'institution_publication', 'academic_paper', 'top_tier_media_discovery'}
        or is_top_tier_media_item(item)
        or is_trusted_deep_discovery_item(item)
    )
    if not credible_form or source_tier > 2:
        return False
    evidence_length = max(
        int(item.get('word_count', 0) or 0),
        len(clean_text(item.get('summary', ''))),
    )
    if evidence_length < 160:
        return False
    return (
        is_strict_deep_public_item(item)
        or is_substantive_policy_document(item)
        or (is_academic_item(item) and item.get('academic_quality') is True)
    )

def is_publication_risky(item):
    if not WECHAT_SAFE_MODE:
        return False
    risk_terms = publication_risk_terms(item)
    high_grade_research = is_high_grade_risk_research(item)
    # Outlet identity is never a blanket exclusion. A source with a strong
    # research product is assessed by document quality, not by its beat.
    if item.get('source') in PUBLICATION_RISK_SOURCES and not high_grade_research:
        return True
    if not risk_terms:
        return False
    if item.get('source') in REPORT_API_SOURCE_NAMES and set(risk_terms) <= {'threat', 'pressure'}:
        return False
    hard_terms = [term for term in risk_terms if term in HARD_PUBLICATION_RISK_LOWER]
    soft_terms = [term for term in risk_terms if term not in HARD_PUBLICATION_RISK_LOWER]
    # High-grade analysis can address difficult subjects. The public gate is
    # concerned with research form and evidentiary depth, not topic avoidance.
    if high_grade_research:
        return False
    # Incident-driven or sensational hard-risk material stays out.
    if hard_terms:
        return True
    # Soft risk (war/afghanistan/sanctions/etc.): allow research-grade deep items,
    # and trusted discovery long-form once original page evidence is available.
    # Non-deep soft-risk news remains internal.
    if soft_terms:
        try:
            if is_research_grade_public_item(item):
                return False
            if (
                is_trusted_deep_discovery_item(item)
                and is_deep_item(item)
                and not is_thin_analytical_news(item)
                and has_strong_central_asia_anchor(item)
                and item.get('core_score', 0) >= 1
                and (
                    item.get('word_count', 0) >= 900
                    or len(clean_text(item.get('summary', ''))) >= 160
                )
                and (
                    item.get('research_score', 0)
                    + item.get('depth_term_score', 0)
                    + item.get('policy_data_score', 0)
                ) >= 2
            ):
                return False
        except Exception:
            pass
        return True
    return False

def is_public_low_value(item):
    text = item_content_text(item)
    if is_generic_item(item):
        return True
    if is_education_promotion(item):
        return True
    if is_news_aggregation_item(item):
        return True
    # Short English tokens need word boundaries so "rain" does not hit "constraints".
    for term in PUBLIC_LOW_VALUE_LOWER:
        if text_contains_risk_term(text, term):
            return True
    return False

def is_event_preview_or_diplomatic_blurb(item):
    """Congress previews, visit/partnership statements, UN agency press releases."""
    title = item_title_text(item)
    summary = clean_text(item.get('summary', ''))
    surface = (title + ' ' + summary[:500]).lower()
    link = (item.get('link') or '').lower()
    source = item.get('source', '') or ''
    publisher = deep_discovery_publisher(source) if is_deep_discovery_source(source) else clean_text(source).lower()

    # Explicit research framing can still pass later gates.
    if any(term in title for term in STRICT_DEEP_FORMAT_LOWER):
        if not re.search(r'\b(congress|conference|exhibition|expo|forum)\b.*\b(to |will )', title, flags=re.I):
            # Keep true analyses even if partnership appears in body.
            if '/press-release' not in link and '/press_releases' not in link:
                return False

    # UN / IFI press-release pages are almost never research-grade for this digest.
    if '/press-release' in link or '/press_releases' in link or '/media-centre/' in link:
        return True
    if any(term in publisher for term in ['unicef', 'undp', 'unhcr', 'who ', 'world health organization']):
        return True

    event_patterns = [
        r'\b(congress|conference|summit|forum|exhibition|expo|roundtable)\b.*\b(to |will |highlights?|to be held|opens?|opening|set to)\b',
        r'\b(to highlight|will highlight|set to host|to be held|will host|to convene)\b',
        r'\b(mining congress|metallurgy congress|amm 20\d{2}|business forum)\b',
        r'大会|展览会|论坛将|将于.*举行|将强调|开幕在即',
    ]
    diplomatic_patterns = [
        r'\bcommit(s|ted)? to (developing |expanding )?(a )?(strategic )?partnership\b',
        r'\b(strategic partnership|mutual political trust|joint statement|issued a statement)\b',
        r'\b(on a visit|working visit|state visit|official visit)\b',
        r'\b(president|minister|premier)\b.*\b(visits?|visited|meets?|met|holds talks|held talks)\b',
        r'\b(visits?|visited|meets?|met|holds talks|held talks)\b.*\b(president|minister|premier)\b',
        r'承诺.*伙伴|发展战略伙伴|建立.*伙伴关系|发表.*声明|工作访问|国事访问|举行会谈',
    ]
    for pattern in event_patterns + diplomatic_patterns:
        if re.search(pattern, surface, flags=re.I):
            # Explanatory long-read titles may still be research.
            if re.search(r'\b(why|how|implications?|limits of|double game|vulnerability|what .* means)\b', title, flags=re.I):
                continue
            if '?' in title and len(title) > 40:
                continue
            return True
    return False

def is_public_simple_news(item):
    """Plain news/deal/visit/personnel blurbs that must not fill the public digest."""
    if is_public_low_value(item) or is_news_aggregation_item(item):
        return True
    if is_thin_analytical_news(item) or is_official_activity_news(item) or is_institute_soft_content(item):
        return True
    # Event previews / diplomatic statements never fill public, even from specialist outlets.
    if is_event_preview_or_diplomatic_blurb(item):
        return True
    # Specialist CA research outlets: substantive pieces are not simple news.
    if specialist_relaxed_longform_ok(item):
        return False
    title = item_title_text(item)
    text_all = item_content_text(item)
    # Explicit research framing survives.
    if any(term in title for term in STRICT_DEEP_FORMAT_LOWER):
        return False
    simple_title_patterns = [
        r'\b(signs?|signed)\b.*\b(deal|agreement|protocol|mou|memorandum)\b',
        r'\b(expands?|expanding|expanded)\b.*\b(ties|relations|cooperation)\b',
        r'\b(keeps? building|builds?|building)\b.*\b(trade|connection|link|ties)\b',
        r'\b(appointed|appointment|named|nominated)\b',
        r'\b(launch|launches|launched)\b.*\b(flight|route|service)\b',
        r'\b(direct flight|air route|air link)\b',
        r'\b(working visit|state visit|official visit|on a visit)\b',
        r'\b(holds? talks?|held talks?|trade talks)\b',
        r'\b(aid|medicine|medical cooperation)\b.*\b(trade talks|talks)\b',
        r'\bcommit(s|ted)? to\b.*\bpartnership\b',
        r'\b(congress|conference|exhibition)\b.*\b(to highlight|will|to be held)\b',
        r'任命|出任|直飞|通航|签署.*协议|扩大.*关系|工作访问|国事访问|发展战略伙伴',
    ]
    for pattern in simple_title_patterns:
        if re.search(pattern, title, flags=re.I):
            return True
    # Media-style discovery/news outlets: require strict deep signals for public.
    source = item.get('source', '')
    publisher = deep_discovery_publisher(source) if is_deep_discovery_source(source) else source.lower()
    is_media = (
        item.get('source_type') != 'institution_publication'
        and any(term in publisher for term in MEDIA_STYLE_DEEP_PUBLISHERS_LOWER)
    )
    if is_media:
        summary_len = len(clean_text(item.get('summary', '')))
        has_format = any(term in title + ' ' + clean_text(item.get('summary', '')).lower() for term in STRICT_DEEP_FORMAT_LOWER)
        explanatory = bool(re.search(r'\b(why|how|what|implications?|highlights?|exposes?|vulnerable|contest|double game|limits of)\b', title, flags=re.I)) or '?' in title
        longform = item.get('word_count', 0) >= 1200 and summary_len >= 160
        if not (has_format or (explanatory and summary_len >= 160) or longform):
            # Short deal/event blurbs from media discovery stay out of public.
            if item.get('research_score', 0) < 2 or summary_len < 180:
                return True
    # Pure personnel/local appointment blurbs.
    if any(term in title for term in ['akim', 'minister of', 'deputy prime', 'governor']):
        if item.get('research_score', 0) < 2 and item.get('depth_term_score', 0) < 1:
            return True
    return False

def is_news_aggregation_item(item):
    title = clean_title(item.get('title', '')).lower()
    text = item_content_text(item)
    return any(term in title or term in text for term in PUBLIC_NEWS_AGGREGATION_LOWER)

def has_public_deep_signal(item):
    text = item_content_text(item)
    if any(term in text for term in PUBLIC_DEEP_SIGNAL_LOWER):
        return True
    if item.get('depth_term_score', 0) >= 1:
        return True
    if item.get('research_score', 0) >= 2:
        return True
    if item.get('source') in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES | set(MEETING_MINUTES_SOURCES) | ACADEMIC_SOURCE_NAMES):
        return True
    return False

def is_deep_discovery_source(source):
    return (source or '').startswith('Deep Discovery: Google News')

def is_report_grade_item(item):
    source = item.get('source', '')
    if source in COUNTRY_ASSESSMENT_SOURCE_NAMES or is_country_assessment_item(item):
        return has_strong_central_asia_anchor(item)
    if source in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES):
        # PDF/report sources still need title/link-level Central Asia relevance.
        title_link = (item_title_text(item) + ' ' + link_relevance_text(item.get('link', ''))).lower()
        if source in REPORT_API_SOURCE_NAMES:
            return True
        return any(term in title_link for term in STRONG_CA_ANCHOR_LOWER)
    link_path = urllib.parse.urlparse(item.get('link', '')).path.lower()
    formal_publication_paths = ['/papers/', '/policy-brief', '/working-paper', '/reports/']
    if source in (DEEP_ANALYSIS_SOURCES | CENTRAL_ASIA_SPECIALIST_SOURCES) or is_deep_discovery_source(source):
        text = item_content_text(item)
        if has_strong_central_asia_anchor(item) and (
            any(term in text for term in REPORT_CLASSIFICATION_LOWER)
            or any(path in link_path for path in formal_publication_paths)
        ):
            return True
    return False

def item_title_text(item):
    return clean_title(item.get('title', '')).lower()

def is_institute_soft_content(item):
    """Institutional PR, event notices and soft lifestyle posts from policy institutes."""
    source = item.get('source', '')
    institute_sources = LOCAL_POLICY_INSTITUTE_SOURCES | {
        'KISI KazISS RSS', 'KISI KazISS Analytics', 'NISI Kyrgyzstan',
        'Tajik CSR Analytical Articles',
    }
    if source not in institute_sources and 'KazISS' not in source and 'KISI' not in source:
        return False
    title = item_title_text(item)
    soft_title_terms = [
        'mom, who is', 'participated in', 'met with', 'made a speech',
        'presented expert', 'discussed', 'visited', 'hosted', 'welcomed',
        'roundtable announcement', 'congratulations', 'anniversary',
        'interactive survey', 'interactive poll', 'survey results', 'poll results',
        '参加', '会见', '发表讲话', '访问', '祝贺',
    ]
    if any(term in title for term in soft_title_terms):
        # Keep only if clearly a research product.
        if not any(term in title for term in STRICT_DEEP_FORMAT_LOWER):
            return True
    return False

def is_official_activity_news(item):
    source = item.get('source', '')
    if (
        source not in OFFICIAL_POLICY_SOURCES
        and source not in MEETING_MINUTES_SOURCES
        and source not in LOCAL_POLICY_INSTITUTE_SOURCES
    ):
        return False
    title = item_title_text(item)
    if any(term in title for term in SUBSTANTIVE_POLICY_DOCUMENT_LOWER):
        return False
    return any(term in title for term in OFFICIAL_ACTIVITY_TITLE_LOWER)

def is_substantive_policy_document(item):
    source = item.get('source', '')
    if (
        source not in OFFICIAL_POLICY_SOURCES
        and source not in MEETING_MINUTES_SOURCES
        and source not in LOCAL_POLICY_INSTITUTE_SOURCES
    ):
        return False
    if is_official_activity_news(item):
        return False
    title = item_title_text(item)
    return (
        any(term in title for term in SUBSTANTIVE_POLICY_DOCUMENT_LOWER)
        and item.get('policy_data_score', 0) >= 1
        and (has_strong_central_asia_anchor(item) or source in NATIONAL_OFFICIAL_SOURCES)
    )

def deep_discovery_publisher(source):
    if not is_deep_discovery_source(source):
        return ''
    if '｜' in source:
        return clean_text(source.split('｜', 1)[-1]).lower()
    if '|' in source:
        return clean_text(source.split('|', 1)[-1]).lower()
    return ''


SOURCE_KEY_ALIASES = {
    'times of central asia': 'the times of central asia',
    'the times of central asia': 'the times of central asia',
    'the diplomat asia pacific': 'the diplomat',
    'the diplomat central asia': 'the diplomat',
    'the diplomat china central asia': 'the diplomat',
    'central asia program wilson center': 'central asia program',
    'kisi kaziss analytics': 'kisi kaziss',
    'kisi kaziss': 'kisi kaziss',
    'kazakhstan institute for strategic studies': 'kisi kaziss',
    'dialogue earth web': 'dialogue earth',
    'third pole': 'the third pole',
    'the third pole': 'the third pole',
    'radio free europe radio liberty': 'rfe rl',
    'rfe rl central asia': 'rfe rl',
    'observer research foundation': 'orfonline',
    'orfonline org': 'orfonline',
    'central asia caucasus analyst': 'central asia caucasus analyst',
    'cacianalyst': 'central asia caucasus analyst',
}


def canonical_source_name(source):
    text = clean_text(source or '').lower()
    text = text.replace('&', ' and ')
    text = re.sub(r'[^a-z0-9а-яё\u4e00-\u9fff]+', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'\s+(rss|web|english)$', '', text).strip()
    return SOURCE_KEY_ALIASES.get(text, text)


def is_academic_item(item):
    return item.get('source', '') in ACADEMIC_SOURCE_NAMES or item.get('academic_quality') is True


def public_source_key(item):
    """Canonical publisher/venue identity used for diversity and source caps."""
    if is_academic_item(item):
        venue = canonical_source_name(item.get('academic_venue', ''))
        provider = canonical_source_name(item.get('source', ''))
        return 'academic:' + (venue or provider or 'unknown')
    source = item.get('source', '')
    publisher = deep_discovery_publisher(source)
    canonical = canonical_source_name(publisher or source)
    return 'source:' + (canonical or 'unknown')


def published_ordinal(item):
    date_value = parse_item_published_date(item)
    if date_value:
        return date_value.toordinal()
    year = parse_item_publication_year(item)
    return year * 366 if year else 0


def same_run_variant_score(item):
    """Prefer the richest copy when the same article arrives through several paths."""
    summary_len = len(clean_text(item.get('summary', '')))
    return (
        1 if has_verifiable_publication_time(item) else 0,
        1 if not is_google_news_url(item.get('link', '')) else 0,
        min(summary_len, 500),
        int(item.get('word_count', 0) or 0),
        1 if clean_text(item.get('content_type', '')) else 0,
        item.get('research_score', 0) + item.get('depth_term_score', 0) + item.get('policy_data_score', 0),
    )


def diversify_ranked_items(
    items,
    limit,
    min_unique_sources=0,
    max_per_source=MAX_PUBLIC_ITEMS_PER_SOURCE,
    max_academic_items=None,
    max_per_academic_venue=MAX_PUBLIC_ITEMS_PER_ACADEMIC_VENUE,
):
    """Keep ranking order while reserving space for distinct real publishers."""
    if limit <= 0:
        return []
    ranked = list(items or [])
    selected = []
    selected_keys = set()
    source_counts = {}
    academic_count = 0

    def can_add(item):
        nonlocal academic_count
        item_key = normalize_key(item)
        if not item_key or item_key in selected_keys:
            return False
        source_key = public_source_key(item)
        source_limit = max_per_source
        if is_academic_item(item):
            source_limit = min(source_limit, max_per_academic_venue)
            if max_academic_items is not None and academic_count >= max_academic_items:
                return False
        if source_counts.get(source_key, 0) >= source_limit:
            return False
        selected.append(item)
        selected_keys.add(item_key)
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if is_academic_item(item):
            academic_count += 1
        return True

    unique_target = min(max(0, min_unique_sources), limit)
    if unique_target:
        for item in ranked:
            if len(source_counts) >= unique_target or len(selected) >= limit:
                break
            if source_counts.get(public_source_key(item), 0) > 0:
                continue
            can_add(item)

    for item in ranked:
        if len(selected) >= limit:
            break
        can_add(item)
    return selected

def extend_public_shortfall(selected, eligible, minimum=MIN_PUBLIC_RECOMMENDATIONS):
    """Add at most one extra item per authority when a deep-only pool is short."""
    result = list(selected or [])
    if len(result) >= minimum:
        return result
    selected_keys = {normalize_key(item) for item in result}
    source_counts = {}
    academic_count = 0
    for item in result:
        source_key = public_source_key(item)
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if is_academic_item(item):
            academic_count += 1
    for item in eligible or []:
        if len(result) >= minimum:
            break
        item_key = normalize_key(item)
        if not item_key or item_key in selected_keys:
            continue
        if source_counts.get(public_source_key(item), 0) >= SHORTFALL_MAX_PUBLIC_ITEMS_PER_SOURCE:
            continue
        if is_academic_item(item) and academic_count >= MAX_PUBLIC_ACADEMIC_ITEMS:
            continue
        result.append(item)
        selected_keys.add(item_key)
        source_key = public_source_key(item)
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if is_academic_item(item):
            academic_count += 1
    return result

def is_trusted_deep_discovery_item(item):
    source = item.get('source', '')
    if not is_deep_discovery_source(source):
        return False
    publisher = deep_discovery_publisher(source)
    if not publisher:
        return False
    return any(term in publisher for term in DEEP_DISCOVERY_TRUSTED_LOWER)


def is_thin_analytical_news(item):
    """Specialized news/event blurbs that look topical but are not deep analysis."""
    title = item_title_text(item)
    text = item_content_text(item)
    if any(term in title for term in STRICT_DEEP_FORMAT_LOWER):
        return False
    if any(term in title for term in THIN_ANALYTICAL_NEWS_TITLE_LOWER):
        return True
    # Short deal/meeting style headlines without analysis framing.
    thin_event_patterns = [
        r'\bholds?\b.*\b(dialogue|talks|meeting)\b',
        r'\b(signs?|signed)\b.*\b(deal|agreement|protocol|mou)\b',
        r'\b(ambassador|minister|officials?)\b.*\b(urges?|calls?|meets?)\b',
        r'\b(enterprise|company|venture)\b.*\b(progress|advances?)\b',
        r'\b(congress|conference|exhibition)\b.*\b(highlight|to be held|will host)\b',
        r'\bcommit(s|ted)? to\b.*\bpartnership\b',
    ]
    for pattern in thin_event_patterns:
        if re.search(pattern, title):
            return True
    # Media discovery/specialist items need either explicit deep framing
    # or explanatory longform signals; plain deal/meeting news stays out.
    source = item.get('source', '')
    publisher = deep_discovery_publisher(source) if is_deep_discovery_source(source) else source.lower()
    is_media_style = (
        item.get('source_type') != 'institution_publication'
        and any(term in publisher for term in MEDIA_STYLE_DEEP_PUBLISHERS_LOWER)
    )
    if is_media_style and not any(term in title for term in STRICT_DEEP_FORMAT_LOWER):
        summary_len = len(clean_text(item.get('summary', '')))
        explanatory = bool(re.search(r'\b(why|how|what|will|whether|implications?|highlights?|exposes?|vulnerable|vulnerability|fallout|contest|double game)\b', title)) or '?' in title
        strong_enough = (
            item.get('research_score', 0) >= 2
            and summary_len >= 160
        ) or (
            item.get('depth_term_score', 0) >= 1
            and item.get('research_score', 0) >= 1
            and summary_len >= 160
        )
        explanatory_enough = (
            explanatory
            and item.get('research_score', 0) >= 1
            and summary_len >= 140
        )
        structural_enough = (
            summary_len >= 180
            and item.get('core_score', 0) >= 1
            and any(term in title + ' ' + text for term in [
                'corridor', 'trade route', 'structural', 'vulnerability',
                'geograph', 'strategy', 'strategic', 'fallout', 'contest',
                '走廓', '通道', '结构性', '战略',
            ])
        )
        if not (strong_enough or explanatory_enough or structural_enough):
            return True
    return False

def is_event_or_conference_announcement(item):
    """Block pure conference/event notices from public deep-read slots."""
    title = item_title_text(item)
    text = item_content_text(item)
    link_path = urllib.parse.urlparse(item.get('link', '')).path.lower()
    event_title_terms = [
        'annual conference', 'international conference', 'conference:',
        'call for papers', 'save the date', 'registration open',
        'will host', 'hosts conference', 'held a conference',
        'roundtable announcement', 'webinar', 'symposium', 'summer school',
        'alumni reunion', 'alumni gathering', 'training workshop',
        'capacity-building workshop', 'study tour', 'award ceremony',
        '年度会议', '国际会议', '研讨会', '征稿', '报名',
        '暑期学校', '校友会', '培训班', '培训研讨会', '颁奖典礼',
        'конференция', 'симпозиум', 'вебинар',
    ]
    partnership_notice_terms = [
        'deepens partnership', 'strengthens partnership', 'partnership through',
        'innovation programme', 'innovation program', 'signs agreement',
        'signed agreement', 'memorandum of understanding', 'mou with',
        'cooperation programme', 'cooperation program', 'joint initiative',
        '深化合作', '签署协议', '合作伙伴关系', '合作项目',
    ]
    analysis_terms = [
        'analysis', 'expert views', 'commentary', 'findings', 'report',
        'study', 'assessment', 'policy brief', 'working paper',
        '分析', '评论', '报告', '研究', '评估', '政策简报',
    ]
    if item.get('source_type') == 'institution_publication':
        institution_soft_patterns = [
            r'\b(hosts?|hosted|welcomes?|welcomed|receives?|received)\b.*\bdelegation\b',
            r'\bdelegation\b.*\b(visits?|visited|meets?|met)\b',
            r'\b(project|programme|program|initiative)\s*$',
            r'\b(research|academic)\s+(project|programme|program|initiative)\b',
        ]
        soft_archive_paths = [
            '/research-initiatives/', '/research-initiative/',
            '/projects/', '/project/', '/programmes/', '/programs/',
        ]
        has_title_analysis = any(term in title for term in analysis_terms)
        if not has_title_analysis and any(re.search(pattern, title) for pattern in institution_soft_patterns):
            return True
        if not has_title_analysis and any(path in link_path for path in soft_archive_paths):
            return True
        if '/news/' in link_path and not has_title_analysis:
            return True
    # Dedicated event pages and institutional community notices are not deep reads.
    if '/events/' in link_path and not any(term in title for term in analysis_terms):
        return True
    # Title-led event notices without analysis framing.
    if any(term in title for term in event_title_terms):
        if not any(term in title for term in analysis_terms):
            return True
    if any(term in title for term in partnership_notice_terms):
        if not any(term in title for term in analysis_terms):
            return True
    # Generic conference spam in discovery results.
    if 'conference' in title and not any(term in title for term in [
        'analysis', 'expert', 'report', 'study', 'policy',
    ]):
        if item.get('depth_term_score', 0) < 2 and item.get('research_score', 0) < 2:
            return True
    return False

SPECIALIST_RELAXED_FORMAT_SOURCES = {
    'Caspian Policy Center RSS', 'Caspian Policy Center',
    'Central Asia Program RSS', 'Central Asia Program Policy Briefs',
    'Central Asia Program (Wilson Center)',
    'Central Asia-Caucasus Analyst',
    'The Times of Central Asia',
    'Novastan English',
    'Eurasianet',
    'CABAR.asia',
    'bne IntelliNews Central Asia',
    'KISI KazISS RSS', 'CAPS Unlock RSS',
    'Voices on Central Asia',
    'The Diplomat Central Asia', 'The Diplomat',
    'Dialogue Earth', 'The Third Pole', 'IWPR Central Asia',
    'Oxus Society', 'Oxus Society RSS', 'ISRS Uzbekistan',
    'IISS Online Analysis', 'CAPS Unlock Publications',
}

SPECIALIST_RELAXED_PUBLISHERS_LOWER = [
    'caspian policy', 'central asia program', 'cacianalyst',
    'central asia-caucasus analyst', 'times of central asia',
    'novastan', 'eurasianet', 'cabar', 'bne', 'intellinews',
    'kisi', 'caps unlock', 'the diplomat', 'voices on central asia',
    'dialogue earth', 'third pole', 'iwpr', 'oxus', 'isrs',
    'iiss', 'merics', 'kennan',
]

def is_specialist_relaxed_format_item(item):
    """Top CA specialist outlets: do not require explicit analysis/report in title."""
    source = item.get('source', '') or ''
    if source in SPECIALIST_RELAXED_FORMAT_SOURCES:
        return True
    if is_deep_discovery_source(source):
        publisher = deep_discovery_publisher(source)
        return any(term in publisher for term in SPECIALIST_RELAXED_PUBLISHERS_LOWER)
    publisher = clean_text(item.get('publisher', '') or source).lower()
    return any(term in publisher for term in SPECIALIST_RELAXED_PUBLISHERS_LOWER)

def specialist_relaxed_longform_ok(item):
    if not is_specialist_relaxed_format_item(item):
        return False
    if is_thin_analytical_news(item) or is_official_activity_news(item) or is_institute_soft_content(item):
        return False
    if is_event_preview_or_diplomatic_blurb(item):
        return False
    if not has_strong_central_asia_anchor(item):
        return False
    summary_len = len(clean_text(item.get('summary', '')))
    word_count = int(item.get('word_count', 0) or 0)
    signal = (
        item.get('research_score', 0)
        + item.get('depth_term_score', 0)
        + item.get('policy_data_score', 0)
    )
    if word_count >= 700 and item.get('core_score', 0) >= 1:
        return True
    if summary_len >= 160 and item.get('core_score', 0) >= 1 and signal >= 1:
        return True
    if summary_len >= 200 and item.get('core_score', 0) >= 1:
        return True
    return False

def is_strict_deep_public_item(item):
    source = item.get('source', '')
    if item.get('access_status') == 'paywalled':
        return False
    publisher_surface = clean_text(item.get('publisher', '') or source).lower()
    if any(term in publisher_surface for term in DISCOVERY_LOW_PRIORITY_PUBLISHERS):
        target_surface = (item_title_text(item) + ' ' + clean_text(item.get('summary', '')).lower())
        matched_terms = {term for term in FRONTIERS_RELEVANCE_TERMS if term in target_surface}
        if not (matched_terms - FRONTIERS_COUNTRY_TERMS):
            return False
    if not has_verifiable_publication_time(item) or not has_strong_central_asia_anchor(item):
        return False
    if is_news_aggregation_item(item) or is_official_activity_news(item):
        return False
    if is_event_or_conference_announcement(item):
        return False
    if is_thin_analytical_news(item):
        return False
    if is_country_assessment_item(item) and item.get('source_type') == 'institution_publication':
        return (
            item.get('access_status') not in {'paywalled', 'blocked'}
            and (
                int(item.get('word_count', 0) or 0) >= 700
                or len(clean_text(item.get('summary', ''))) >= 160
            )
        )
    # A top-tier media feature can be deep even when the headline does not use
    # the words "analysis" or "report". Require original-page evidence instead
    # of headline vocabulary: readable body, strong regional anchor, and at
    # least one research signal.
    if (
        item.get('source_type') == 'top_tier_media_discovery'
        and not is_event_or_conference_announcement(item)
        and has_strong_central_asia_anchor(item)
        and int(item.get('word_count', 0) or 0) >= 650
        and (
            item.get('research_score', 0)
            + item.get('depth_term_score', 0)
            + item.get('policy_data_score', 0)
        ) >= 1
    ):
        return True
    if source in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES):
        return True
    if source in ACADEMIC_SOURCE_NAMES:
        return item.get('academic_quality') is True
    title = item_title_text(item)
    summary_text = clean_text(item.get('summary', '')).lower()
    summary_len = len(clean_text(item.get('summary', '')))
    # Prestige/specialist RSS often puts "policy brief/report" in the excerpt, not the title.
    format_surface = title
    if source in (PRESTIGE_LONGFORM_SOURCES | HIGH_SIGNAL_DEEP_SOURCES | CENTRAL_ASIA_SPECIALIST_SOURCES):
        format_surface = (title + ' ' + summary_text).strip()
    has_explicit_format = any(term in format_surface for term in STRICT_DEEP_FORMAT_LOWER)
    content_type = clean_text(item.get('content_type', '')).lower()
    has_metadata_format = any(term in content_type for term in PRESTIGE_DEEP_METADATA_LOWER)
    has_summary_format = any(term in summary_text for term in STRICT_DEEP_FORMAT_LOWER)
    # Search/archive adapters for major institutions identify original
    # publication pages. Judge these as institutional research, not ordinary
    # web news, while still requiring substantive original-page evidence.
    is_verified_institution_publication = (
        item.get('source_type') == 'institution_publication'
        and (
            source in DURABLE_PRESTIGE_DISCOVERY_SOURCES
            or is_neighbor_institution_publisher_text(item.get('publisher', ''))
        )
        and int(item.get('source_tier', 3) or 3) <= 2
        and item.get('access_status') not in {'paywalled', 'blocked'}
        and (
            (
                (has_metadata_format or has_explicit_format)
                and (
                    int(item.get('word_count', 0) or 0) >= 500
                    or summary_len >= 160
                )
            )
            or (
                int(item.get('word_count', 0) or 0) >= 650
                and (
                    item.get('research_score', 0)
                    + item.get('depth_term_score', 0)
                    + item.get('policy_data_score', 0)
                ) >= 1
            )
            or (
                summary_len >= 160
                and item.get('research_score', 0) >= 1
                and (
                    item.get('depth_term_score', 0) >= 1
                    or item.get('policy_data_score', 0) >= 1
                )
            )
        )
    )
    # Verified prestige longform: do not require full-page word_count for stable RSS excerpts.
    is_verified_prestige_longform = (
        source in PRESTIGE_LONGFORM_SOURCES
        and summary_len >= 160
        and (
            has_metadata_format
            or item.get('word_count', 0) >= 1000
            or has_summary_format
            or (
                has_explicit_format
                and item.get('research_score', 0) + item.get('depth_term_score', 0) >= 2
            )
        )
        and not is_thin_analytical_news(item)
    )
    # Trusted discovery: explicit deep framing, or strong research signals.
    # Specialist outlets (CACI Analyst etc.) often lack "analysis" lexical hits in
    # short RSS/GN snippets; do not require depth_term_score when research is strong.
    is_trusted_discovery_longform = (
        is_trusted_deep_discovery_item(item)
        and not is_thin_analytical_news(item)
        and (
            has_explicit_format
            or (
                item.get('research_score', 0) >= 2
                and summary_len >= 160
                and item.get('core_score', 0) >= 1
                and has_strong_central_asia_anchor(item)
            )
            or (
                item.get('depth_term_score', 0) >= 1
                and item.get('research_score', 0) >= 1
                and summary_len >= 180
                and has_strong_central_asia_anchor(item)
            )
            or (
                # Structural corridor / regional-order longform from trusted discovery:
                # allow when summary is long and policy/research signals combine.
                summary_len >= 160
                and item.get('core_score', 0) >= 1
                and has_strong_central_asia_anchor(item)
                and (
                    item.get('research_score', 0)
                    + item.get('depth_term_score', 0)
                    + item.get('policy_data_score', 0)
                ) >= 2
                and any(term in (title + ' ' + summary_text) for term in [
                    'corridor', 'trade route', 'structural', 'vulnerability',
                    'geograph', 'strategy', 'strategic', 'fallout', 'contest',
                    'double game', 'turn to the east', 'implications',
                    '走廓', '通道', '结构性', '战略', '影响',
                ])
            )
            or (
                # After pre-gate enrich: long original articles from trusted discovery.
                item.get('word_count', 0) >= 900
                and item.get('core_score', 0) >= 1
                and has_strong_central_asia_anchor(item)
                and (
                    item.get('research_score', 0) >= 1
                    or item.get('depth_term_score', 0) >= 1
                    or item.get('policy_data_score', 0) >= 1
                )
            )
            or (
                source in PRESTIGE_LONGFORM_SOURCES
                and summary_len >= 200
                and item.get('research_score', 0) >= 2
            )
        )
    )
    is_specialist_relaxed = specialist_relaxed_longform_ok(item)
    if (
        not has_explicit_format
        and not is_verified_prestige_longform
        and not is_verified_institution_publication
        and not is_trusted_discovery_longform
        and not is_specialist_relaxed
    ):
        return False
    if source in OFFICIAL_POLICY_SOURCES or source in MEETING_MINUTES_SOURCES:
        return is_substantive_policy_document(item)
    if source in LOCAL_AND_OFFICIAL_SOURCES and source not in OFFICIAL_POLICY_SOURCES:
        return len(clean_text(item.get('summary', ''))) >= 120
    return (
        source in DEEP_ANALYSIS_SOURCES
        or source in CENTRAL_ASIA_SPECIALIST_SOURCES
        or is_verified_institution_publication
        or is_deep_discovery_source(source)
    )

def is_public_deep_or_reference_grade(item):
    source = item.get('source', '')
    summary = clean_text(item.get('summary', ''))
    if is_news_aggregation_item(item):
        return False
    if source in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES | set(MEETING_MINUTES_SOURCES) | ACADEMIC_SOURCE_NAMES):
        return True
    if is_deep_discovery_source(source):
        return has_public_deep_signal(item) and not is_news_aggregation_item(item)
    if source in OFFICIAL_POLICY_SOURCES:
        return item.get('policy_data_score', 0) >= 2 and (item.get('research_score', 0) >= 1 or bool(item.get('priority_topics', [])))
    if is_deep_item(item):
        return has_public_deep_signal(item) and len(summary) >= 120
    return False

def is_internal_low_value_news(item):
    text = item_content_text(item)
    title = item_title_text(item)
    if any(term in text for term in STRICT_INTERNAL_LOW_VALUE_LOWER):
        return True
    # Pure weather/heat-record blurbs are never research-grade, even if
    # temperature keywords accidentally trigger climate priority tags.
    weather_terms = [
        'heat record', 'temperature record', 'record heat', 'record high',
        'heatwave', 'heat wave', 'weather forecast', 'air temperature',
        '高温纪录', '气温纪录', '创纪录高温', '热浪', '天气预报', '气温',
        'температура', 'жара', 'аптап', 'ауа температурасы',
        '°c', 'градус',
    ]
    if any(term in title or term in text for term in weather_terms):
        if item.get('research_score', 0) < 1 and item.get('depth_term_score', 0) < 1:
            if not any(term in text for term in [
                'climate change', 'water stress', 'drought', 'glacier',
                'hydropower', 'irrigation', 'water management',
                '气候变化', '水压力', '干旱', '冰川', '水电', '灌溉', '水资源管理',
            ]):
                return True
    # Pure presidential/leader travel blurbs without policy substance.
    visit_terms = [
        'working visit', 'state visit', 'official visit', 'on a visit to',
        'is visiting', 'arrived in', 'departed for', 'left for',
        '工作访问', '国事访问', '正式访问', '出访', '正在访问',
        'рабочий визит', 'государственный визит', 'жұмыс сапары',
    ]
    if any(term in title for term in visit_terms):
        if item.get('research_score', 0) < 1 and item.get('depth_term_score', 0) < 1:
            if item.get('policy_data_score', 0) < 2:
                return True
    if any(term in text for term in INTERNAL_LOW_VALUE_NEWS_LOWER):
        if (
            item.get('research_score', 0) < 1
            and item.get('depth_term_score', 0) < 1
        ):
            return True
    return False

def has_public_conversion_exclusion(item):
    text = item_content_text(item)
    for term in PUBLIC_CONVERSION_EXCLUDE_LOWER:
        if re.fullmatch(r'[a-z0-9 ]+', term):
            if re.search(r'\b' + re.escape(term) + r'\b', text):
                return True
        elif term in text:
            return True
    return False

def has_local_context(source):
    return source in LOCAL_AND_OFFICIAL_SOURCES or source in CENTRAL_ASIA_SPECIALIST_SOURCES

def has_meaningful_signal(source, core_score, research_score, depth_term_score, keyword_score, policy_data_score, event_signal_score):
    if core_score >= 1 and (research_score >= 1 or depth_term_score >= 1 or keyword_score >= 1):
        return True
    if source in NATIONAL_OFFICIAL_SOURCES and policy_data_score >= 1:
        return True
    if source in CENTRAL_ASIA_POLICY_SOURCES and (policy_data_score >= 1 or event_signal_score >= 1):
        return True
    if source in REGIONAL_POLICY_SOURCES and core_score >= 1 and policy_data_score >= 1:
        return True
    if source in CENTRAL_ASIA_SPECIALIST_SOURCES and (research_score >= 1 or depth_term_score >= 1 or keyword_score >= 1):
        return True
    if source in LOCAL_NEWS_CONTEXT_SOURCES and (research_score >= 1 or policy_data_score >= 1 or event_signal_score >= 1):
        return True
    return False

def is_policy_data_item(item):
    source = item.get('source', '')
    if is_generic_item(item):
        return False
    policy_data_score = item.get('policy_data_score', 0)
    core_score = item.get('core_score', 0)
    if source in NATIONAL_OFFICIAL_SOURCES:
        return policy_data_score >= 1 or core_score >= 1
    if source in CENTRAL_ASIA_POLICY_SOURCES:
        return policy_data_score >= 1 or core_score >= 1
    if source in REGIONAL_POLICY_SOURCES:
        return core_score >= 1 and policy_data_score >= 1
    return False

def is_deep_item(item):
    source = item.get('source', '')
    if is_generic_item(item):
        return False
    core_score = item.get('core_score', 0)
    research_score = item.get('research_score', 0)
    depth_term_score = item.get('depth_term_score', 0)
    if source not in DEEP_ANALYSIS_SOURCES and not is_deep_discovery_source(source):
        return False
    if core_score < 1 and source not in CENTRAL_ASIA_SPECIALIST_SOURCES and not is_deep_discovery_source(source):
        return False
    if (source in DEEP_ANALYSIS_SOURCES or is_deep_discovery_source(source)) and (depth_term_score >= 1 or research_score >= 1 or source in CENTRAL_ASIA_SPECIALIST_SOURCES):
        return True
    return depth_term_score >= 1 and research_score >= 1 and core_score >= 1

def is_research_grade_public_item(item):
    if not is_recent_item(item) or not has_verifiable_publication_time(item):
        return False
    if is_news_aggregation_item(item) or is_public_low_value(item) or is_public_simple_news(item):
        return False
    if is_thin_analytical_news(item) or is_institute_soft_content(item):
        return False
    source = item.get('source', '')
    if source in BROAD_REGIONAL_DEEP_SOURCES and not has_strong_central_asia_anchor(item):
        return False
    # Public digest is deep-only: structural CA anchor required.
    if not has_strong_central_asia_anchor(item):
        return False
    if is_strict_deep_public_item(item):
        return True
    if is_substantive_policy_document(item):
        return True
    return False
def is_internal_review_grade_item(item):
    """Internal review stores research-value items unfit for public WeChat body.

    It is NOT a dump for ordinary news. Simple news should be discarded, not archived.
    """
    if not is_recent_item(item):
        return False
    if is_generic_item(item) or is_public_low_value(item) or is_public_simple_news(item):
        return False
    if is_internal_low_value_news(item) or is_thin_analytical_news(item):
        return False
    if is_institute_soft_content(item) or is_official_activity_news(item):
        return False
    if is_stale_low_signal_internal_item(item):
        return False
    source = item.get('source', '')
    if source in BROAD_REGIONAL_DEEP_SOURCES and not has_strong_central_asia_anchor(item):
        return False
    if not has_strong_central_asia_anchor(item):
        return False
    # Only deep research products that would otherwise be public-grade.
    if is_research_grade_public_item(item):
        return True
    if is_strict_deep_public_item(item):
        return True
    # Trusted specialist longform blocked only by risk framing, with original evidence.
    if (
        is_trusted_deep_discovery_item(item)
        and is_deep_item(item)
        and not is_thin_analytical_news(item)
        and (
            item.get('word_count', 0) >= 900
            or len(clean_text(item.get('summary', ''))) >= 180
        )
        and (
            item.get('research_score', 0)
            + item.get('depth_term_score', 0)
            + item.get('policy_data_score', 0)
        ) >= 2
    ):
        return True
    # Formal reports / academic even when risk-tagged.
    if source in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES | ACADEMIC_SOURCE_NAMES):
        return item.get('research_score', 0) >= 1 or item.get('policy_data_score', 0) >= 1
    return False

def is_stale_low_signal_internal_item(item):
    source = item.get('source', '')
    if source in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES | set(MEETING_MINUTES_SOURCES) | ACADEMIC_SOURCE_NAMES):
        return False
    if source in HIGH_SIGNAL_DEEP_SOURCES or is_deep_item(item):
        return False
    item_date = parse_item_published_date(item)
    if not item_date:
        return source in NATIONAL_OFFICIAL_SOURCES
    age_days = (TODAY - item_date).days
    if age_days <= INTERNAL_FAST_SIGNAL_MAX_AGE_DAYS:
        return False
    if source in NATIONAL_OFFICIAL_SOURCES:
        return True
    if source in LOCAL_AND_OFFICIAL_SOURCES and item.get('depth_term_score', 0) < 1 and item.get('research_score', 0) < 1:
        return True
    return False

def is_public_convertible_internal_item(item):
    if not is_internal_review_grade_item(item):
        return False
    if not parse_item_published_date(item):
        return False
    if has_public_conversion_exclusion(item):
        return False
    # Conversion is a cautious public bridge for deep research only, never thin news.
    if is_public_low_value(item) or is_public_simple_news(item) or is_thin_analytical_news(item) or is_official_activity_news(item):
        return False
    if not is_strict_deep_public_item(item) and not is_research_grade_public_item(item):
        return False
    title = item_title_text(item)
    if any(term in title for term in [
        'flight', 'airline', 'airport', 'direct flight', 'launches flight',
        '航班', '通航', '直飞', '航线',
    ]):
        return False
    source = item.get('source', '')
    if source not in PUBLIC_CONVERTIBLE_INTERNAL_SOURCES:
        return False
    tags = item_public_tags(item)
    priority_topics = item.get('priority_topics', [])
    if any(tag in tags for tag in ['经济能源', '水资源气候', '对外关系']):
        return True
    if any(topic in priority_topics for topic in ['中间走廊与互联互通', '关键矿产与能源转型', '大国关系与多向量外交']):
        return True
    title_text = item_content_text(item)
    if source in {'The Diplomat', 'Vlast.kz'} and 'kazakhstan' in title_text and ('constitution' in title_text or 'constitutional' in title_text):
        return True
    return False

def is_normal_public_conversion_item(item):
    if not is_public_convertible_internal_item(item):
        return False
    tags = item_public_tags(item)
    priority_topics = item.get('priority_topics', [])
    text = item_content_text(item)
    if any(tag in tags for tag in ['区域稳定', '安全防务', '政治治理']):
        return False
    if any(tag in tags for tag in NORMALIZED_PUBLIC_CONVERSION_TAGS):
        return True
    if any(topic in priority_topics for topic in NORMALIZED_PUBLIC_CONVERSION_TOPICS):
        return True
    for term in NORMALIZED_PUBLIC_CONVERSION_LOWER:
        if re.fullmatch(r'[a-z0-9 ]+', term):
            if re.search(r'\b' + re.escape(term) + r'\b', text):
                return True
        elif term in text:
            return True
    return False

def normal_public_conversion_pool(internal_review_items):
    return [item for item in internal_review_items if is_normal_public_conversion_item(item)]

def cautious_public_conversion_pool(internal_review_items):
    return [item for item in internal_review_items if not is_normal_public_conversion_item(item)]

def classify_item_framework(item):
    """Assign orthogonal research-information labels used by filters/renderers."""
    source = clean_text(item.get('source', ''))
    source_lower = source.lower()
    source_type = clean_text(item.get('source_type', ''))
    if source_type == 'academic_paper' or source in ACADEMIC_SOURCE_NAMES:
        evidence_type, document_form = 'academic_paper', 'journal_article'
    elif source_type == 'institution_publication' or source in PDF_REPORT_SOURCES or source in REPORT_API_SOURCE_NAMES:
        evidence_type = 'institutional_analysis'
        kind = clean_text(item.get('institution_publication_kind', '')).lower()
        document_form = 'policy_brief' if 'brief' in kind or 'memo' in kind else 'report'
    elif source in MEETING_MINUTES_SOURCES:
        evidence_type, document_form = 'meeting_record', 'meeting_record'
    elif source_type == 'top_tier_media_discovery' or is_top_tier_media_item(item):
        evidence_type, document_form = 'media_investigation', 'deep_media'
    elif source_type == 'discovery' or is_deep_discovery_source(source):
        evidence_type, document_form = 'discovery_lead', 'news'
    elif source in OFFICIAL_POLICY_SOURCES or source in NATIONAL_OFFICIAL_SOURCES:
        evidence_type, document_form = 'primary_official', 'legal_text' if any(term in source_lower for term in ['law', 'parliament', 'court', 'legal']) else 'report'
    elif source in LOCAL_AND_OFFICIAL_SOURCES or source in TELEGRAM_SOURCES:
        evidence_type, document_form = 'local_observation', 'news'
    else:
        evidence_type, document_form = 'media_investigation' if is_deep_item(item) else 'discovery_lead', 'deep_media' if is_deep_item(item) else 'news'

    tags = item_public_tags(item)
    priority_topics = item.get('priority_topics', [])
    if document_form == 'journal_article':
        research_function = 'literature_review'
    elif document_form in {'dataset', 'legal_text'} or evidence_type == 'primary_official':
        research_function = 'data_reference' if document_form == 'dataset' else 'policy_evaluation'
    elif any(tag in tags for tag in ['治理动态', '区域稳定', '对外关系']):
        research_function = 'policy_evaluation'
    elif any(topic in priority_topics for topic in ['水资源与气候约束', '关键矿产与能源转型', '中间走廊与互联互通']):
        research_function = 'causal_analysis'
    elif evidence_type == 'institutional_analysis' or evidence_type == 'media_investigation':
        research_function = 'causal_analysis'
    else:
        research_function = 'situational_awareness'

    geography = [tag for tag in tags if tag in COUNTRY_TAGS]
    if not geography:
        geography = ['区域综合'] if any(term in item_content_text(item) for term in ['central asia', '中亚', 'средняя азия']) else ['待核定']
    if evidence_type == 'academic_paper':
        output_channel = 'academic_bibliography'
    elif evidence_type == 'institutional_analysis':
        output_channel = 'institution_library'
    elif evidence_type == 'primary_official' and document_form in {'dataset', 'legal_text'}:
        output_channel = 'data_watch' if document_form == 'dataset' else 'primary_document_watch'
    elif evidence_type == 'discovery_lead':
        output_channel = 'discovery_queue'
    else:
        output_channel = 'daily_deep_digest'

    item.update({
        'framework_version': RESEARCH_FRAMEWORK_VERSION,
        'evidence_type': evidence_type,
        'document_form': document_form,
        'research_function': research_function,
        'geography_scope': geography[:5],
        'output_channel': output_channel,
        'source_tier': item.get('source_tier') or (1 if evidence_type in {'academic_paper', 'institutional_analysis', 'primary_official'} else 2),
    })
    return item

def refresh_item_evidence(item):
    """Recompute evidence scores after original-page enrichment.

    Keeping this in one place prevents the pre-gate and final gate from using
    different score formulas after a Google News link has been resolved.
    """
    content = item_content_text(item)
    core_score = count_terms(content, CORE_CA_LOWER)
    research_score = count_terms(content, RESEARCH_LOWER)
    depth_term_score = count_terms(content, DEPTH_LOWER)
    keyword_score = count_terms(content, KW_LOWER)
    policy_data_score = count_terms(content, POLICY_DATA_LOWER)
    event_signal_score = count_terms(content, EVENT_SIGNAL_LOWER)
    item['core_score'] = core_score
    item['research_score'] = research_score
    item['depth_term_score'] = depth_term_score
    item['policy_data_score'] = policy_data_score
    item['event_signal_score'] = event_signal_score
    item['kw_score'] = (
        item['core_score'] * 6 + item['research_score'] * 2
        + keyword_score + item['policy_data_score']
    )
    topic_matches = research_topic_matches(item)
    if topic_matches:
        item['priority_topics'] = [match['label'] for match in topic_matches[:3]]
        item['priority_score'] = research_topic_score(item)
    else:
        item['priority_topics'] = []
        item['priority_score'] = 0
    item['depth_score'] = (
        item['depth_term_score'] * 3
        + (8 if is_deep_item(item) else 0)
        + (5 if is_policy_data_item(item) else 0)
        + item['research_score']
        + item['policy_data_score']
        + item.get('priority_score', 0) // 20
    )
    classify_item_framework(item)
    return item

def filter_items(items):
    neg_tagged = []
    filtered = []
    for item in items:
        # Durable-research eligibility depends on depth/research scores. Score
        # first; otherwise an older institution article is judged as "simple
        # news" before its original-page evidence has been interpreted.
        refresh_item_evidence(item)
        if not is_recent_item(item):
            continue
        if is_generic_item(item):
            continue
        source = item.get('source', '')
        content = item_content_text(item)
        core_score = count_terms(content, CORE_CA_LOWER)
        research_score = count_terms(content, RESEARCH_LOWER)
        depth_term_score = count_terms(content, DEPTH_LOWER)
        keyword_score = count_terms(content, KW_LOWER)
        policy_data_score = count_terms(content, POLICY_DATA_LOWER)
        event_signal_score = count_terms(content, EVENT_SIGNAL_LOWER)
        neg_count = count_terms(content, NEG_KW_LOWER)
        source_context_score = 1 if has_local_context(source) and core_score == 0 else 0
        passes_relevance = has_meaningful_signal(
            source, core_score, research_score, depth_term_score,
            keyword_score, policy_data_score, event_signal_score
        )
        if (
            not passes_relevance
            and item.get('source_type') == 'institution_publication'
            and (
                source in DURABLE_PRESTIGE_DISCOVERY_SOURCES
                or is_neighbor_institution_publisher_text(item.get('publisher', ''))
            )
            and int(item.get('source_tier', 3) or 3) <= 2
            and has_strong_central_asia_anchor(item)
            and (research_score + depth_term_score + policy_data_score) >= 1
        ):
            passes_relevance = True
        if passes_relevance and (core_score + research_score + depth_term_score + policy_data_score) > neg_count * 3:
            filtered.append(item)
        elif passes_relevance and neg_count > 0:
            neg_tagged.append(item)
    return filtered, neg_tagged

# ================================================================
#  分类逻辑
# ================================================================
def categorize_item(item):
    s = item['source']
    t = (item.get('title','') + ' ' + item.get('summary','')).lower()

    if s in THINK_TANK_SOURCES:
        return 'think_tank'
    if s in RU_SOURCES:
        return 'ru'
    if s in LOCAL_KZ or s in LOCAL_UZ or s in LOCAL_KG or s in LOCAL_TJ or s in LOCAL_TM or s in REGIONAL_LOCAL:
        return 'local'
    if s in CN_SOURCES:
        return 'cn'
    if any(kw in t for kw in ['election', 'constitution', 'security', 'terrorism', 'military', 'border', 'taliban', 'afghanistan', 'opposition', 'government', 'cabinet', 'minister', 'president', 'prime minister', 'judiciary', 'corruption', 'akim', 'reform']):
        return 'pol_sec'
    if any(kw in t for kw in ['gas', 'oil', 'pipeline', 'mining', 'investment', 'trade', 'water', 'economic', 'gdp', 'currency', 'som', 'tenge', 'somoni', 'sum', 'manat', 'banking', 'ebrd', 'world bank', 'aisdb', 'export', 'import', 'sanction', 'inflation', 'remittance', 'labor migration', 'uranium', 'gold', 'lithium', 'copper', 'irrigation', 'dam project']):
        return 'econ'
    if any(kw in t for kw in ['eu ', 'diplomacy', 'summit', 'visit', 'partnership', 'strategy', 'us aid', 'russia central asia', 'moscow', 'putin', 'post-soviet', 'eurasian', 'sco', 'shanghai cooperation', 'turkic council', 'cica', 'eeu', 'eaeu', 'csto', 'collective security', 'beijing', 'china central asia']):
        return 'diplomacy'
    return 'news'

def clean_title(title):
    return clean_text((title or '').replace('#', '')).strip()

def reading_note(item):
    tags = tag_item(item)
    source = item.get('source', '')
    if item.get('source_type') == 'institution_publication' and is_report_grade_item(item):
        return '权威机构长篇研究报告，适合用于专题研判、制度比较与背景核验。'
    if source in ACADEMIC_SOURCE_NAMES or item.get('academic_quality'):
        venue = clean_text(item.get('academic_venue', ''))
        if venue:
            return '白名单期刊论文（' + venue + '），适合纳入文献综述、理论对话与专题书目。'
        return '白名单期刊论文，适合纳入文献综述、理论对话与专题书目。'
    if is_policy_data_item(item):
        return '官方或机构来源，适合核对政策口径、数据和表述。'
    if is_deep_item(item):
        return '深度来源，适合作为专题研判、文献综述和背景阅读。'
    if source in LOCAL_AND_OFFICIAL_SOURCES:
        return '本地来源，可用于观察国内议程设置和政策执行细节。'
    if any(tag in tags for tag in ['政治治理', '安全防务', '经济能源', '水资源气候', '外交关系']):
        return '议题相关度较高，可作为后续跟踪线索。'
    return ''

def append_wechat_item(lines, item, item_no, translate_summary=True, translate_title=True):
    title = clean_title(item.get('title', ''))
    link = item.get('link', '').strip()
    summary = clean_text(item.get('summary', '')).strip()
    published = clean_text(item.get('published', '')).strip()
    source = item.get('source', '').strip()
    title_cn = translate_text(title) if translate_title else title
    summary_cn = translate_text(summary) if summary and translate_summary else summary
    tags = tag_item(item)

    lines.append(f'**{item_no}. {title_cn}**')
    lines.append(f'来源｜{source}')
    if tags:
        lines.append('标签｜' + ' / '.join(format_public_tags(tags)))
    note = reading_note(item)
    if note:
        lines.append('研究价值｜' + note)
    if summary_cn:
        lines.append('摘要｜' + summary_cn)
    if title_cn != title:
        lines.append('原题｜' + title)
    if published:
        lines.append('时间｜' + published)
    lines.extend(item_link_lines(item, bullet=False))
    lines.append('—')
    lines.append('')

def topic_overview(items):
    counts = {}
    topic_labels = set(TOPIC_TAGS.keys())
    for item in items[:60]:
        for tag in tag_item(item):
            if tag in topic_labels:
                counts[tag] = counts.get(tag, 0) + 1
    ranked = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
    return [label for label, _ in ranked[:5]]

def write_internal_review_file(items):
    lines = []
    lines.append('# 中亚研究内部备查线索')
    lines.append('')
    lines.append('**' + TODAY.strftime('%Y年%m月%d日') + '**')
    lines.append('')
    lines.append('说明：本文件只存放“有研究价值、但因表述敏感/平台风险不宜进入公众号正文”的深度线索；普通新闻与低价值动态会被丢弃，不会进入本文件。仅供内部阅读、原文核验和选题跟踪。')
    lines.append('')
    if not items:
        lines.append('本期没有被公众号稳妥版移出的线索。')
    else:
        for index, item in enumerate(items[:80], start=1):
            append_wechat_item(lines, item, index, False, True)
    atomic_write_text(INTERNAL_REVIEW_FILE, '\n'.join(lines))

def trim_text(text, max_len=180):
    text = clean_text(text)
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip(' ，,。.;；') + '……'

def item_title_cn(item):
    title = clean_title(item.get('title', ''))
    title = re.sub(
        r'\s*[-–—|]\s*(?:The Foreign Policy Centre|Foreign Policy Centre|'
        r'KAZAKHSTAN INSTITUTE FOR STRATEGIC STUDIES UNDER THE PRESIDENT OF THE REPUBLIC OF KAZAKHSTAN|'
        r'Stimson Center|CSIS)\s*$',
        '',
        title,
        flags=re.I,
    ).strip()
    title_key = title.lower().replace('–', '-').replace('—', '-')
    link_path = urllib.parse.urlparse(item.get('link', '') or '').path.lower()
    if 'joint-working-group-on-international-and-eu-water-diplomacy-in-focus-central-asia' in link_path:
        title = 'Joint Working Group on International and EU Water Diplomacy - In Focus: Central Asia'
        title_key = title.lower()
    elif 'charting-central-asias-technological-renaissance-and-future-potential' in link_path:
        title = "Charting Central Asia's Technological Renaissance and Future Potential"
        title_key = title.lower()
    overrides = {
        'retreating rights - kyrgyzstan: introduction': '权利倒退：吉尔吉斯斯坦导论',
        'numbers and destinies: lessons from the 2025 annual address': '数字与命运：2025年国情咨文的启示',
    }
    return overrides.get(title_key, translate_text(title))

def item_summary_cn(item, max_len=220):
    title_lowered = clean_title(item.get('title', '')).lower()
    if (
        item.get('source') == 'World Bank Documents & Reports'
        and 'recommendations for scaling-up nature-based solutions' in title_lowered
    ):
        return '报告聚焦中亚土地退化与气候压力，分析如何通过区域合作扩大基于自然的景观修复方案，特别是在共享流域和跨境生态廊道中提升社区、生态系统与基础设施韧性。'
    summary = clean_rss_summary_html(item.get('summary', ''))
    title = clean_title(item.get('title', ''))
    if not summary:
        # Last-resort clue from title framing, still better than empty.
        if any(term in title.lower() for term in ['analysis', 'expert views', 'report', 'study', 'brief']):
            return trim_text('原文从分析/报告视角讨论：' + title, max_len)
        return ''
    # Drop summary that is basically the title plus publisher junk.
    summary_cmp = re.sub(r'\W+', '', summary.lower())
    title_cmp = re.sub(r'\W+', '', title.lower())
    if title_cmp and summary_cmp in {title_cmp, title_cmp + 'eurasianet', title_cmp + 'thediplomat'}:
        return ''
    if len(summary) < 40 and title_cmp and title_cmp in summary_cmp:
        return ''
    translated = translate_text(summary)
    # If translation collapses into title-like noise, keep a trimmed original excerpt.
    if translated and title and normalize_title_key(translated) == normalize_title_key(title):
        return trim_text(summary, max_len)
    return trim_text(translated or summary, max_len)

def item_public_tags(item):
    return format_public_tags(tag_item(item))

def item_topic_text(item):
    tags = item_public_tags(item)
    return ' / '.join(tags[:3]) if tags else '综合观察'

def core_question_for_item(item):
    tags = item_public_tags(item)
    if '经济能源' in tags:
        return '该动态将如何影响中亚经济转型、能源配置与产业链布局？'
    if '水资源气候' in tags:
        return '该议题对中亚水资源治理、能源供给和农业韧性有何影响？'
    if '对外关系' in tags:
        return '该动态如何改变中亚国家的多向合作空间与区域互联互通？'
    if '治理动态' in tags:
        return '相关制度或政策变化将如何影响国家治理能力与发展预期？'
    if '社会文化' in tags:
        return '该现象如何反映中亚社会结构、身份认同或公共服务变化？'
    if '区域稳定' in tags:
        return '该线索对区域稳定、跨境协作和风险治理有何启示？'
    return '该线索对理解中亚区域变化有什么研究价值？'

def research_value_for_item(item):
    source = item.get('source', '')
    tags = item_public_tags(item)
    priority_topics = item.get('priority_topics', [])
    title_surface = item_title_text(item).lower()
    if item.get('source_type') == 'institution_publication' and is_report_grade_item(item):
        return '提供权威机构的长篇、结构性证据，可用于专题研判、制度比较并核验跨领域判断。'
    if any(term in title_surface for term in ['armed forces', 'military', 'defence', 'defense', 'security sector']):
        return '可用于比较中亚国家安全部门建设、军事能力调整与区域安全结构变化。'
    if '安全、防务与边境秩序' in priority_topics:
        return '聚焦军事能力、安全部门、边境秩序或阿富汗外溢风险，适合研判区域安全结构与政策选择。'
    if '政治经济与国家能力' in priority_topics:
        return '可用于分析财政金融、国家能力、产业政策与制度安排如何共同塑造中亚的发展路径。'
    if '水资源与气候约束' in priority_topics:
        return '聚焦中亚长期约束性议题，可用于跟踪水资源、气候压力与区域合作机制的互动。'
    if '中间走廊与互联互通' in priority_topics:
        return '关系到中亚作为跨里海和欧亚物流节点的战略位置，适合跟踪通道、投资与制度瓶颈。'
    if '关键矿产与能源转型' in priority_topics:
        return '有助于判断能源转型、矿产开发和外部资本进入对中亚发展路径的影响。'
    if '大国关系与多向量外交' in priority_topics:
        return '可观察中亚国家在大国之间的政策平衡、议程设置和合作空间。'
    if '治理改革与制度演进' in priority_topics:
        return '有助于理解中亚国家制度改革话语、治理工具和政治传统再解释。'
    if '劳务移民与社会结构' in priority_topics:
        return '适合跟踪劳务流动、人口结构与社会政策变化对国家治理的影响。'
    if source in TELEGRAM_SOURCES:
        return '来自本地语种或区域 Telegram 公开频道，可补足官网与英文媒体之外的议程设置和现场观察。'
    if source in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES):
        return '机构报告或 PDF 入口，适合提取数据、图表、政策判断和可引用的背景材料。'
    if source in MEETING_MINUTES_SOURCES:
        return '会议纪要、新闻稿或多边机制材料，适合核对官方议程、合作措辞和机构间互动。'
    if source in ACADEMIC_SOURCE_NAMES:
        return '近期学术论文线索，适合进入文献综述、理论框架更新和专题书目积累。'
    if is_policy_data_item(item):
        return '适合用于核对官方政策口径、原始数据和机构表述，后续可作为报告写作的事实依据。'
    if is_deep_item(item) or is_strict_deep_public_item(item):
        title = item_title_text(item)
        if 'news analysis' in title or 'analysis' in title:
            return '带有明确分析框架的深度报道，适合提取机制解释、行为体动机与政策含义。'
        if 'expert views' in title or 'commentary' in title:
            return '专家评论/观点类材料，适合对照不同机构判断并补充研究假设。'
        if item.get('word_count', 0) >= 1000 or item.get('summary_enriched'):
            return '正文信息较充分，适合作为专题研究的背景材料并与官方数据交叉验证。'
        return '适合作为专题研究的背景材料，可与智库报告、官方数据和本地媒体信息交叉验证。'
    if source in LOCAL_AND_OFFICIAL_SOURCES:
        return '有助于观察本地政策落地、国内议程设置和社会经济运行细节。'
    if '经济能源' in tags or '对外关系' in tags:
        return '可作为跟踪区域互联互通、投资合作和外部伙伴关系变化的线索。'
    return '可作为后续选题跟踪线索，建议结合原文与其他来源进行核验。'

def source_note_for_item(item):
    source = item.get('source', '').strip()
    link = item.get('link', '').strip()
    if link:
        return '> 来源：' + source + '｜原文链接：' + link
    return '> 来源：' + source

def append_doubao_headline(lines, item, item_no):
    title = item_title_cn(item)
    summary = item_summary_cn(item, 230)
    tags = item_public_tags(item)
    published = clean_text(item.get('published', ''))
    lines.append('### ' + str(item_no) + '. ' + title)
    lines.append('')
    lines.append('**核心问题**：' + core_question_for_item(item))
    lines.append('')
    lines.append('**关键发现**：')
    if summary:
        lines.append('- 【' + item.get('source', '') + '】' + summary)
    else:
        lines.append('- 【' + item.get('source', '') + '】该条目聚焦“' + item_topic_text(item) + '”，可作为观察中亚相关议题的新线索。')
    if tags:
        lines.append('- 议题标签：' + '、'.join(tags[:4]))
    if published:
        lines.append('- 时间线索：' + published)
    lines.append('')
    lines.append('**研究价值**：' + research_value_for_item(item))
    lines.append('')
    lines.append(source_note_for_item(item))
    lines.append('')
    lines.append('---')
    lines.append('')

def select_public_items(pool, limit, used_keys, published_items, predicate=None, source_counts=None, max_per_source=MAX_PUBLIC_ITEMS_PER_SOURCE):
    selected = []
    source_counts = source_counts if source_counts is not None else {}
    academic_count = sum(1 for item in published_items if is_academic_item(item))
    for item in pool:
        key = normalize_key(item)
        if key in used_keys:
            continue
        if predicate and not predicate(item):
            continue
        source_key = public_source_key(item)
        source_limit = max_per_source
        if is_academic_item(item):
            source_limit = min(source_limit, MAX_PUBLIC_ITEMS_PER_ACADEMIC_VENUE)
            if academic_count >= MAX_PUBLIC_ACADEMIC_ITEMS:
                continue
        if source_counts.get(source_key, 0) >= source_limit:
            continue
        used_keys.add(key)
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if is_academic_item(item):
            academic_count += 1
        selected.append(item)
        published_items.append(item)
        if len(selected) >= limit:
            break
    return selected

def is_headline_candidate(item):
    title = clean_title(item.get('title', '')).lower()
    summary = clean_text(item.get('summary', ''))
    if is_academic_item(item):
        return False
    if not is_strict_deep_public_item(item):
        return False
    if is_report_grade_item(item):
        return False
    if not summary and ('roundup' in title or '综述' in title):
        return False
    return bool(summary or item_public_tags(item))

def is_substantive_deep_item(item):
    title = clean_title(item.get('title', '')).lower()
    summary = clean_text(item.get('summary', ''))
    if is_academic_item(item):
        return False
    if not is_strict_deep_public_item(item):
        return False
    if is_report_grade_item(item):
        return False
    if not summary and ('roundup' in title or '综述' in title):
        return False
    return bool(summary or item_public_tags(item))

def is_policy_table_candidate(item):
    return is_substantive_policy_document(item)
def markdown_escape_cell(text):
    return clean_text(text).replace('|', '｜').replace('\n', ' ')

def append_item_table(lines, items):
    lines.append('| 线索 | 来源 | 议题 | 研究意义 |')
    lines.append('|---|---|---|---|')
    for item in items:
        title = markdown_escape_cell(trim_text(item_title_cn(item), 42))
        source = markdown_escape_cell(item.get('source', ''))
        topic = markdown_escape_cell(item_topic_text(item))
        value = markdown_escape_cell(trim_text(research_value_for_item(item), 48))
        lines.append('| ' + title + ' | ' + source + ' | ' + topic + ' | ' + value + ' |')
    lines.append('')

def research_agenda_for_topic(topic):
    mapping = {
        '经济能源': '跟踪能源、矿产、交通走廊与投资项目的联动，重点观察项目融资、产能约束和产业链本地化。',
        '水资源气候': '关注枯水期、水电调度、农业用水和跨境水资源协调，识别其对区域合作的长期影响。',
        '对外关系': '观察中亚国家在多边机制、双边访问和经贸合作中的多向平衡策略。',
        '治理动态': '跟踪制度改革、公共治理和政策执行细节，关注官方表述与实际落地之间的差异。',
        '社会文化': '关注教育、人口、劳务流动、身份认同和文化政策对国家建设的影响。',
        '区域稳定': '以风险治理和跨境协作为框架，观察区域机制如何回应非传统挑战。',
    }
    return mapping.get(topic, '继续跟踪该议题的政策变化、数据更新和本地社会反应。')

def append_research_agenda(lines, topics):
    if not topics:
        topics = ['经济能源', '对外关系', '治理动态']
    for topic in format_public_tags(topics)[:5]:
        lines.append('- **' + topic + '**：' + research_agenda_for_topic(topic))
    lines.append('')

def source_summary(items):
    buckets = {
        '本地语种/Telegram': [],
        '报告/会议/论文': [],
        '智库/深度媒体': [],
        '官方与机构': [],
        '本地媒体': [],
        '国际媒体': [],
    }
    for item in items:
        source = item.get('source', '')
        if source in TELEGRAM_SOURCES:
            buckets['本地语种/Telegram'].append(source)
        elif source in PDF_REPORT_SOURCES or source in REPORT_API_SOURCE_NAMES or source in MEETING_MINUTES_SOURCES or source in ACADEMIC_SOURCE_NAMES:
            buckets['报告/会议/论文'].append(source)
        elif source in THINK_TANK_SOURCES or source in DEEP_ANALYSIS_SOURCES:
            buckets['智库/深度媒体'].append(source)
        elif source in OFFICIAL_POLICY_SOURCES:
            buckets['官方与机构'].append(source)
        elif source in LOCAL_AND_OFFICIAL_SOURCES:
            buckets['本地媒体'].append(source)
        else:
            buckets['国际媒体'].append(source)
    lines = []
    for label, sources in buckets.items():
        unique_sources = sorted(set(sources))[:10]
        if unique_sources:
            lines.append('- **' + label + '**：' + '、'.join(unique_sources))
    return lines

def render_doubao_public_digest(
    deduped, deep_focus, policy_focus, cats,
    active_feeds, active_web, feed_jobs, web_jobs, candidate_web_jobs,
    cross_day_skipped, internal_review_items
):
    used_keys = set()
    published_items = []
    lines = []
    active_feed_sources = sources_with_items(feed_jobs)
    active_web_sources = sources_with_items(web_jobs)
    candidate_active_sources = sources_with_items(candidate_web_jobs)
    candidate_text = '；候选网页源 ' + str(candidate_active_sources) + '/' + str(len(CANDIDATE_WEB_SOURCES)) + ' 返回非空（尚未过滤）' if TEST_CANDIDATE_SOURCES else ''
    topic_focus = topic_overview(deduped)
    public_topics = format_public_tags(topic_focus)

    lines.append('# 中亚研究每日简报（深度分析版）')
    lines.append('')
    lines.append('**日期：' + TODAY.strftime('%Y年%m月%d日') + '**')
    lines.append('')
    lines.append('---')
    lines.append('')
    lines.append('本简报面向中亚区域国别研究，参考“专题综述+深度点评+来源汇总”的写法，优先呈现研究价值较高的公开信息。')
    if public_topics:
        lines.append('')
        lines.append('**本期关注方向**：' + '、'.join(public_topics) + '。')
    lines.append('')
    lines.append('---')
    lines.append('')

    headline_pool = deep_focus[:12] + policy_focus[:8] + cats.get('econ', [])[:8] + cats.get('diplomacy', [])[:8] + deduped
    headlines = select_public_items(headline_pool, 6, used_keys, published_items, is_headline_candidate)
    lines.append('## 一、头条深度（Top Deep Dives）')
    lines.append('')
    if headlines:
        for index, item in enumerate(headlines, start=1):
            append_doubao_headline(lines, item, index)
    else:
        lines.append('本期未抓取到足够的高价值头条线索。')
        lines.append('')

    lines.append('## 二、智库报告与深度媒体（Think Tanks & Deep Reads）')
    lines.append('')
    think_items = select_public_items(deep_focus + cats.get('think_tank', []), 8, used_keys, published_items, is_substantive_deep_item)
    if think_items:
        append_item_table(lines, think_items)
    else:
        lines.append('本期暂无新增可发布的智库深读条目。')
        lines.append('')

    lines.append('## 三、政策追踪与数据线索（Policy & Data）')
    lines.append('')
    policy_items = select_public_items(policy_focus + cats.get('local', []) + deduped, 8, used_keys, published_items, is_policy_table_candidate)
    if policy_items:
        append_item_table(lines, policy_items)
    else:
        lines.append('本期暂无新增可发布的政策与数据线索。')
        lines.append('')

    lines.append('## 四、经济、能源与互联互通（Economy & Connectivity）')
    lines.append('')
    econ_items = select_public_items(cats.get('econ', []) + deduped, 8, used_keys, published_items, lambda item: '经济能源' in item_public_tags(item))
    if econ_items:
        append_item_table(lines, econ_items)
    else:
        lines.append('本期暂无新增可发布的经济能源条目。')
        lines.append('')

    lines.append('## 五、对外关系与区域合作（Diplomacy & Cooperation）')
    lines.append('')
    diplomacy_items = select_public_items(cats.get('diplomacy', []) + deduped, 8, used_keys, published_items, lambda item: '对外关系' in item_public_tags(item))
    if diplomacy_items:
        append_item_table(lines, diplomacy_items)
    else:
        lines.append('本期暂无新增可发布的对外关系条目。')
        lines.append('')

    lines.append('## 六、社会与人文纵深（Society & Humanities）')
    lines.append('')
    society_items = select_public_items(deduped, 6, used_keys, published_items, lambda item: '社会文化' in item_public_tags(item))
    if society_items:
        append_item_table(lines, society_items)
    else:
        lines.append('本期暂无新增可发布的社会人文条目。')
        lines.append('')

    lines.append('## 七、研究前瞻（Research Agenda）')
    lines.append('')
    append_research_agenda(lines, topic_focus)

    lines.append('## 八、今日信息来源汇总')
    lines.append('')
    source_lines = source_summary(published_items)
    if source_lines:
        lines.extend(source_lines)
    else:
        lines.append('- 本期公开正文未形成稳定来源汇总。')
    lines.append('')

    lines.append('## 九、资料来源与使用说明')
    lines.append('')
    lines.append('本期抓取范围：启用 ' + str(len(active_feeds)) + '/' + str(len(FEEDS)) + ' 个 RSS 源、' + str(len(active_web)) + '/' + str(len(WEB_SOURCES)) + ' 个网页源；有效返回：RSS ' + str(active_feed_sources) + '/' + str(len(active_feeds)) + '，网页 ' + str(active_web_sources) + '/' + str(len(active_web)) + candidate_text + '；跨日去重排除 ' + str(cross_day_skipped) + ' 条；公众号正文候选 ' + str(len(deduped)) + ' 条。')
    lines.append('')
    if WECHAT_SAFE_MODE:
        lines.append('公众号稳妥版：已将 ' + str(len(internal_review_items)) + ' 条不适合直接公开展开的研究线索移入内部备查文件 ' + INTERNAL_REVIEW_FILE.name + '。')
        lines.append('')
    lines.append('说明：本简报为公开信息自动聚合与中文整理结果，机器翻译和自动分类可能存在偏差；涉及事实、数字、机构表述与敏感判断时，请以原文链接为准并进行人工核验。')
    lines.append('')
    lines.append('---')
    lines.append('')
    lines.append('*本简报为深度分析版，聚焦研究价值，过滤日常新闻噪音。*')
    append_project_collaboration_note(lines)
    return lines, published_items


def item_link_lines(item, bullet=True):
    """Format original and fallback links for public/internal digests."""
    lines = []
    link = clean_text(item.get('link', ''))
    prefix = '- ' if bullet else ''
    sep = '：' if bullet else '｜'
    if link:
        lines.append(prefix + '原文链接' + sep + link)
    # If still a Google News redirect, provide publisher fallbacks for deep reading.
    if is_google_news_url(link):
        search = clean_text(item.get('publisher_search_link', ''))
        home = clean_text(item.get('publisher_home', ''))
        if search:
            lines.append(prefix + '出版方检索' + sep + search)
        elif home:
            lines.append(prefix + '出版方主页' + sep + home)
    return lines

def item_time_status(item):
    if clean_text(item.get('date_precision', '')).lower() == 'year':
        year = parse_item_publication_year(item)
        if year == TODAY.year:
            return '本年度研究报告（首次收录）'
        return '年度研究报告（首次收录）'
    age_days = item_age_days(item)
    if age_days is None:
        return '日期待核验'
    if age_days <= 1:
        return '当日/近日报告'
    if age_days <= NEW_DISCOVERY_LOOKBACK_DAYS:
        return '近期首次发现'
    if age_days <= MAX_SLOW_PUBLICATION_AGE_DAYS:
        return '近期研究'
    return '长期有效研究（首次收录）'

def append_research_link_item(lines, item, item_no):
    title = item_title_cn(item)
    source = item.get('source', '').strip()
    link = item.get('link', '').strip()
    tags = item_public_tags(item)
    priority_topics = item.get('priority_topics', [])
    summary = item_summary_cn(item, 180)
    published = clean_text(item.get('published', ''))
    publication_year = parse_item_publication_year(item)
    doi = clean_text(item.get('doi', ''))

    lines.append('### ' + str(item_no) + '. ' + title)
    source_line = source
    if item.get('academic_venue'):
        source_line = source + '；刊物：' + clean_text(item.get('academic_venue', ''))
        lines.append('- 文献属性：' + source_line)
    elif item.get('source_type') == 'institution_publication':
        institution = clean_text(item.get('institution', source))
        publication_kind = clean_text(item.get('institution_publication_kind', 'report'))
        kind_labels = {
            'research_analysis': '研究分析',
            'research_publication': '研究出版物',
            'policy_brief': '政策简报',
            'policy_memo': '政策备忘录',
            'working_paper': '工作论文',
            'report': '研究报告',
            'research_report': '研究报告',
        }
        lines.append('- 来源机构：' + institution)
        lines.append('- 文献类型：' + kind_labels.get(publication_kind.lower(), publication_kind or '研究出版物'))
    else:
        lines.append('- 文献属性：' + source_line)
    authors = clean_text(item.get('academic_authors', ''))
    if authors:
        lines.append('- 书目信息：' + authors + (('；DOI：' + doi) if doi else ''))
    if published:
        lines.append('- 发布日期：' + published + '；' + item_time_status(item))
    elif publication_year:
        lines.append('- 发布年份：' + str(publication_year) + '；' + item_time_status(item))
    if not doi and is_academic_item(item):
        link = clean_text(item.get('link', ''))
        if 'doi.org/' in link:
            doi = link.split('doi.org/', 1)[-1].strip()
    if doi and not authors:
        lines.append('- DOI：' + doi)
    if tags:
        lines.append('- 研究焦点：' + ' / '.join(tags[:3]))
    if summary:
        lines.append('- 内容提要：' + summary)
    lines.append('- 阅读价值：' + research_value_for_item(item))
    lines.extend(item_link_lines(item, bullet=True))
    lines.append('')

CORE_RESEARCH_PILLARS = (
    '安全、防务与边境秩序',
    '政治经济与国家能力',
    '治理改革与制度演进',
    '大国关系与多向量外交',
)

def core_research_pillar(item):
    """Assign an item to a core research pillar for soft daily balancing."""
    raw_tags = set(tag_item(item))
    topics = set(item.get('priority_topics', []))
    if '安全防务' in raw_tags or topics & {'安全、防务与边境秩序', '阿富汗关联与边境风险'}:
        return '安全、防务与边境秩序'
    if '经济能源' in raw_tags or '政治经济与国家能力' in topics:
        return '政治经济与国家能力'
    if '政治治理' in raw_tags or '治理改革与制度演进' in topics:
        return '治理改革与制度演进'
    if '外交关系' in raw_tags or '大国关系与多向量外交' in topics:
        return '大国关系与多向量外交'
    return ''

def softly_balance_core_pillars(items):
    """Reserve ordering space for distinct core pillars without adding fillers."""
    ranked = list(items or [])
    selected = []
    used_keys = set()
    for pillar in CORE_RESEARCH_PILLARS:
        for item in ranked:
            key = normalize_key(item)
            if key in used_keys or core_research_pillar(item) != pillar:
                continue
            selected.append(item)
            used_keys.add(key)
            break
    for item in ranked:
        key = normalize_key(item)
        if key and key not in used_keys:
            selected.append(item)
            used_keys.add(key)
    return selected

def report_meeting_academic_intro_lines():
    return [
        '本栏仅列正式报告、政策文件、会议纪要和经书目要素核验的学术论文；摘要与书目信息均须以原文或 DOI 页面复核。',
    ]

def append_research_link_section(lines, title, items, used_keys, published_items, limit=8, source_counts=None, max_per_source=MAX_PUBLIC_ITEMS_PER_SOURCE, intro_lines=None, balance_core_pillars=False):
    if balance_core_pillars:
        items = softly_balance_core_pillars(items)
    selected = select_public_items(items, limit, used_keys, published_items, source_counts=source_counts, max_per_source=max_per_source)
    if not selected:
        return False
    lines.append('## ' + title)
    lines.append('')
    if intro_lines:
        for intro in intro_lines:
            lines.append(intro)
        lines.append('')
    for index, item in enumerate(selected, start=1):
        append_research_link_item(lines, item, index)
    return True

def published_item_count(published_items):
    return len({normalize_key(item) for item in published_items})

def recent_review_rank(item):
    """Rank non-counting review items by research value and source authority."""
    if is_public_simple_news(item) or is_institute_soft_content(item):
        return -1000
    if item.get('source_type') == 'institution_publication' or is_report_grade_item(item):
        return 400
    if is_top_tier_media_item(item):
        return 300
    if is_academic_item(item):
        return 200
    if is_substantive_policy_document(item):
        return 150
    return 50

def append_research_shortfall_section(lines, deduped, used_keys, published_items, source_counts):
    shortfall = MIN_PUBLIC_RECOMMENDATIONS - published_item_count(published_items)
    if shortfall <= 0:
        return False
    fallback_pool = [
        item for item in deduped
        if is_research_grade_public_item(item)
        and (is_strict_deep_public_item(item) or is_substantive_policy_document(item))
    ]
    selected = select_public_items(
        fallback_pool, shortfall, used_keys, published_items,
        source_counts=source_counts,
        max_per_source=SHORTFALL_MAX_PUBLIC_ITEMS_PER_SOURCE
    )
    if not selected:
        return False
    lines.append('## 补充阅读材料')
    lines.append('')
    lines.append(
        '说明：本栏只在公开版少于 ' + str(MIN_PUBLIC_RECOMMENDATIONS) +
        ' 条时启用，不放宽时效、研究价值和公众号稳妥表述门槛；同一权威来源最多只额外补充 1 条。'
    )
    lines.append('')
    for index, item in enumerate(selected, start=1):
        append_research_link_item(lines, item, index)
    return True

def public_conversion_title(item):
    text = item_content_text(item)
    if 'kazakhstan' in text and ('constitution' in text or 'constitutional' in text):
        return '哈萨克斯坦制度观察：宪法法院解释与任期安排讨论'
    return item_title_cn(item)

def append_public_conversion_item(lines, item, item_no):
    title = public_conversion_title(item)
    source = item.get('source', '').strip()
    link = item.get('link', '').strip()
    tags = item_public_tags(item)
    priority_topics = item.get('priority_topics', [])
    summary = item_summary_cn(item, 150)
    published = clean_text(item.get('published', ''))
    lines.append('**' + str(item_no) + '. ' + title + '**')
    lines.append('- 来源：' + source)
    if tags:
        lines.append('- 议题：' + ' / '.join(tags[:4]))
    if priority_topics:
        lines.append('- 研究议程：' + ' / '.join(public_research_topic_labels(priority_topics[:3])))
    lines.append('- 为什么值得读：可补足公开版的经济、能源或制度观察维度，适合以事实和研究问题为中心阅读。')
    if summary:
        lines.append('- 内容线索：' + summary)
    if published:
        lines.append('- 时间：' + published)
    lines.extend(item_link_lines(item, bullet=True))
    lines.append('')

def append_public_conversion_section(lines, internal_review_items, used_keys, published_items, source_counts):
    selected = []
    display_titles = set()
    for item in internal_review_items:
        key = normalize_key(item)
        if key in used_keys:
            continue
        if not is_public_convertible_internal_item(item):
            continue
        display_key = normalize_title_key(public_conversion_title(item))
        if display_key in display_titles:
            continue
        source_key = public_source_key(item)
        if source_counts.get(source_key, 0) >= 2:
            continue
        used_keys.add(key)
        display_titles.add(display_key)
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        selected.append(item)
        published_items.append(item)
        if len(selected) >= PUBLIC_CONVERSION_SECTION_LIMIT:
            break
    if not selected:
        return False
    lines.append('## 审慎公开转化阅读')
    lines.append('')
    lines.append('说明：本栏选取事实性、低争议、适合公开讨论的经济、能源和制度观察材料；其余线索仍保留在内部备查。')
    lines.append('')
    for index, item in enumerate(selected, start=1):
        append_public_conversion_item(lines, item, index)
    return True

def blindspot_radar(deduped, internal_review_items, cross_day_skipped=0):
    all_tags = []
    all_priority_topics = []
    for item in deduped:
        all_tags.extend(item_public_tags(item))
        all_priority_topics.extend(item.get('priority_topics', []))
    tag_counts = {tag: all_tags.count(tag) for tag in sorted(set(all_tags))}
    priority_counts = {
        topic['label']: all_priority_topics.count(topic['label'])
        for topic in RESEARCH_TOPIC_PRIORITIES
    }
    radar = []
    if not deduped and cross_day_skipped:
        radar.append(
            '本期没有新增公开条目；部分合格候选已因跨日去重排除（' +
            str(cross_day_skipped) + ' 条），并非抓取通道关闭。'
        )
    elif len(deduped) < MIN_PUBLIC_RECOMMENDATIONS:
        radar.append(
            '本期经时效、中亚强相关、来源信誉和深度体裁四重筛选后，仅有 ' +
            str(len(deduped)) + ' 条达到公开版门槛；其余候选多为普通分析性新闻、官方活动稿或未达深度标准的材料，未用于凑数。'
        )
    expected = ['经济能源', '水资源气候', '对外关系', '治理动态', '社会文化', '区域稳定']
    missing = [tag for tag in expected if tag_counts.get(tag, 0) == 0]
    thin = [tag for tag in expected if 0 < tag_counts.get(tag, 0) <= 1]
    priority_missing = [
        label for label, count in priority_counts.items()
        if count == 0
    ]
    priority_thin = [
        label for label, count in priority_counts.items()
        if 0 < count <= 1
    ]
    if priority_missing:
        radar.append('按长期研究议程看，本期公开版未覆盖：' + '、'.join(public_research_topic_labels(priority_missing[:5])) + '。')
    if priority_thin:
        radar.append('长期研究议程中覆盖较薄：' + '、'.join(public_research_topic_labels(priority_thin[:5])) + '。')
    if missing:
        radar.append('本期公开正文缺少或几乎没有覆盖：' + '、'.join(missing) + '。建议在内部备查或后续专题中补读。')
    if thin:
        radar.append('本期覆盖较薄的议题：' + '、'.join(thin) + '。不宜据此形成趋势判断。')
    if internal_review_items:
        radar.append('有 ' + str(len(internal_review_items)) + ' 条需要谨慎表述但可能有研究价值的线索已移入内部备查，涉及治理、区域安全或社会运行相关议题。')
    if not radar:
        radar.append('本期公开正文议题分布相对均衡，但仍建议与内部备查和原文链接交叉核验。')
    radar.append('机制提示：公开版坚持深度体裁证据；分析性新闻、会议预告与无日期材料不会为凑数进入正文；Google News 链接会尽量解析为原文。')
    radar.append('学术机制：白名单期刊定向拉取 + 近 ' + str(ACADEMIC_LOOKBACK_DAYS) + ' 天窗口；无 DOI/作者/摘要或中亚锚点不足的论文不进公开版，学术栏允许为 0。')
    return radar

def render_researcher_link_digest(
    deduped, deep_focus, policy_focus, cats,
    active_feeds, active_web, feed_jobs, web_jobs, candidate_web_jobs,
    cross_day_skipped, internal_review_items, extra_source_jobs=None, recent_review_items=None
):
    extra_source_jobs = extra_source_jobs or []
    recent_review_items = recent_review_items or []
    used_keys = set()
    source_counts = {}
    published_items = []
    lines = []
    active_feed_sources = sources_with_items(feed_jobs)
    active_web_sources = sources_with_items(web_jobs)
    candidate_active_sources = sources_with_items(candidate_web_jobs)
    # On low-yield days the evidence should occupy the page, not boilerplate.
    # This estimate is deliberately conservative: it only counts current,
    # research-grade pools that can actually feed the public edition.
    estimated_public_count = len({
        normalize_key(item)
        for item in (deep_focus + policy_focus + cats.get('econ', []) + cats.get('diplomacy', []))
        if is_research_grade_public_item(item)
    })
    compact_template = estimated_public_count <= 4
    candidate_text = '；候选网页源 ' + str(candidate_active_sources) + '/' + str(len(CANDIDATE_WEB_SOURCES)) + ' 返回非空（尚未过滤）' if TEST_CANDIDATE_SOURCES else ''
    extra_text = extra_source_scope_text(extra_source_jobs)
    topic_focus = format_public_tags(topic_overview(deduped))

    lines.append('# 中亚研究每日简报')
    lines.append('')
    lines.append('**编制日期**：' + TODAY.strftime('%Y年%m月%d日'))
    lines.append('')
    lines.append('**研究范围**：中亚五国及其跨境政治、经济、安全、社会与外部关系。')
    lines.append('**选编原则**：仅收录原文可访问、发布日期或正式报告年份可核验、具有实质中亚关联的报告、政策分析、学术论文和深度调查；不收普通快讯、活动稿、书评或仅有付费摘要的线索。')
    lines.append('**时效口径**：普通深度分析原则上近 ' + str(MAX_ITEM_AGE_DAYS) + ' 日；顶级媒体、权威机构分析与正式报告可放宽至近 ' + str(MAX_DEEP_ANALYSIS_AGE_DAYS) + ' 日；长期研究按权威性、深度、可访问性和耐久性门禁择优追溯，最长近 ' + str(DURABLE_RESEARCH_MAX_AGE_DAYS // 365) + ' 年。所有材料均须此前未在简报出现。')
    if topic_focus:
        lines.append('**本期议题**：' + '、'.join(topic_focus[:5]) + '。')
    lines.append('**使用说明**：内容提要为自动整理，正式引用请回到原文或 DOI 页面核验。')
    lines.append('')

    normal_conversion_items = normal_public_conversion_pool(internal_review_items)
    cautious_conversion_items = cautious_public_conversion_pool(internal_review_items)

    recent_priority_pool = [
        item for item in (deep_focus + policy_focus + cats.get('econ', []) + cats.get('diplomacy', []) + deduped)
        if parse_item_published_date(item)
        and (TODAY - parse_item_published_date(item)).days <= MAX_SLOW_PUBLICATION_AGE_DAYS
    ]
    priority_candidates = [
        item for item in recent_priority_pool
        if is_headline_candidate(item)
        or is_research_grade_public_item(item)
        or is_strict_deep_public_item(item)
    ]
    append_research_link_section(
        lines, '近期优先阅读',
        priority_candidates,
        used_keys, published_items, 4, source_counts,
        balance_core_pillars=True,
    )
    append_research_link_section(
        lines, '长期有效研究（首次收录）',
        [
            item for item in deduped
            if item_age_days(item) is not None
            and item_age_days(item) > MAX_SLOW_PUBLICATION_AGE_DAYS
            and is_durable_research_grade(item)
        ],
        used_keys, published_items, 4, source_counts,
        balance_core_pillars=True,
    )
    top_media_pool = [
        item for item in (deep_focus + deduped)
        if is_top_tier_media_item(item) and is_strict_deep_public_item(item)
    ]
    append_research_link_section(
        lines, '国际媒体深度报道',
        top_media_pool,
        used_keys, published_items, 6, source_counts,
        intro_lines=[] if compact_template else [
            '本栏仅收录国际媒体的调查、解释性长文、专题分析、数据报道或专家访谈，不收普通快讯、会议消息和转载稿。',
            '媒体材料与智库报告分开呈现；其事实信息仍应结合机构原文、数据源或学术文献进行交叉核验。',
        ],
    )
    append_research_link_section(
        lines, '智库研究与深度分析',
        [
            item for item in deep_focus + cats.get('think_tank', [])
            if is_substantive_deep_item(item)
        ],
        used_keys, published_items, 10, source_counts
    )
    # Historical collisions are retained only for diagnostics. The public
    # daily edition never repeats an item that has appeared before.
    append_research_link_section(
        lines, '政策文件与官方数据',
        [item for item in policy_focus + deduped + normal_conversion_items if is_policy_table_candidate(item)],
        used_keys, published_items, 10, source_counts
    )
    append_research_link_section(
        lines, '经济、能源与互联互通',
        [
            item for item in cats.get('econ', []) + deduped + normal_conversion_items
            if is_strict_deep_public_item(item)
            and not is_report_grade_item(item)
            and ('经济能源' in item_public_tags(item) or '水资源气候' in item_public_tags(item))
        ],
        used_keys, published_items, 8, source_counts
    )
    append_research_link_section(
        lines, '对外关系与区域合作',
        [
            item for item in cats.get('diplomacy', []) + deduped + normal_conversion_items
            if is_strict_deep_public_item(item)
            and not is_report_grade_item(item)
            and '对外关系' in item_public_tags(item)
        ],
        used_keys, published_items, 8, source_counts
    )
    append_research_link_section(
        lines, '本地观察与社会文化',
        [
            item for item in cats.get('local', []) + deduped
            if is_strict_deep_public_item(item) and '社会文化' in item_public_tags(item)
        ],
        used_keys, published_items, 8, source_counts
    )

    local_language_pool = [
        item for item in deduped
        if item.get('source') in TELEGRAM_SOURCES or item.get('source') in LOCAL_AND_OFFICIAL_SOURCES
        if not is_report_grade_item(item)
        if is_strict_deep_public_item(item)
    ]
    if not append_research_link_section(
        lines, '本地语种深度材料',
        local_language_pool,
        used_keys, published_items, 8, source_counts
    ):
        pass

    report_meeting_academic_pool = [
        item for item in deduped
        if (item.get('source') in PDF_REPORT_SOURCES
        or item.get('source') in REPORT_API_SOURCE_NAMES
        or item.get('source') in MEETING_MINUTES_SOURCES
        or item.get('source') in ACADEMIC_SOURCE_NAMES
        or is_report_grade_item(item)
        or is_substantive_policy_document(item))
    ]
    academic_intro = report_meeting_academic_intro_lines()
    if not append_research_link_section(
        lines, '报告、会议纪要与学术论文',
        report_meeting_academic_pool,
        used_keys, published_items, 8, source_counts,
        intro_lines=academic_intro,
    ):
        pass

    append_public_conversion_section(lines, cautious_conversion_items, used_keys, published_items, source_counts)

    append_research_shortfall_section(lines, deduped, used_keys, published_items, source_counts)

    # A classified item can occasionally miss every themed section after page
    # enrichment changes its tags. Never let a non-empty, quality-gated pool
    # render as an empty daily brief: this is a presentation fallback only and
    # does not relax any source, depth, date, or history requirement.
    if not published_items and deduped:
        append_research_link_section(
            lines, '本期合格研究', deduped,
            used_keys, published_items, 6, source_counts,
        )

    retained_internal_review_items = [
        item for item in internal_review_items
        if normalize_key(item) not in {normalize_key(published_item) for published_item in published_items}
    ]

    if not compact_template:
        lines.append('## 研究盲点与证据缺口')
        lines.append('')
        for point in blindspot_radar(deduped, retained_internal_review_items, cross_day_skipped):
            lines.append('- ' + point)
        lines.append('')

        lines.append('## 资料来源与使用说明')
        lines.append('')
        lines.append('本期抓取范围：启用 ' + str(len(active_feeds)) + '/' + str(len(FEEDS)) + ' 个 RSS 源、' + str(len(active_web)) + '/' + str(len(WEB_SOURCES)) + ' 个网页源；有效返回：RSS ' + str(active_feed_sources) + '/' + str(len(active_feeds)) + '，网页 ' + str(active_web_sources) + '/' + str(len(active_web)) + candidate_text + extra_text + '；跨日去重排除 ' + str(cross_day_skipped) + ' 条；公众号正文候选 ' + str(len(deduped)) + ' 条；公开版推荐 ' + str(published_item_count(published_items)) + ' 条。')
        lines.append('')
    if WECHAT_SAFE_MODE:
        lines.append('公众号稳妥版：已将 ' + str(len(retained_internal_review_items)) + ' 条不适合直接公开展开的研究线索移入内部备查文件 ' + INTERNAL_REVIEW_FILE.name + '。')
        lines.append('')
    if compact_template:
        lines.append('**文献状态**：以上条目均为本轮首次公开推荐，不重复收录往期简报材料。')
        lines.append('检索诊断：RSS 有效源 ' + str(active_feed_sources) + '/' + str(len(active_feeds))
                     + '，网页有效源 ' + str(active_web_sources) + '/' + str(len(active_web))
                     + '，当前候选 ' + str(len(deduped)) + ' 条，历史重复排除 '
                     + str(cross_day_skipped) + ' 条；“0 条”仅表示本轮没有新的合格公开材料。')
    else:
        lines.append('建议用法：公开版只放深度分析与达标学术论文；内部备查只放不宜公开的深度线索；普通新闻与非白名单论文不进入任一版本；正式引用前请打开原文/DOI 核验。')
    append_project_collaboration_note(lines)
    return lines, published_items

# ================================================================
#  主函数
# ================================================================
def main():
    global TRANSLATE_CACHE
    if RUNTIME.replay:
        replay_saved_render()
        return
    TRANSLATE_CACHE = load_translation_cache()

    local_kz = LOCAL_KZ
    local_uz = LOCAL_UZ
    local_kg = LOCAL_KG
    local_tj = LOCAL_TJ
    local_tm = LOCAL_TM

    all_items = []
    recent_review_items = []
    skipped_feeds = skipped_feed_sources()
    active_feeds = {
        source: urls for source, urls in FEEDS.items()
        if source not in skipped_feeds
    }
    active_web = get_active_web_sources()
    print('=' * 70)
    print('  中亚研究每日简报 - Scholar Edition')
    print('  Date: ' + str(TODAY) + '  |  Stable mode: ' + ('ON' if STABLE_MODE else 'OFF'))
    print('  Active RSS: ' + str(len(active_feeds)) + '/' + str(len(FEEDS)) + '  |  Active Web: ' + str(len(active_web)) + '/' + str(len(WEB_SOURCES)))
    if TEST_CANDIDATE_SOURCES:
        print('  Candidate Web: ' + str(len(CANDIDATE_WEB_SOURCES)))
    print('  Deep supplements: Telegram ' + ('ON ' if ENABLE_TELEGRAM_SOURCES else 'OFF ') + str(len(TELEGRAM_SOURCES)) + ' | Reports ' + str(len(PDF_REPORT_SOURCES) + len(REPORT_API_SOURCE_NAMES)) + ' | Meetings ' + str(len(MEETING_MINUTES_SOURCES)) + ' | Academic tasks ' + str(ACADEMIC_DAILY_TASK_COUNT))
    print('=' * 70)
    candidate_web_jobs = []
    extra_source_jobs = []

    print('\n[1/3] Fetching RSS feeds...')
    feed_jobs = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        future_map = {}
        for sn, urls in active_feeds.items():
            for url in urls:
                future = executor.submit(fetch_feed, url, sn)
                future_map[future] = sn
        for future in as_completed(future_map):
            sn = future_map[future]
            items = future.result()
            all_items.extend(items)
            feed_jobs.append((sn, len(items)))
    print('  RSS sources with items: ' + str(sources_with_items(feed_jobs)) + '/' + str(len(active_feeds))
          + ' (' + str(jobs_with_items(feed_jobs)) + ' non-empty URL jobs)')
    print('  Skipped RSS sources: ' + str(len(skipped_feeds)))

    print('\n[1b/3] Fetching global and neighboring-country deep-discovery queries...')
    deep_discovery_jobs = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_map = {}
        for sn, urls in DEEP_DISCOVERY_SOURCES.items():
            for url in urls:
                future = executor.submit(fetch_feed, url, sn, 'DEEP_DISCOVERY')
                future_map[future] = sn
        for future in as_completed(future_map):
            sn = future_map[future]
            items = future.result()
            all_items.extend(items)
            deep_discovery_jobs.append((sn, len(items)))
    extra_source_jobs.append({
        'label': '全球及周边国家多语种深度发现查询',
        'kind': 'DEEP_DISCOVERY',
        'jobs': deep_discovery_jobs,
        'total': DEEP_DISCOVERY_TOTAL_TASKS,
    })
    print('  Deep-discovery queries with items: ' + str(jobs_with_items(deep_discovery_jobs)) + '/' + str(DEEP_DISCOVERY_TOTAL_TASKS))

    print('\n[2/3] Scraping web sources...')
    web_jobs = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {executor.submit(fetch_web, sn, url): sn for sn, url in active_web.items()}
        for future in as_completed(future_map):
            sn = future_map[future]
            items = future.result()
            all_items.extend(items)
            web_jobs.append((sn, len(items)))
    print('  Web sources with items: ' + str(sources_with_items(web_jobs)) + '/' + str(len(active_web)))
    print('  Skipped web sources: ' + str(len(WEB_SOURCES) - len(active_web)))
    if TEST_CANDIDATE_SOURCES:
        print('\n[2b/3] Testing candidate web sources...')
        with ThreadPoolExecutor(max_workers=6) as executor:
            future_map = {
                executor.submit(fetch_web, sn, url, 'CANDIDATE_WEB'): sn
                for sn, url in CANDIDATE_WEB_SOURCES.items()
            }
            for future in as_completed(future_map):
                sn = future_map[future]
                items = future.result()
                all_items.extend(items)
                candidate_web_jobs.append((sn, len(items)))
        print('  Candidate web sources with items: ' + str(sources_with_items(candidate_web_jobs)) + '/' + str(len(CANDIDATE_WEB_SOURCES)))

    if ENABLE_TELEGRAM_SOURCES:
        print('\n[2c/3] Fetching Telegram public preview sources...')
        telegram_jobs = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            future_map = {
                executor.submit(fetch_telegram, sn, url): sn
                for sn, url in TELEGRAM_SOURCES.items()
            }
            for future in as_completed(future_map):
                sn = future_map[future]
                items = future.result()
                all_items.extend(items)
                telegram_jobs.append((sn, len(items)))
        extra_source_jobs.append({
            'label': 'Telegram 公开频道',
            'kind': 'TELEGRAM',
            'jobs': telegram_jobs,
            'total': len(TELEGRAM_SOURCES),
        })
        print('  Telegram sources with items: ' + str(jobs_with_items(telegram_jobs)) + '/' + str(len(TELEGRAM_SOURCES)))

    if ENABLE_PDF_REPORT_SOURCES:
        print('\n[2d/3] Fetching PDF/report sources...')
        pdf_report_jobs = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_map = {
                executor.submit(fetch_pdf_reports, sn, url): sn
                for sn, url in PDF_REPORT_SOURCES.items()
            }
            for future in as_completed(future_map):
                sn = future_map[future]
                items = future.result()
                all_items.extend(items)
                pdf_report_jobs.append((sn, len(items)))
        world_bank_items = fetch_world_bank_reports()
        all_items.extend(world_bank_items)
        pdf_report_jobs.append(('World Bank Documents & Reports API', len(world_bank_items)))
        extra_source_jobs.append({
            'label': 'PDF/报告源',
            'kind': 'PDF_REPORT',
            'jobs': pdf_report_jobs,
            'total': len(PDF_REPORT_SOURCES) + len(REPORT_API_SOURCE_NAMES),
        })
        print('  PDF/report sources with items: ' + str(jobs_with_items(pdf_report_jobs)) + '/' + str(len(PDF_REPORT_SOURCES) + len(REPORT_API_SOURCE_NAMES)))

        print('\n[2d2/3] Fetching national country-assessment pages...')
        country_assessment_records = country_assessment_seed_records()
        country_assessment_jobs = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_map = {
                executor.submit(fetch_country_assessment, record): record
                for record in country_assessment_records
            }
            for future in as_completed(future_map):
                record = future_map[future]
                items = future.result()
                all_items.extend(items)
                job_name = (
                    record['source'] + ': ' + record['name']
                    + ((' ' + str(record['edition_year'])) if record.get('edition_year') else '')
                )
                country_assessment_jobs.append((job_name, len(items)))
        extra_source_jobs.append({
            'label': '五国权威国家研究报告直连页',
            'kind': 'RESEARCH_REPORT',
            'jobs': country_assessment_jobs,
            'total': len(country_assessment_records),
        })
        print(
            '  Country-assessment pages with items: '
            + str(jobs_with_items(country_assessment_jobs)) + '/'
            + str(len(country_assessment_records))
        )

    if ENABLE_MEETING_MINUTES_SOURCES:
        print('\n[2e/3] Fetching meeting/minutes sources...')
        meeting_jobs = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_map = {
                executor.submit(fetch_meeting_minutes, sn, url): sn
                for sn, url in MEETING_MINUTES_SOURCES.items()
            }
            for future in as_completed(future_map):
                sn = future_map[future]
                items = future.result()
                all_items.extend(items)
                meeting_jobs.append((sn, len(items)))
        extra_source_jobs.append({
            'label': '会议纪要/多边机制',
            'kind': 'MEETING',
            'jobs': meeting_jobs,
            'total': len(MEETING_MINUTES_SOURCES),
        })
        print('  Meeting/minutes sources with items: ' + str(jobs_with_items(meeting_jobs)) + '/' + str(len(MEETING_MINUTES_SOURCES)))

    if ENABLE_ACADEMIC_SOURCES:
        print('\n[2f/3] Fetching quality academic paper signals...')
        reset_academic_fetch_diag()
        academic_jobs = []
        academic_tasks = []
        # Primary: whitelist-journal targeted pulls (OpenAlex source IDs + Crossref ISSNs).
        academic_tasks.append(('OpenAlex-Whitelist', fetch_openalex_whitelist_sources, None))
        academic_tasks.append(('Crossref-ISSN', fetch_crossref_whitelist_issns, None))
        # Secondary: thematic OpenAlex queries remain constrained to whitelist
        # venues. Crossref does not repeat these broad queries.
        for query in ACADEMIC_QUERIES:
            academic_tasks.append(('OpenAlex', fetch_openalex, query))
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_map = {}
            for provider, fetcher, query in academic_tasks:
                if query is None:
                    future_map[executor.submit(fetcher)] = (provider, 'whitelist-targeted')
                else:
                    future_map[executor.submit(fetcher, query)] = (provider, query)
            for future in as_completed(future_map):
                provider, query = future_map[future]
                try:
                    items = future.result()
                except Exception as exc:
                    note_academic_diag('errors')
                    record_source_warning('ACADEMIC', provider, query, exc)
                    items = []
                all_items.extend(items)
                academic_jobs.append((provider + ': ' + query, len(items)))
        extra_source_jobs.append({
            'label': '优质学术论文 OpenAlex+Crossref',
            'kind': 'ACADEMIC',
            'jobs': academic_jobs,
            'total': len(academic_tasks),
        })
        print('  Quality academic searches with items: ' + str(jobs_with_items(academic_jobs)) + '/' + str(len(academic_tasks)))
        print('  Academic gate diag: ' + ', '.join(
            key + '=' + str(ACADEMIC_FETCH_DIAG.get(key, 0))
            for key in ['api_results', 'pass', 'venue_not_whitelist', 'abstract_short', 'no_ca_anchor', 'errors', 'api_429_retry', 'api_success_after_429', 'api_final_429']
        ))

    print('  Main source warnings recorded: ' + str(warning_count({'RSS', 'WEB'})))
    if TEST_CANDIDATE_SOURCES:
        print('  Candidate warnings recorded: ' + str(warning_count({'CANDIDATE_WEB'})))
        update_candidate_history(candidate_web_jobs)
    if extra_source_jobs:
        print('  Blindspot source warnings recorded: ' + str(warning_count(SPECIAL_SOURCE_WARNING_KINDS)))
    write_source_health_log(feed_jobs, web_jobs, len(active_feeds), len(active_web), candidate_web_jobs, extra_source_jobs)
    print('  Details saved: ' + str(SOURCE_HEALTH_LOG.name))

    print('\n[3/3] Filtering & Categorizing...')
    relevant, neg_tagged_items = filter_items(all_items)
    cached_daily_items = load_daily_selection_cache()
    if cached_daily_items:
        anchored_live = mark_same_day_anchors(relevant, cached_daily_items)
        retained_cached_items = []
        for anchor_rank, cached_item in enumerate(cached_daily_items):
            item = dict(cached_item)
            item['same_day_anchor'] = True
            item['same_day_anchor_rank'] = anchor_rank
            # The snapshot is a fallback copy, not a quality exemption. Keep it
            # alongside a live variant and let same-run dedupe prefer the richer
            # live evidence; if live enrichment fails, this copy can still pass.
            refresh_item_evidence(item)
            if is_research_grade_public_item(item):
                retained_cached_items.append(item)
        relevant.extend(retained_cached_items)
        print('  Same-day live anchors: ' + str(anchored_live))
        print('  Restored fully qualified same-day selections: ' + str(len(retained_cached_items)))
    # Async fetch completion order is nondeterministic. Put richer copies first so
    # same-run dedupe keeps the dated/full-summary version of a syndicated item.
    relevant.sort(key=lambda item: (
        *same_day_anchor_sort_key(item),
        *(-value for value in same_run_variant_score(item)),
        -published_ordinal(item),
        clean_text(item.get('source', '')).lower(),
        clean_text(item.get('title', '')).lower(),
        normalize_history_link(item.get('link', '')).lower(),
    ))
    print('  Total fetched: ' + str(len(all_items)))
    print('  Central Asia relevant: ' + str(len(relevant)))

    seen_history = load_seen_history()
    prior_keys = prior_seen_keys(seen_history)
    cross_day_skipped = 0
    same_run_skipped = 0
    deduped = []
    current_history_keys = set()
    for item in relevant:
        item_keys = item_history_keys(item)
        if item_keys & prior_keys:
            cross_day_skipped += 1
            # Preserve historical collisions for the non-counting review shelf.
            # The previous path discarded them before the later review collector,
            # which made a valid prior-day item disappear completely.
            if is_research_grade_public_item(item):
                recent_review_items.append(item)
            continue
        if item_keys & current_history_keys:
            same_run_skipped += 1
            continue
        iid = normalize_key(item)
        if iid not in seen_hashes:
            seen_hashes.add(iid)
            current_history_keys.update(item_keys)
            deduped.append(item)
    print('  Skipped by cross-day history: ' + str(cross_day_skipped))
    print('  Skipped by same-run history: ' + str(same_run_skipped))

    # Prestige scores
    PRESTIGE = {
        'Carnegie Endowment Central Asia': 95, 'Eurasianet': 90,
        'CABAR.asia': 90, 'The Times of Central Asia': 85,
        'Caspian Policy Center': 80, 'Oxus Society': 80,
        'Dialogue Earth': 84, 'The Third Pole': 84, 'IWPR Central Asia': 86,
        'Oxus Society RSS': 82, 'ISRS Uzbekistan': 80, 'IISS Online Analysis': 86,
        'International Crisis Group Central Asia': 88,
        'Human Rights Watch Central Asia': 78, 'Eurasian Development Bank': 78,
        'UNRCCA': 78, 'UNDP Eurasia': 72, 'OSCE News': 72,
        'ADB Central and West Asia': 72, 'World Bank ECA': 72,
        'The Diplomat': 85, 'Foreign Policy': 80, 'Foreign Affairs': 80,
        'Financial Times World': 90, 'Financial Times Asia': 90,
        'The Economist Asia': 90, 'The Economist Europe': 88,
        'New York Times World': 84, 'Nikkei Asia': 82,
        'War on the Rocks': 82, 'German Marshall Fund': 80,
        'Eurasia Daily Monitor (Jamestown)': 85, 'Central Asia-Caucasus Analyst': 85,
        'Central Asia New Strategies': 85, 'Central Asian Survey': 80,
        'Post-Soviet Affairs': 80, 'Inner Asia': 80, 'Kritika': 80,
        'Reuters World': 100, 'Al Jazeera': 90, 'BBC World': 90,
        'Meduza': 70, 'TASS': 65, 'RIA Novosti': 65,
        'Azattyq (Kazakh)': 75, 'Ozodi (Uzbek/Tajik)': 75,
        'Khronika.info': 70, 'Regnum Agency': 60,
        'Carnegie Endowment': 95, 'CSIS Central Asia': 80,
        'Atlantic Council Central Asia': 75, 'Brookings Russia and Eurasia': 75,
        'Stimson Center Eurasia': 70, 'Chatham House Russia and Eurasia': 75,
        'RUSI Central Asia': 75, 'ISW': 80, 'Lowy Interpreter': 70,
        'DW English': 75, 'France 24 EN': 75, 'Euronews': 75,
        'The Guardian World': 75, 'RFE/RL Central Asia': 80,
        'AP News': 75, 'Kavkaz.Realii': 65,
        'SCMP Asia': 65, 'Caixin Global': 65, 'CGTN': 60, 'China Daily': 60,
        'Kazinform': 75, 'Informburo': 70, 'Tengrinews': 70, 'Zakon.kz': 70,
        'Informburo.kz': 70, 'Tengrinews.kz': 70, 'The Astana Times': 75,
        'Vlast.kz': 72, 'Orda.kz': 70, 'Kapital.kz': 68,
        'UzA': 70, 'Gazeta.uz': 70, 'Kun.uz': 70, 'Report.uz': 65,
        'Daryo.uz': 68, 'Podrobno.uz': 68, 'Spot.uz': 70,
        'Kabar KG': 70, '24.kg': 65, 'KgNews': 65, 'AKIpress': 70, 'Kloop': 72,
        'Khovar TJ': 65, 'Asia-Plus TJ': 65, 'Somon.tj': 60, 'Avesta TJ': 63,
        'TDH TM': 60, 'Business Turkmenistan': 68, 'Turkmenportal': 64, 'Orient TM': 64,
        'Trend AZ': 65, 'Day.az': 60,
        'Central Asia Program (Wilson Center)': 80,
        'Central Asia Foundation': 70, 'Open Society Foundations (Central Asia)': 70,
        'NDTV World': 60, 'Al Arabiya English': 60,
        'Middle East Eye': 65, 'RSIS Singapore': 70, 'SAIIA': 60,
        'Lenta.ru': 60, 'Novaya Gazeta': 65, 'Interfax': 65,
        'Gazeta.ru': 60, 'Washington Post World': 80, 'Le Monde': 75,
        'Aktualno.kz': 65, 'Nur.kz': 65, 'XalqSozi': 65, 'Zonadaily.uz': 60,
        'Asiacenter.kg': 65, 'Arna.kg': 60, 'Ziyo.net': 60,
        'Nahod.tj': 60, 'Vazhnoe.tj': 60, 'Tribune.TM': 60, 'News-tm': 60,
        'Kyrgyzstan Today': 65, 'Dunyoxabarlari.uz': 65,
        'China-US Focus': 65, 'Navro': 60, 'Kabar.kg': 70,
        'Kazpravda': 65, 'Turbina7.kz': 65,
        'Akorda': 82, 'Kazakhstan Government': 78, 'Kazakhstan MFA': 78,
        'National Bank of Kazakhstan': 78,
        'President of Uzbekistan': 82, 'Uzbekistan Government': 78,
        'Central Bank of Uzbekistan': 78, 'Statistics Agency Uzbekistan': 76,
        'President of Kyrgyzstan': 78, 'Kyrgyz Cabinet': 75, 'Kyrgyz MFA': 75,
        'National Bank Kyrgyzstan': 76, 'Kyrgyz Statistics': 74,
        'Tajik MFA': 74, 'National Bank Tajikistan': 74,
        'Turkmenistan Official': 74, 'Turkmenistan MFA': 74,
        'Forbes Kazakhstan': 70, 'Kaktus.media': 70, 'Your.tj': 68,
        'New Lines Central Asia': 84, 'SpecialEurasia Central Asia': 78,
        'CAREC': 78, 'IMF Central Asia': 80,
        'KISI KazISS Analytics': 78,
        'KISI KazISS RSS': 84,
        'CAREC Institute Publications': 82,
        'Central Asia Program Policy Briefs': 84,
        'Central Asia Program RSS': 86,
        'OSCE Academy Policy Briefs': 84,
        'EUCAM Policy Briefs': 82,
        'University of Central Asia Publications': 84,
        'FES Central Asia Publications': 80,
        'CAPS Unlock Publications': 78,
        'CAPS Unlock RSS': 82,
        'IAI Publications': 80,
        'Silk Road Studies Publications': 82,
        'Wilson Center Search Central Asia': 82,
        'Kennan Institute Search Central Asia': 82,
        'Davis Center Harvard Central Asia': 80,
        'FPRI Search Central Asia': 80,
        'Foreign Policy Centre Search Central Asia': 76,
        'SIPRI Search Central Asia': 82,
        'RAND Search Central Asia': 82,
        'China Global South Central Asia': 78,
        'CER Search Central Asia': 78,
        'PONARS Eurasia': 82, 'Voices on Central Asia': 82,
        'Novastan English': 76, 'Eurasian Research Institute': 76,
        'Global Voices Central Asia': 72,
        'Caspian Policy Center RSS': 80,
        'The Diplomat Central Asia': 86,
        'The Diplomat China-Central Asia': 84,
        'Riddle Russia': 80,
        'OSW Central Asia': 82,
        'Kursiv Kazakhstan English': 72,
        'SWP Berlin': 82, 'Clingendael': 80, 'EUISS': 80,
        'ECFR': 78, 'Dialogue Earth': 76,
        'Gazeta.uz Telegram': 68, 'Kun.uz Telegram': 68, 'Daryo Telegram': 66,
        'Vlast.kz Telegram': 70, 'Orda.kz Telegram': 68, 'AKIpress Telegram': 68,
        'Kloop Telegram': 70, 'Asia-Plus TJ Telegram': 66, 'Your.tj Telegram': 64,
        'Orient TM Telegram': 64, 'CABAR.asia Telegram': 80,
        'Fergana Agency Telegram': 70, 'Novastan Telegram': 76,
        'EDB Reports': 82, 'EBRD Publications': 80,
        'World Bank ECA Publications': 82, 'World Bank Documents & Reports': 92,
        'CAREC Publications': 78,
        'SCO News': 74, 'CICA Press Releases': 74, 'OIC News': 68,
        'CAREC Events': 72, 'UNRCCA Press Releases': 76,
        'Academic: Crossref': 84, 'Academic: OpenAlex': 86,
    }
    DISCOVERY_PRESTIGE = {
        'reuters': 100, 'financial times': 99, 'the economist': 98,
        'new york times': 97, 'foreign affairs': 96, 'foreign policy': 95,
        'international crisis group': 96, 'carnegie': 95, 'brookings': 95,
        'csis': 94, 'chatham house': 94, 'rand': 94, 'sipri': 93,
        'iiss': 93, 'rusi': 92, 'oecd': 92, 'world bank': 92, 'adb': 91,
        'osw': 91, 'swp': 91, 'clingendael': 90, 'ifri': 90,
        'central asia program': 89, 'eucam': 89, 'wilson center': 89,
        'the diplomat': 88, 'eurasianet': 88, 'the times of central asia': 86,
        'caspian policy center': 84, 'the astana times': 82,
        'frontiers': 68,
    }

    def prestige_score(item):
        source = item.get('source', '')
        if item.get('source_type') == 'institution_publication':
            return 94 if item.get('source_tier', 2) == 1 else 84
        if item.get('source_type') == 'academic_paper':
            return 92
        if is_deep_discovery_source(source):
            publisher = deep_discovery_publisher(source)
            for name, score in DISCOVERY_PRESTIGE.items():
                if name in publisher:
                    return score
            return 80
        return PRESTIGE.get(source, 0)

    deduped.sort(key=lambda x: (
        *same_day_anchor_sort_key(x),
        -x.get('priority_score', 0),
        -x.get('depth_score', 0),
        -prestige_score(x),
        -int(x.get('word_count', 0) or 0),
        -x.get('kw_score', 0),
        -published_ordinal(x),
        clean_text(x.get('source', '')).lower(),
        clean_text(x.get('title', '')).lower(),
        normalize_history_link(x.get('link', '')).lower(),
    ))
    all_deduped = deduped
    # Neg-tagged items: only keep if they still look like research-grade depth material.
    # Ordinary news with weak negative-keyword collisions must not enter internal as a dump.
    neg_tagged_internal = [
        item for item in neg_tagged_items
        if item.get('core_score', 0) >= 1
        and not is_public_simple_news(item)
        and not is_public_low_value(item)
        and (is_strict_deep_public_item(item) or is_deep_item(item))
    ]
    deduped.extend(neg_tagged_internal)
    all_deduped = deduped

    # Pre-gate resolve+enrich for high-signal deep candidates so research-grade and
    # soft-risk decisions use original-page evidence, not thin Google News blurbs.
    pregate_pool = []
    for item in all_deduped:
        source = item.get('source', '')
        if (
            is_deep_discovery_source(source)
            or source in HIGH_SIGNAL_DEEP_SOURCES
            or source in PRESTIGE_LONGFORM_SOURCES
            or item.get('source_type') == 'institution_publication'
        ):
            if item.get('core_score', 0) >= 1 and not is_public_low_value(item):
                pregate_pool.append(item)
    # Cap runtime while giving short-summary sources a fair chance to obtain
    # original-page evidence. The old 40/2 cap systematically favored the
    # same few publishers before enrichment; use a wider, source-balanced pool.
    pregate_pool = diversify_ranked_items(
        pregate_pool,
        PREGATE_POOL_SIZE,
        min_unique_sources=min(60, PREGATE_POOL_SIZE),
        max_per_source=4,
    )
    if pregate_pool:
        print('  Pre-gate resolve/enrich for deep candidates: ' + str(len(pregate_pool)))
        resolve_item_links(pregate_pool, label='pre-gate deep candidates')
        enrich_items_for_output(pregate_pool, label='pre-gate deep candidates')
        for item in pregate_pool:
            refresh_item_evidence(item)

    # Enrichment may change depth/research scores, so restore ranked order before
    # the final history and public gates.
    all_deduped.sort(key=lambda x: (
        *same_day_anchor_sort_key(x),
        -x.get('priority_score', 0),
        -x.get('depth_score', 0),
        -prestige_score(x),
        -int(x.get('word_count', 0) or 0),
        -x.get('kw_score', 0),
        -published_ordinal(x),
        clean_text(x.get('source', '')).lower(),
        clean_text(x.get('title', '')).lower(),
        normalize_history_link(x.get('link', '')).lower(),
    ))

    # Google News / aggregator links only reveal the original URL after resolve.
    # Re-run history dedupe so cross-publisher reposts (shared slug/title core) are caught.
    all_deduped, post_resolve_skipped, _ = drop_history_duplicate_items(
        all_deduped, prior_keys, skipped_sink=recent_review_items
    )
    if post_resolve_skipped:
        cross_day_skipped += post_resolve_skipped
        print('  Skipped by post-resolve history: ' + str(post_resolve_skipped))
    deduped = list(all_deduped)

    internal_review_items = [
        item for item in all_deduped
        if is_publication_risky(item) and is_internal_review_grade_item(item)
    ]
    prior_internal_review_keys = collect_prior_internal_review_keys()
    internal_review_repeat_skipped = 0
    if prior_internal_review_keys:
        fresh_internal_review_items = []
        for item in internal_review_items:
            if item_history_keys(item) & prior_internal_review_keys:
                internal_review_repeat_skipped += 1
                continue
            fresh_internal_review_items.append(item)
        internal_review_items = fresh_internal_review_items
    low_value_public_items = [
        item for item in all_deduped
        if not is_publication_risky(item) and is_public_low_value(item)
    ]
    low_research_public_items = [
        item for item in all_deduped
        if not is_publication_risky(item)
        and not is_public_low_value(item)
        and not is_research_grade_public_item(item)
    ]
    deduped = [
        item for item in all_deduped
        if not is_publication_risky(item)
        and not is_public_low_value(item)
        and not is_public_simple_news(item)
        and not is_institute_soft_content(item)
        and is_research_grade_public_item(item)
    ]
    # If the strict public gate unexpectedly yields zero, retain a small,
    # clearly logged reserve of newly discovered, open-access, high-authority
    # research items. This prevents an adapter/gate mismatch from masquerading
    # as a world-wide absence of Central Asia research.
    if not deduped:
        discovery_reserve = [
            item for item in all_deduped
            if item.get('source_tier', 3) <= 2
            and item.get('access_status') not in {'paywalled', 'blocked'}
            and item.get('link')
            and is_new_discovery_item(item)
            and has_strong_central_asia_anchor(item)
            and not is_publication_risky(item)
            and not is_public_low_value(item)
            and not is_public_simple_news(item)
            and not is_institute_soft_content(item)
            and (is_deep_item(item) or is_report_grade_item(item) or is_substantive_policy_document(item))
        ]
        discovery_reserve.sort(key=lambda x: (
            *same_day_anchor_sort_key(x),
            -int(x.get('source_tier', 3)),
            -x.get('depth_score', 0),
            -published_ordinal(x),
        ))
        deduped = discovery_reserve[:5]
        if deduped:
            print('  Discovery reserve activated: ' + str(len(deduped)))
    # Recovery pass: candidates that were only short Google News blurbs may
    # become public-grade after original-page enrichment. Keep this pass
    # separate from the main pool so a ranking quirk cannot make the whole
    # public digest zero before evidence is fetched.
    if len(deduped) < MIN_PUBLIC_RECOMMENDATIONS:
        existing_ids = {item.get('id') or item.get('link') for item in deduped}
        recovery_pool = [
            item for item in all_deduped
            if (item.get('id') or item.get('link')) not in existing_ids
            and not is_publication_risky(item)
            and not is_public_low_value(item)
            and (
                is_deep_discovery_source(item.get('source', ''))
                or item.get('source') in HIGH_SIGNAL_DEEP_SOURCES
                or item.get('source') in PRESTIGE_LONGFORM_SOURCES
                or item.get('source') in (set(PDF_REPORT_SOURCES) | REPORT_API_SOURCE_NAMES)
                or item.get('source_type') == 'institution_publication'
            )
        ]
        recovery_pool = diversify_ranked_items(
            recovery_pool,
            PREGATE_RECOVERY_POOL_SIZE,
            min_unique_sources=min(30, PREGATE_RECOVERY_POOL_SIZE),
            max_per_source=PREGATE_RECOVERY_MAX_ITEMS_PER_SOURCE,
        )
        if recovery_pool:
            print('  Recovery resolve/enrich for public shortfall: ' + str(len(recovery_pool)))
            resolve_item_links(recovery_pool, label='public shortfall recovery')
            enrich_items_for_output(recovery_pool, label='public shortfall recovery')
            for item in recovery_pool:
                refresh_item_evidence(item)
            recovery_public = [
                item for item in recovery_pool
                if not is_publication_risky(item)
                and not is_public_low_value(item)
                and not is_public_simple_news(item)
                and not is_institute_soft_content(item)
                and is_research_grade_public_item(item)
            ]
            current_keys = set()
            for item in deduped:
                current_keys.update(item_history_keys(item))
            recovery_public, recovery_skipped, _ = drop_history_duplicate_items(
                recovery_public, prior_keys, current_keys
            )
            if recovery_skipped:
                cross_day_skipped += recovery_skipped
            if recovery_public:
                print('  Recovered public candidates: ' + str(len(recovery_public)))
                deduped.extend(recovery_public)
    # Keep public pool within researcher reading budget (prefer 8-12, hard max 15).
    max_public_pool = max(TARGET_PUBLIC_RECOMMENDATIONS, MIN_PUBLIC_RECOMMENDATIONS)
    max_public_pool = min(15, max(max_public_pool, TARGET_PUBLIC_RECOMMENDATIONS))
    deduped.sort(key=lambda x: (
        *same_day_anchor_sort_key(x),
        -x.get('priority_score', 0),
        -x.get('depth_score', 0),
        -prestige_score(x),
        -int(x.get('word_count', 0) or 0),
        -x.get('kw_score', 0),
        -published_ordinal(x),
        clean_text(x.get('source', '')).lower(),
        clean_text(x.get('title', '')).lower(),
        normalize_history_link(x.get('link', '')).lower(),
    ))
    # Apply diversity while the wider eligible pool is still available. The old
    # code sliced top-12 first and only then applied source caps in the renderer.
    eligible_public_pool = list(deduped)
    deduped = diversify_ranked_items(
        eligible_public_pool,
        max_public_pool,
        min_unique_sources=MIN_PUBLIC_SOURCE_DIVERSITY,
        max_per_source=MAX_PUBLIC_ITEMS_PER_SOURCE,
        max_academic_items=MAX_PUBLIC_ACADEMIC_ITEMS,
    )
    if len(deduped) < MIN_PUBLIC_RECOMMENDATIONS:
        before_shortfall = len(deduped)
        deduped = extend_public_shortfall(deduped, eligible_public_pool)
        if len(deduped) > before_shortfall:
            print('  Added authority shortfall items: ' + str(len(deduped) - before_shortfall))
    print('  WeChat-safe public items: ' + str(len(deduped)))
    print('  Moved to internal review: ' + str(len(internal_review_items)))
    print('  Skipped repeated internal review items: ' + str(internal_review_repeat_skipped))
    print('  Dropped low-value public items: ' + str(len(low_value_public_items)))
    print('  Dropped stale or low-research public items: ' + str(len(low_research_public_items)))

    CATS = {
        'think_tank': [], 'pol_sec': [], 'econ': [], 'diplomacy': [],
        'news': [], 'ru': [], 'local': [], 'cn': [],
    }
    for item in deduped:
        cat = categorize_item(item)
        CATS[cat].append(item)

    # 按国家细分本地媒体
    LOCAL_SUB = {
        'local_kz': [], 'local_uz': [], 'local_kg': [], 'local_tj': [], 'local_tm': [],
    }
    for item in CATS.get('local', []):
        s = item['source']
        if s in local_kz or s in REGIONAL_LOCAL:
            LOCAL_SUB['local_kz'].append(item)
        elif s in local_uz:
            LOCAL_SUB['local_uz'].append(item)
        elif s in local_kg:
            LOCAL_SUB['local_kg'].append(item)
        elif s in local_tj:
            LOCAL_SUB['local_tj'].append(item)
        elif s in local_tm:
            LOCAL_SUB['local_tm'].append(item)

    print('Generating markdown...')
    # Resolve Google News redirects only for items likely to be shown.
    resolve_item_links(deduped, label='public candidates')
    resolve_item_links(internal_review_items, label='internal review')
    # Final history pass after public/internal resolve (covers non-pregate items).
    deduped, final_pub_skipped, final_keys = drop_history_duplicate_items(
        deduped, prior_keys, skipped_sink=recent_review_items, exact_link_only=True
    )
    internal_review_items, final_int_skipped, _ = drop_history_duplicate_items(
        internal_review_items, prior_keys, final_keys, exact_link_only=True
    )
    if final_pub_skipped or final_int_skipped:
        cross_day_skipped += final_pub_skipped + final_int_skipped
        print('  Skipped by final-resolve history: public ' + str(final_pub_skipped)
              + ' / internal ' + str(final_int_skipped))
    # After resolution, enrich original pages for better clues and depth evidence.
    enrich_items_for_output(deduped, label='public candidates')
    enrich_items_for_output(internal_review_items, label='internal review')
    # Keep only recent, high-grade, non-paywalled review items; de-duplicate the
    # review shelf itself and never let it affect today's new-item count.
    review_seen = set()
    recent_review_items = [
        item for item in recent_review_items
        if normalize_key(item) not in review_seen
        and not review_seen.add(normalize_key(item))
        and parse_item_published_date(item)
        and (TODAY - parse_item_published_date(item)).days <= 30
        and item.get('source_tier', 3) <= 2
        and item.get('access_status') != 'paywalled'
        and is_research_grade_public_item(item)
        and not is_public_low_value(item)
        and not is_public_simple_news(item)
        and not is_institute_soft_content(item)
    ]
    recent_review_items.sort(
        key=lambda item: (recent_review_rank(item), published_ordinal(item)),
        reverse=True,
    )
    recent_review_items = recent_review_items[:5]
    print('  Recent high-grade review items: ' + str(len(recent_review_items)))
    near_misses = summarize_near_misses(low_research_public_items, limit=8)
    try:
        health_lines = []
        if SOURCE_HEALTH_LOG.exists():
            health_lines = SOURCE_HEALTH_LOG.read_text(encoding='utf-8').splitlines()
        health_lines.append('')
        write_near_miss_section(health_lines, near_misses)
        health_lines.append('')
        health_lines.append('机制备注：Google News 中转链在落盘前解析；入选公开/内部条目会抓取原文摘要以改善内容线索；公开版坚持深度优先、宁缺毋滥。')
        atomic_write_text(SOURCE_HEALTH_LOG, chr(10).join(health_lines) + chr(10))
    except Exception:
        pass
    deep_focus = [item for item in deduped if is_deep_item(item)]
    policy_focus = [item for item in deduped if is_policy_data_item(item)]
    if RESEARCHER_LINKLIST_PUBLIC:
        L, published_items = render_researcher_link_digest(
            deduped, deep_focus, policy_focus, CATS,
            active_feeds, active_web, feed_jobs, web_jobs,
            candidate_web_jobs, cross_day_skipped, internal_review_items,
            extra_source_jobs, recent_review_items
        )
    else:
        L, published_items = render_doubao_public_digest(
            deduped, deep_focus, policy_focus, CATS,
            active_feeds, active_web, feed_jobs, web_jobs,
            candidate_web_jobs, cross_day_skipped, internal_review_items
        )

    rendered_markdown = '\n'.join(L)
    atomic_write_text(OUTPUT_FILE, rendered_markdown)
    atomic_write_text(DAILY_RENDER_CACHE_FILE, rendered_markdown)
    published_keys = {normalize_key(item) for item in published_items}
    retained_internal_review_items = [
        item for item in internal_review_items
        if normalize_key(item) not in published_keys
    ]
    write_internal_review_file(retained_internal_review_items)
    append_selection_funnel_to_health_log(
        all_items,
        relevant,
        all_deduped,
        deduped,
        retained_internal_review_items,
        published_items,
        cross_day_skipped,
        same_run_skipped,
        len(low_value_public_items),
        len(low_research_public_items),
    )
    write_selection_audit(
        published_items,
        all_items=all_items,
        relevant=relevant,
        deduped=deduped,
        internal_review_items=retained_internal_review_items,
        cross_day_skipped=cross_day_skipped,
        same_run_skipped=same_run_skipped,
    )
    save_seen_history(seen_history, published_items)
    save_daily_selection_cache(published_items)
    save_translation_cache()
    print('\nDone! Saved: ' + str(OUTPUT_FILE))
    print('Internal review saved: ' + str(INTERNAL_REVIEW_FILE))
    print('Total public items: ' + str(len(deduped)))
    # Summary printed above
    print("Done!")

if __name__ == '__main__':
    main()




