#!/usr/bin/env python3
"""
Merger jadwal olahraga dari dua sumber:
- Sumber 1: cartelive.club/vip3.php  (HTML, nama Prancis, channel ID → URL digenerate)  → UTC+1
- Sumber 2: sportsonline.st/prog.txt (plain text, nama Inggris, URL stream langsung)    → UTC+0

Timezone handling:
- Semua waktu di-normalize ke UTC+0 untuk output
- Sumber 1 (UTC+1) dikurangi 1 jam
- Sumber 2 sudah UTC+0

Output: merged_schedule.json — flat list JSON dengan stream dikelompokkan per bahasa
"""

import json
import re
import sys
import os
import io
import time
import urllib.error
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from html.parser import HTMLParser

# Fix encoding untuk Windows PowerShell (cp1252 tidak support emoji)
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ═══════════════════════════════════════════════
# KONFIGURASI
# ═══════════════════════════════════════════════

SOURCE1_URL = "https://cartelive.club/vip3.php"
SOURCE2_URL = "https://sportsonline.st/prog.txt"
# Base URL untuk generate stream link sumber 1
# Formula: CH{N} → player/{N+3}/1
CARTELIVE_BASE = "https://cartelive.club/player"
CARTELIVE_OFFSET = 3  # CH1 = player/4/1

OUTPUT_FILE = "merged_schedule.json"


# ═══════════════════════════════════════════════
# FETCHER
# ═══════════════════════════════════════════════

def fetch_url(url: str, retries: int = 3, timeout: int = 30) -> str:
    """Fetch URL content dengan User-Agent browser dan mekanisme retry."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    req = Request(url, headers=headers)
    
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            if attempt < retries:
                print(f"      [!] Gagal (percobaan {attempt}/{retries}): {e} - Mencoba lagi dalam 3 detik...")
                time.sleep(3)
            else:
                print(f"      [!] Gagal (percobaan {attempt}/{retries}): {e} - Menyerah.")
    
    raise last_err


# ═══════════════════════════════════════════════
# PARSER HTML SUMBER 1 — Ekstrak textarea content
# ═══════════════════════════════════════════════

class TextareaExtractor(HTMLParser):
    """Ekstrak semua isi dari elemen <textarea>."""

    def __init__(self):
        super().__init__()
        self.in_textarea = False
        self.textareas = []
        self._buf = []

    def handle_starttag(self, tag, attrs):
        if tag == "textarea":
            self.in_textarea = True
            self._buf = []

    def handle_endtag(self, tag):
        if tag == "textarea" and self.in_textarea:
            self.in_textarea = False
            self.textareas.append("".join(self._buf))
            self._buf = []

    def handle_data(self, data):
        if self.in_textarea:
            self._buf.append(data)


def extract_textareas(html: str) -> list[str]:
    """Ambil semua konten textarea dari HTML."""
    parser = TextareaExtractor()
    parser.feed(html)
    return parser.textareas


# ═══════════════════════════════════════════════
# CHANNEL MAP BUILDER — Parse daftar channel sumber 1
# ═══════════════════════════════════════════════

# Mapping suffix bahasa dari channel ID di sumber 1
LANG_SUFFIX_MAP = {
    "fr": "fr",
    "es": "es",
    "us": "en",
    "gb": "en",
    "de": "de",
    "nl": "nl",
    "it": "it",
    "pt": "pt",
    "ar": "ar",
    "tr": "tr",
    "gr": "el",
    "be": "fr",  # Belgium
}

# Bahasa default berdasarkan nama channel (untuk channel tanpa suffix bahasa eksplisit)
CHANNEL_LANG_HINTS = {
    "beIN SPORT": "fr",
    "canal+": "fr",
    "eurosport": "fr",
    "rmc": "fr",
    "equipe": "fr",
    "tf1": "fr",
    "m6": "fr",
    "france": "fr",
    "C+Live": "fr",
    "LIGUE 1 FR": "fr",
    "ES ": "es",
    "DAZN": "es",
    "ESPN": "es",
    "FOX Deportes": "es",
    "beIN En español": "es",
    "NBC UNIVERSO": "es",
    "Telemundo": "es",
    "TyC Sports": "es",
    "WINsport": "es",
    "TNT sport arg": "es",
    "FOXsport": "es",
    "Liga1MAX": "es",
    "GOLPERU": "es",
    "Zapping": "es",
    "directv": "es",
    "TUDN": "es",
    "CANAL5": "es",
    "Azteca": "es",
    "VTV": "es",
    "AYM": "es",
    "LAS latin": "es",
    "DE ": "de",
    "UK ": "en",
    "IT ": "it",
    "NL ": "nl",
    "PT ": "pt",
    "GR ": "el",
    "TR ": "tr",
    "EXTRA SPORT": "en",  # default
}


def guess_channel_lang(channel_name: str) -> str:
    """Tebak bahasa channel berdasarkan nama."""
    for hint, lang in CHANNEL_LANG_HINTS.items():
        if hint.lower() in channel_name.lower():
            return lang
    return "unknown"


def parse_channel_list(raw: str) -> dict:
    """
    Parse daftar channel dari textarea kanan sumber 1.
    Format: (CH1) - beIN SPORT 1
    Return: {1: {"name": "beIN SPORT 1", "lang": "fr"}, ...}
    """
    channels = {}
    pattern = re.compile(r'\(CH(\d+)\)\s*-\s*(.+)')
    for line in raw.splitlines():
        line = line.strip()
        m = pattern.match(line)
        if m:
            ch_num = int(m.group(1))
            ch_name = m.group(2).strip()
            # Bersihkan info tambahan di dalam kurung (bisa ada di mana saja)
            clean_name = re.sub(r'\s*\(.*?\)', '', ch_name).strip()
            lang = guess_channel_lang(clean_name)
            channels[ch_num] = {
                "name": clean_name,
                "full_name": ch_name,
                "lang": lang,
            }
    return channels


def make_cartelive_url(ch_num: int) -> str:
    """Generate streaming URL untuk channel sumber 1."""
    # Format baru: multi.govoet.cc/?envivo={id}
    return f"https://multi.govoet.cc/?envivo={ch_num}"


# ═══════════════════════════════════════════════
# NORMALISASI & FUZZY MATCHING
# ═══════════════════════════════════════════════

def get_eu_dst_offset(dt: datetime, tz_name: str) -> int:
    """
    Menghitung offset waktu berdasar Daylight Saving Time (DST) di Eropa.
    Situs Eropa & UK mengubah jam mereka pada hari Minggu terakhir bulan Maret.
    """
    year = dt.year
    march_last_sun = max(day for day in range(25, 32) if datetime(year, 3, day).weekday() == 6)
    dst_start = datetime(year, 3, march_last_sun, 1, 0)
    
    oct_last_sun = max(day for day in range(25, 32) if datetime(year, 10, day).weekday() == 6)
    dst_end = datetime(year, 10, oct_last_sun, 1, 0)
    
    is_dst = dst_start <= dt < dst_end
    
    if tz_name == "UK":  # Europe/London (GMT/BST)
        return 1 if is_dst else 0
    elif tz_name == "CEU": # Europe/Paris (CET/CEST)
        return 2 if is_dst else 1
    return 0

# Muat kamus terjemahan eksternal untuk tim, league, title
DICT_FILE = "dictionary.json"
translator = {"teams": {}, "leagues": {}, "titles": {}}
skip_events = set()

if os.path.exists(DICT_FILE):
    try:
        with open(DICT_FILE, "r", encoding="utf-8") as f:
            custom_dict = json.load(f)
            for category in ["teams", "leagues", "titles"]:
                if category in custom_dict:
                    for standard_name, aliases in custom_dict[category].items():
                        for alias in aliases:
                            translator[category][alias.lower()] = standard_name
            for skip_str in custom_dict.get("skip_events", []):
                skip_events.add(skip_str.lower())
    except Exception as e:
        print(f"Warning: gagal membaca {DICT_FILE} - {e}")

def apply_translation(text: str, category: str) -> str:
    """Konversi aliase acak menjadi nama standar berdasarkan dictionary.json"""
    if not text:
        return text
    clean_text = text.strip()
    return translator[category].get(clean_text.lower(), clean_text)


KNOWN_LEAGUE_PREFIXES = [
    ("Tennis - ATP World Tour 1000", "Tennis"),
    ("UFC Fight Night", "UFC"),
    ("DP World Tour", "Golf"),
    ("Formula 1", "Formula 1"),
    ("MotoGP", "MotoGP"),
    ("Moto2/3", "Moto2/3"),
    ("Moto2", "Moto2"),
    ("Moto3", "Moto3"),
    ("NBA", "NBA"),
    ("NHL", "NHL"),
    ("MLB", "MLB"),
    ("NFL", "NFL"),
    ("WNBA", "WNBA"),
    ("Volleyball", "Volleyball"),
    ("Basketball", "Basketball"),
    ("Boxing", "Boxing"),
]


def normalize_text(name: str) -> str:
    """Normalisasi teks untuk matching: lowercase, hapus aksen, normalisasi spasi."""
    name = name.strip().lower()
    replacements = {
        'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a',
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'í': 'i', 'ì': 'i', 'î': 'i',
        'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o',
        'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u',
        'ñ': 'n', 'ç': 'c',
    }
    for src, dst in replacements.items():
        name = name.replace(src, dst)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def tokenize(text: str) -> set[str]:
    """Pecah teks jadi token, hapus kata terlalu pendek."""
    tokens = set(normalize_text(text).split())
    # Hapus token pendek & umum
    stop_words = {"fc", "cf", "ac", "sc", "de", "la", "el", "x", "vs", "@", "-", "w", "u19", "u20"}
    return {t for t in tokens if len(t) > 2 and t not in stop_words}


def similarity_score(text_a: str, text_b: str) -> float:
    """Hitung skor kecocokan token overlap (Jaccard similarity)."""
    ta = tokenize(text_a)
    tb = tokenize(text_b)
    if not ta or not tb:
        return 0.0
    overlap = len(ta & tb)
    union = len(ta | tb)
    return overlap / union if union > 0 else 0.0


def team_name_match(name_a: str, name_b: str) -> bool:
    """Cek apakah dua nama tim cukup mirip untuk dianggap sama."""
    na = normalize_text(name_a)
    nb = normalize_text(name_b)
    # Exact match setelah normalize
    if na == nb:
        return True
    # Token overlap >= 50%
    ta = tokenize(name_a)
    tb = tokenize(name_b)
    if not ta or not tb:
        return False
    overlap = len(ta & tb)
    # Minimal separuh token match
    return overlap >= min(len(ta), len(tb)) * 0.5


def events_are_matchable(e1: dict, e2: dict) -> float:
    """
    Hitung skor matching antara dua event dengan logika yang lebih cerdas.
    Return: 0.0 - 1.0 (atau lebih dengan bonus)
    """
    # Tanggal HARUS sama
    if e1["date"] != e2["date"]:
        return 0.0

    # Cek jarak waktu — toleransi ±120 menit (sumber kadang beda interpretasi waktu)
    try:
        t1 = datetime.strptime(e1["time_utc"], "%H:%M")
        t2 = datetime.strptime(e2["time_utc"], "%H:%M")
        diff_min = abs((t1 - t2).total_seconds()) / 60
        # Handle midnight wrap (23:50 vs 00:10 = 20 min, bukan 1420 min)
        if diff_min > 720:
            diff_min = 1440 - diff_min
        if diff_min > 120:
            return 0.0  # Terlalu jauh waktunya
    except (ValueError, KeyError):
        pass

    # Jika kedua event punya matchup (team_home & team_away)
    e1_has_teams = e1.get("team_home") and e1.get("team_away")
    e2_has_teams = e2.get("team_home") and e2.get("team_away")

    if e1_has_teams and e2_has_teams:
        # Mode matchup: kedua tim HARUS cocok (home vs home, away vs away)
        home_ok = team_name_match(e1["team_home"], e2["team_home"])
        away_ok = team_name_match(e1["team_away"], e2["team_away"])
        # Atau terbalik (home vs away, away vs home)
        home_rev = team_name_match(e1["team_home"], e2["team_away"])
        away_rev = team_name_match(e1["team_away"], e2["team_home"])

        if (home_ok and away_ok) or (home_rev and away_rev):
            return 1.0  # Match sempurna
        else:
            return 0.0  # Tim tidak cocok → JANGAN match

    # Jika salah satu punya tim dan yang lain tidak → tidak cocok
    if e1_has_teams != e2_has_teams:
        return 0.0

    # Keduanya non-matchup (F1, Tennis, MotoGP dll) → pakai token similarity
    score = similarity_score(e1["_match_key"], e2["_match_key"])

    # Jika league cocok, beri bonus besar (membantu matching event seperti "Tennis: Miami")
    e1_league = (e1.get("league") or "").lower()
    e2_league = (e2.get("league") or "").lower()
    if e1_league and e2_league and (e1_league == e2_league or e1_league in e2_league or e2_league in e1_league):
        score += 0.3

    # Bonus jika waktu dekat
    try:
        t1 = datetime.strptime(e1["time_utc"], "%H:%M")
        t2 = datetime.strptime(e2["time_utc"], "%H:%M")
        diff = abs((t1 - t2).total_seconds()) / 60
        if diff <= 15:
            score += 0.15
        elif diff <= 60:
            score += 0.05
    except (ValueError, KeyError):
        pass

    return score


def extract_league_from_title(title: str) -> tuple[str | None, str]:
    """Ekstrak league dari prefix title sumber 2."""
    for prefix, league_name in KNOWN_LEAGUE_PREFIXES:
        if title.startswith(prefix + ":"):
            rest = title[len(prefix) + 1:].strip()
            return league_name, rest
    return None, title


# ═══════════════════════════════════════════════
# PARSER SUMBER 1 — cartelive.club
# ═══════════════════════════════════════════════

def parse_source1(schedule_text: str, channel_map: dict) -> list[dict]:
    """
    Parse jadwal sumber 1.
    Format: DD-MM-YYYY (HH:MM) League : Team A - Team B  (CH##xx) (CH##yy) ...
    Waktu = UTC+1 (CET/Paris)
    """
    events = []
    # Pattern utama
    pattern = re.compile(
        r'^(\d{2}-\d{2}-\d{4})\s+\((\d{2}:\d{2})\)\s+(.+?)\s*:\s*(.*?)\s*((?:\(CH\d+\w*\)\s*)+)$'
    )
    ch_pattern = re.compile(r'\(CH(\d+)(\w*)\)')

    for line in schedule_text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = pattern.match(line)
        if not m:
            continue

        date_str, time_utc1, league_raw, teams_raw, channels_raw = m.groups()

        # Parse tanggal
        try:
            date_obj = datetime.strptime(date_str, "%d-%m-%Y")
        except ValueError:
            continue

        # Normalisasi league
        league = apply_translation(league_raw.strip(), "leagues")

        # Parse nama tim
        teams_raw = teams_raw.strip()
        if not teams_raw or teams_raw == '-' or teams_raw.endswith(' -'):
            # Event tanpa matchup (F1, MotoGP, Tennis, dll.)
            event_name = teams_raw.rstrip(' -').strip() if teams_raw else ""
            
            if event_name.lower() in skip_events:
                continue

            # Cek translasi
            translated = apply_translation(event_name, "titles")
            if translated and translated != event_name:
                title = translated
            elif event_name:
                title = f"{league}: {event_name}"
            else:
                continue
            team_home = None
            team_away = None
        elif ' - ' in teams_raw:
            parts = teams_raw.split(' - ', 1)
            team_home = apply_translation(parts[0].strip(), "teams") or None
            team_away = apply_translation(parts[1].strip(), "teams") or None
            title = f"{team_home} vs {team_away}" if team_home and team_away else teams_raw
        else:
            team_home = apply_translation(teams_raw, "teams")
            team_away = None
            title = teams_raw

        # Hitung waktu UTC+0 dengan dukungan Daylight Saving Time untuk Paris/CET
        dt_ceu = datetime.strptime(
            f"{date_obj.strftime('%Y-%m-%d')}T{time_utc1}", "%Y-%m-%dT%H:%M"
        )
        offset_hours = get_eu_dst_offset(dt_ceu, "CEU")
        dt_utc0 = dt_ceu - timedelta(hours=offset_hours)

        # Parse channel
        streams = []
        for ch_match in ch_pattern.finditer(channels_raw):
            ch_num = int(ch_match.group(1))
            ch_suffix = ch_match.group(2).lower()  # "fr", "es", "us", etc.

            # Ambil info channel dari map
            ch_info = channel_map.get(ch_num, {"name": f"CH{ch_num}", "lang": "unknown"})

            # Tentukan bahasa: prioritaskan suffix, fallback ke channel map
            lang = LANG_SUFFIX_MAP.get(ch_suffix, ch_info.get("lang", "unknown"))

            streams.append({
                "channel_id": f"CH{ch_num}",
                "channel_name": ch_info["name"],
                "language": lang,
                "url": make_cartelive_url(ch_num),
                "source": "cartelive",
            })

        events.append({
            "date": dt_utc0.strftime("%Y-%m-%d"),
            "time_utc": dt_utc0.strftime("%H:%M"),
            "time_utc1": time_utc1,
            "league": league,
            "title": title,
            "team_home": team_home,
            "team_away": team_away,
            "streams": streams,
            "_source": "cartelive",
            "_match_key": normalize_text(title),
        })

    return events


# ═══════════════════════════════════════════════
# PARSER SUMBER 2 — sportsonline.st
# ═══════════════════════════════════════════════

def determine_dates_from_today() -> dict[str, str]:
    """
    Tentukan mapping hari → tanggal berdasarkan tanggal hari ini.
    Cari hari Jumat terdekat (sebelum atau hari ini), lalu map semua 7 hari (Fri-Thu).
    """
    today = datetime.now()
    weekday = today.weekday()  # 0=Mon, 4=Fri

    # Cari Jumat terdekat (<=0 hari yang lalu, atau sampai 6 hari yang lalu)
    days_since_friday = (weekday - 4) % 7
    friday = today - timedelta(days=days_since_friday)

    day_names = ["FRIDAY", "SATURDAY", "SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY"]
    return {
        day_names[i]: (friday + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(7)
    }


# Mapping bahasa channel sumber 2
S2_LANG_MAP = {
    "ENGLISH": "en",
    "GERMAN": "de",
    "DUTCH": "nl",
    "SPANISH": "es",
    "ITALIAN": "it",
    "FRENCH": "fr",
    "BRAZILIAN": "pt-BR",
    "ARABIC": "ar",
    "GREEK": "el",
    "ESTONIAN": "et",
    "UNDEFINED": "unknown",
    "PORTUGUESE": "pt",
    "BELGIAN": "fr",
    "POLISH": "pl",
    "BULGARIAN": "bg",
    "DANISH": "da",
}


def parse_source2(raw: str) -> list[dict]:
    """
    Parse format sumber 2.
    Waktu = UTC+0
    """
    events_by_key: dict[tuple, dict] = {}
    day_dates = determine_dates_from_today()

    current_day = None
    current_date = None
    channel_langs: dict[str, str] = {}  # "hd1" → "en"
    last_time_h = None
    crossed_midnight = False

    event_pat = re.compile(r'^(\d{2}:\d{2})\s+(.+?)\s*\|\s*(https?://\S+)\s*$')
    ch_header_pat = re.compile(r'^(HD\d+|BR\d+)\s+(.+)$', re.IGNORECASE)
    day_pat = re.compile(r'^(FRIDAY|SATURDAY|SUNDAY|MONDAY|TUESDAY|WEDNESDAY|THURSDAY)$', re.IGNORECASE)

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue

        # Header hari
        dm = day_pat.match(line)
        if dm:
            current_day = dm.group(1).upper()
            current_date = day_dates.get(current_day)
            channel_langs = {}
            last_time_h = None
            crossed_midnight = False
            continue

        # Header channel
        cm = ch_header_pat.match(line)
        if cm and current_date:
            ch_key = cm.group(1).lower()
            lang_str = cm.group(2).strip()
            # Ambil bahasa pertama jika ada "&"
            primary_lang = lang_str.split("&")[0].strip().upper()
            channel_langs[ch_key] = S2_LANG_MAP.get(primary_lang, "unknown")
            continue

        # Baris event
        em = event_pat.match(line)
        if em and current_date:
            time_str = em.group(1)
            title_raw = em.group(2).strip()
            url = em.group(3).strip()

            # Deteksi midnight crossing
            time_h = int(time_str.split(':')[0])
            if last_time_h is not None and last_time_h >= 18 and time_h < 12:
                crossed_midnight = True
            last_time_h = time_h

            # Tentukan tanggal (jika sudah lewat midnight, tambah 1 hari)
            if crossed_midnight:
                actual_date = (
                    datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
            else:
                actual_date = current_date

            # Ekstrak info channel dari URL
            ch_url_match = re.search(r'/channels/(\w+)/(\w+)\.php', url)
            if ch_url_match:
                ch_folder = ch_url_match.group(1)  # "hd", "bra", "pt"
                ch_file = ch_url_match.group(2).lower()  # "hd1", "br2", "sporttv1"

                # Tentukan bahasa
                if ch_folder == "pt":
                    lang = "pt"
                elif ch_folder == "bra":
                    lang = "pt-BR"
                else:
                    lang = channel_langs.get(ch_file, "unknown")

                channel_display = ch_file.upper()
                
                # Format URL baru: multi.govoet.cc/?ss={id}
                final_url = f"https://multi.govoet.cc/?ss={ch_file}"
            else:
                lang = "unknown"
                channel_display = "unknown"
                final_url = url

            # Ekstrak league dari title
            detected_league, clean_title = extract_league_from_title(title_raw)

            # Parse tim
            team_home = None
            team_away = None
            sep_match = re.search(r'\s+(?:x|vs|@)\s+', clean_title, re.IGNORECASE)
            if sep_match:
                parts = re.split(r'\s+(?:x|vs|@)\s+', clean_title, maxsplit=1, flags=re.IGNORECASE)
                team_home = apply_translation(parts[0], "teams")
                team_away = apply_translation(parts[1], "teams") if len(parts) > 1 else None

            # Normalisasi title jika ada tim, atau translasi judul
            if team_home and team_away:
                title = f"{team_home} vs {team_away}"
            else:
                title = apply_translation(clean_title, "titles")
                
            # Normalisasi league
            detected_league = apply_translation(detected_league, "leagues") if detected_league else None

            # Group key untuk deduplikasi (event yang sama, beda stream)
            norm_title = normalize_text(title)
            group_key = (actual_date, time_str, norm_title)
            
            # Hitung waktu UTC yang benar (memperhatikan DST UK)
            dt_uk = datetime.strptime(f"{actual_date}T{time_str}", "%Y-%m-%dT%H:%M")
            offset_hours = get_eu_dst_offset(dt_uk, "UK")
            dt_utc0 = dt_uk - timedelta(hours=offset_hours)

            stream_entry = {
                "channel_id": channel_display,
                "channel_name": channel_display,
                "language": lang,
                "url": final_url,
                "source": "sportsonline",
            }

            if group_key not in events_by_key:
                time_ceu_str = (dt_utc0 + timedelta(hours=get_eu_dst_offset(dt_utc0, "CEU"))).strftime("%H:%M")
                events_by_key[group_key] = {
                    "date": dt_utc0.strftime("%Y-%m-%d"),
                    "time_utc": dt_utc0.strftime("%H:%M"),
                    "time_utc1": time_ceu_str,
                    "league": detected_league,
                    "title": title,
                    "team_home": team_home,
                    "team_away": team_away,
                    "streams": [],
                    "_source": "sportsonline",
                    "_match_key": norm_title,
                }

            events_by_key[group_key]["streams"].append(stream_entry)

    return list(events_by_key.values())


# ═══════════════════════════════════════════════
# MERGER
# ═══════════════════════════════════════════════

def find_best_match(event: dict, candidates: list[dict], threshold: float = 0.4) -> dict | None:
    """Cari event paling cocok menggunakan logika matching cerdas."""
    best = None
    best_score = 0.0

    for c in candidates:
        score = events_are_matchable(event, c)

        if score > best_score and score >= threshold:
            best_score = score
            best = c

    return best


def merge_streams_by_lang(streams: list[dict]) -> dict:
    """Kelompokkan stream berdasarkan bahasa."""
    by_lang: dict[str, list] = {}
    seen = set()  # Hindari duplikasi URL

    for s in streams:
        lang = s.get("language", "unknown")
        url = s.get("url")
        key = (lang, s.get("channel_name", ""), url or "")
        if key in seen:
            continue
        seen.add(key)

        if lang not in by_lang:
            by_lang[lang] = []
        by_lang[lang].append({
            "channel": s.get("channel_name", s.get("channel_id", "?")),
            "url": url,
            "source": s.get("source"),
        })

    return by_lang


def merge_events(events1: list[dict], events2: list[dict]) -> list[dict]:
    """
    Gabungkan event dari kedua sumber.
    Event yang cocok → stream digabung.
    Event unik → masuk sendiri.
    """
    matched_s2_indices = set()
    merged = []

    for e1 in events1:
        match = find_best_match(e1, events2)

        all_streams = list(e1["streams"])
        sources = ["cartelive"]

        if match:
            all_streams.extend(match["streams"])
            sources.append("sportsonline")
            # Tandai match agar tidak masuk lagi
            for i, e2 in enumerate(events2):
                if e2 is match:
                    matched_s2_indices.add(i)
                    break

        # League: prioritas sumber 1 (lebih lengkap), fallback sumber 2
        league = e1.get("league") or (match.get("league") if match else None)

        title_e1 = f"{e1['team_home']} vs {e1['team_away']}" if (e1.get("team_home") and e1.get("team_away")) else e1["title"]

        merged.append({
            "date": e1["date"],
            "time_utc": e1["time_utc"],
            "time_utc1": e1["time_utc1"],
            "league": league,
            "title": title_e1,
            "team_home": e1.get("team_home"),
            "team_away": e1.get("team_away"),
            "sources": sources,
            "streams_by_language": merge_streams_by_lang(all_streams),
        })

    # Tambah event sumber 2 yang tidak punya pasangan
    for i, e2 in enumerate(events2):
        if i in matched_s2_indices:
            continue

        title_e2 = f"{e2['team_home']} vs {e2['team_away']}" if (e2.get("team_home") and e2.get("team_away")) else e2["title"]

        merged.append({
            "date": e2["date"],
            "time_utc": e2["time_utc"],
            "time_utc1": e2["time_utc1"],
            "league": e2.get("league"),
            "title": title_e2,
            "team_home": e2.get("team_home"),
            "team_away": e2.get("team_away"),
            "sources": ["sportsonline"],
            "streams_by_language": merge_streams_by_lang(e2["streams"]),
        })

    import calendar

    # Tambahkan startTimestamp ke semua event
    for e in merged:
        dt_str = f"{e['date']} {e['time_utc']}"
        try:
            dt_obj = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
            # calendar.timegm menganggap tuple sebagai UTC dan mengembalikan unix timestamp
            e["startTimestamp"] = int(calendar.timegm(dt_obj.timetuple()))
        except ValueError:
            e["startTimestamp"] = 0

    # Sort by startTimestamp
    merged.sort(key=lambda x: x["startTimestamp"])
    return merged


# ═══════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  📡  SPORTS SCHEDULE MERGER")
    print("=" * 60)

    # ── Fetch sumber 1 ──
    print(f"\n🔽 Fetching sumber 1: {SOURCE1_URL}")
    try:
        html = fetch_url(SOURCE1_URL)
        print(f"   ✅ Berhasil ({len(html):,} bytes)")
    except Exception as e:
        print(f"   ❌ Gagal: {e}")
        sys.exit(1)

    # Ekstrak textarea (pertama = jadwal, kedua = daftar channel)
    textareas = extract_textareas(html)
    if len(textareas) < 2:
        print(f"   ❌ Hanya ditemukan {len(textareas)} textarea (butuh 2)")
        sys.exit(1)

    schedule_text = textareas[0]
    channel_list_text = textareas[1]
    print(f"   📋 Jadwal: {len(schedule_text.splitlines())} baris")
    print(f"   📋 Daftar channel: {len(channel_list_text.splitlines())} baris")

    # Parse channel map
    channel_map = parse_channel_list(channel_list_text)
    print(f"   📺 {len(channel_map)} channel ter-mapping")

    # Parse jadwal sumber 1
    print("\n🔍 Parsing sumber 1 (cartelive)...")
    events1 = parse_source1(schedule_text, channel_map)
    print(f"   → {len(events1)} event ditemukan")

    # ── Fetch sumber 2 ──
    print(f"\n🔽 Fetching sumber 2: {SOURCE2_URL}")
    try:
        raw2 = fetch_url(SOURCE2_URL)
        print(f"   ✅ Berhasil ({len(raw2):,} bytes)")
    except Exception as e:
        print(f"   ❌ Gagal: {e}")
        sys.exit(1)

    # Parse sumber 2
    print("\n🔍 Parsing sumber 2 (sportsonline)...")
    events2 = parse_source2(raw2)
    print(f"   → {len(events2)} event ditemukan")

    # ── Merge ──
    print("\n🔗 Merging events...")
    merged = merge_events(events1, events2)
    print(f"   → {len(merged)} event total")

    # Statistik
    both = sum(1 for e in merged if len(e["sources"]) > 1)
    only1 = sum(1 for e in merged if e["sources"] == ["cartelive"])
    only2 = sum(1 for e in merged if e["sources"] == ["sportsonline"])

    print(f"\n📊 Statistik:")
    print(f"   ✅ Matched (kedua sumber) : {both}")
    print(f"   🔵 Hanya cartelive        : {only1}")
    print(f"   🟢 Hanya sportsonline     : {only2}")

    # ── Output ──
    output = {
        "generated_at": datetime.now().isoformat(),
        "sources": [
            {"name": "cartelive", "url": SOURCE1_URL, "timezone": "UTC+1"},
            {"name": "sportsonline", "url": SOURCE2_URL, "timezone": "UTC+0"},
        ],
        "timezone_output": "UTC+0",
        "total_events": len(merged),
        "events": merged,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Output disimpan: {OUTPUT_FILE}")

    # Preview
    print(f"\n{'─' * 60}")
    print("📋 Preview (5 event pertama):")
    print(f"{'─' * 60}")
    for e in merged[:5]:
        src_icons = {"cartelive": "🔵", "sportsonline": "🟢"}
        src_str = " ".join(src_icons.get(s, "⚪") for s in e["sources"])
        langs = list(e["streams_by_language"].keys())
        stream_count = sum(len(v) for v in e["streams_by_language"].values())
        print(f"  {src_str} [{e['date']} {e['time_utc']} UTC] {e['title']}")
        print(f"       League: {e['league'] or '—'}")
        print(f"       Streams: {stream_count} ({', '.join(langs)})")
        print()


if __name__ == "__main__":
    main()
