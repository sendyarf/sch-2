#!/usr/bin/env python3
import json
import re
import sys
import os
import io
import urllib.request
import urllib.error
from datetime import datetime

# Fix encoding untuk Windows PowerShell
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

SOURCE1_URL = "https://18zone.click/vip3.php"
SOURCE2_URL = "https://sportsonline.pk/prog.txt"
SOURCE3_URL = "https://v2-gvtsch.pages.dev/manual_sch.json"
DICT_FILE = "dictionary.json"

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
# We try the model deepseek-v4-flash, and if it fails/errors, we'll try deepseek-chat.
MODEL_NAME = "deepseek-v4-flash"

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

def fetch_url(url: str, retries: int = 3, timeout: int = 30) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    req = urllib.request.Request(url, headers=headers)
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            if attempt < retries:
                print(f"      [!] Gagal fetch {url} (percobaan {attempt}/{retries}): {e}. Retrying...")
                import time
                time.sleep(2)
            else:
                raise e

from html.parser import HTMLParser

class TextareaExtractor(HTMLParser):
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
    parser = TextareaExtractor()
    parser.feed(html)
    return parser.textareas

def extract_league_from_title(title: str) -> tuple[str | None, str]:
    for prefix, league_name in KNOWN_LEAGUE_PREFIXES:
        if title.startswith(prefix + ":"):
            rest = title[len(prefix) + 1:].strip()
            return league_name, rest
    return None, title

def call_deepseek_api(prompt: str) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
    }
    
    # Try models
    models_to_try = [MODEL_NAME, "deepseek-chat"]
    last_err = None
    
    for model in models_to_try:
        data = {
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a professional sports data translation and normalization helper. Output ONLY valid JSON."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.1
        }
        
        req = urllib.request.Request(
            DEEPSEEK_API_URL, 
            data=json.dumps(data).encode("utf-8"), 
            headers=headers,
            method="POST"
        )
        
        print(f"   🤖 Mengirim request ke DeepSeek menggunakan model: '{model}'...")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                res_body = json.loads(resp.read().decode("utf-8"))
                content = res_body["choices"][0]["message"]["content"]
                return json.loads(content)
        except Exception as e:
            print(f"   ⚠️ Gagal dengan model '{model}': {e}")
            last_err = e
            
    raise last_err

def main():
    print("=" * 60)
    print("  🤖 DEEPSEEK SPORTS DICTIONARY UPDATER")
    print("=" * 60)

    # 1. Load existing dictionary
    if os.path.exists(DICT_FILE):
        with open(DICT_FILE, "r", encoding="utf-8") as f:
            dictionary = json.load(f)
    else:
        dictionary = {"teams": {}, "leagues": {}, "titles": {}, "skip_events": []}

    # Helper lookup for existing aliases
    existing_aliases = {
        "teams": set(),
        "leagues": set(),
        "titles": set()
    }
    for category in ["teams", "leagues", "titles"]:
        for std_name, aliases in dictionary.get(category, {}).items():
            existing_aliases[category].add(std_name.lower())
            for alias in aliases:
                existing_aliases[category].add(alias.lower())

    # 2. Fetch Source 1 and Source 2
    print("\n🔽 Fetching Source 1...")
    html = fetch_url(SOURCE1_URL)
    textareas = extract_textareas(html)
    if len(textareas) < 1:
        print("❌ Tidak dapat menemukan textarea di Source 1!")
        sys.exit(1)
    
    schedule_text = textareas[0]
    
    print("🔽 Fetching Source 2...")
    raw2 = fetch_url(SOURCE2_URL)

    print("🔽 Fetching Source 3...")
    raw3 = fetch_url(SOURCE3_URL)

    # 3. Parse Source 1 (French)
    s1_leagues = set()
    s1_teams = set()
    s1_titles = set()

    pattern1 = re.compile(
        r'^(\d{2}-\d{2}-\d{4})\s+\((\d{2}:\d{2})\)\s+(.+?)\s*:\s*(.*?)\s*((?:\(CH\d+\w*\)\s*)+)$'
    )
    for line in schedule_text.splitlines():
        line = line.strip()
        m = pattern1.match(line)
        if not m:
            continue
        _, _, league_raw, teams_raw, _ = m.groups()
        
        league = league_raw.strip()
        if league:
            s1_leagues.add(league)

        teams_raw = teams_raw.strip()
        if not teams_raw or teams_raw == '-' or teams_raw.endswith(' -'):
            event_name = teams_raw.rstrip(' -').strip() if teams_raw else ""
            if event_name:
                s1_titles.add(event_name)
        elif ' - ' in teams_raw:
            parts = teams_raw.split(' - ', 1)
            if parts[0].strip():
                s1_teams.add(parts[0].strip())
            if parts[1].strip():
                s1_teams.add(parts[1].strip())
        else:
            s1_teams.add(teams_raw)

    # 4. Parse Source 2 (English / Standard)
    s2_leagues = set()
    s2_teams = set()
    s2_titles = set()

    event_pat = re.compile(r'^(\d{2}:\d{2})\s+(.+?)\s*\|\s*(https?://\S+)\s*$')
    day_pat = re.compile(r'^(FRIDAY|SATURDAY|SUNDAY|MONDAY|TUESDAY|WEDNESDAY|THURSDAY)$', re.IGNORECASE)
    
    for line in raw2.splitlines():
        line = line.strip()
        if not line or day_pat.match(line):
            continue
        em = event_pat.match(line)
        if em:
            title_raw = em.group(2).strip()
            detected_league, clean_title = extract_league_from_title(title_raw)
            if detected_league:
                s2_leagues.add(detected_league)
            
            sep_match = re.search(r'\s+(?:x|vs|@)\s+', clean_title, re.IGNORECASE)
            if sep_match:
                parts = re.split(r'\s+(?:x|vs|@)\s+', clean_title, maxsplit=1, flags=re.IGNORECASE)
                s2_teams.add(parts[0].strip())
                if len(parts) > 1:
                    s2_teams.add(parts[1].strip())
            else:
                s2_titles.add(clean_title)

    # 4.5. Parse Source 3 (Manual JSON)
    # Memasukkan item dari Source 3 agar AI bisa mencocokkan variasi kata dari ketiga sumber.
    try:
        data3 = json.loads(raw3)
        for m in data3:
            league = m.get("league")
            if league:
                s1_leagues.add(league)
                s2_leagues.add(league)
            
            t1 = m.get("team1", {}).get("name")
            t2 = m.get("team2", {}).get("name")
            if t1:
                s1_teams.add(t1)
                s2_teams.add(t1)
            if t2:
                s1_teams.add(t2)
                s2_teams.add(t2)
    except Exception as e:
        print(f"   ⚠️ Gagal parse Source 3 untuk dictionary: {e}")

    # 5. Filter out already translated/mapped items
    unmatched_leagues = sorted([l for l in s1_leagues if l.lower() not in existing_aliases["leagues"]])
    unmatched_teams = sorted([t for t in s1_teams if t.lower() not in existing_aliases["teams"]])
    unmatched_titles = sorted([t for t in s1_titles if t.lower() not in existing_aliases["titles"]])

    candidate_leagues = sorted(list(s2_leagues))
    candidate_teams = sorted(list(s2_teams))
    candidate_titles = sorted(list(s2_titles))

    print(f"\n📊 Analisis Unmatched:")
    print(f"   - Leagues: {len(unmatched_leagues)} dari {len(s1_leagues)} total")
    print(f"   - Teams:   {len(unmatched_teams)} dari {len(s1_teams)} total")
    print(f"   - Titles:  {len(unmatched_titles)} dari {len(s1_titles)} total")

    if not unmatched_leagues and not unmatched_teams and not unmatched_titles:
        print("\n✅ Semua entri dari Source 1 sudah ter-mapping atau memiliki translasi. Tidak perlu update.")
        return

    # 6. Construct prompt for DeepSeek
    prompt = f"""
We need to translate and match French terms from Source 1 to English/standard terms from Source 2.
Here is the data:

=== SOURCE 1 UNMATCHED LEAGUES (French) ===
{json.dumps(unmatched_leagues, indent=2)}

=== SOURCE 2 CANDIDATE LEAGUES (English/Standard) ===
{json.dumps(candidate_leagues, indent=2)}

=== SOURCE 1 UNMATCHED TEAMS (French) ===
{json.dumps(unmatched_teams, indent=2)}

=== SOURCE 2 CANDIDATE TEAMS (English/Standard) ===
{json.dumps(candidate_teams, indent=2)}

=== SOURCE 1 UNMATCHED TITLES (French) ===
{json.dumps(unmatched_titles, indent=2)}

=== SOURCE 2 CANDIDATE TITLES (English/Standard) ===
{json.dumps(candidate_titles, indent=2)}

Please return a JSON object mapping the correct standard/English name (usually from Source 2 candidate list, or translated directly to English if not present in Source 2) as the key, and a list containing the French/raw name as the value.
If you map to an existing standard term or translate it, use proper English casing (e.g., "South Korea" instead of "Corée du Sud").
Only include matching pairs where the translation is confident.

Example output format:
{{
  "teams": {{
    "South Korea": ["Corée du Sud"],
    "Czech Republic": ["République Tchèque"]
  }},
  "leagues": {{
    "World Cup": ["Coupe Du Monde"]
  }},
  "titles": {{}}
}}

Output ONLY the raw JSON object. Do not include markdown code block or any explanation text.
"""

    # 7. Call DeepSeek
    try:
        translations = call_deepseek_api(prompt)
        print("   ✅ Berhasil mendapatkan response dari DeepSeek.")
    except Exception as e:
        print(f"❌ Gagal memanggil DeepSeek API: {e}")
        sys.exit(1)

    # 8. Merge translations into dictionary.json
    updated_counts = {"teams": 0, "leagues": 0, "titles": 0}
    
    for category in ["teams", "leagues", "titles"]:
        category_data = translations.get(category, {})
        for std_name, new_aliases in category_data.items():
            if not isinstance(new_aliases, list):
                new_aliases = [new_aliases]
            
            # Find standard name in existing dict (case-insensitive)
            matched_key = None
            for existing_key in dictionary[category].keys():
                if existing_key.lower() == std_name.lower():
                    matched_key = existing_key
                    break
            
            if matched_key is None:
                # Add new standard name key
                dictionary[category][std_name] = []
                matched_key = std_name

            # Add aliases to standard name list
            for alias in new_aliases:
                alias_clean = alias.strip()
                # Check if it already exists in the aliases list (case-insensitive)
                existing_lower = [a.lower() for a in dictionary[category][matched_key]]
                if alias_clean.lower() not in existing_lower:
                    dictionary[category][matched_key].append(alias_clean)
                    updated_counts[category] += 1
                
                # Also ensure lowercase or exact name is in the list
                if alias_clean.lower() != alias_clean and alias_clean.lower() not in [a.lower() for a in dictionary[category][matched_key]]:
                    dictionary[category][matched_key].append(alias_clean.lower())

    # 9. Save dictionary.json
    with open(DICT_FILE, "w", encoding="utf-8") as f:
        json.dump(dictionary, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Dictionary updated successfully!")
    print(f"   - Teams added:   {updated_counts['teams']}")
    print(f"   - Leagues added: {updated_counts['leagues']}")
    print(f"   - Titles added:  {updated_counts['titles']}")

if __name__ == "__main__":
    main()
