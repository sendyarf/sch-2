import json
from collections import defaultdict
from itertools import combinations
import re

with open('merged_schedule.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

events = data['events']
duplicates = []

def similarity(t1, t2):
    if not t1 or not t2: return 0
    t1 = set(re.sub(r'[^a-z0-9]', ' ', t1.lower()).split())
    t2 = set(re.sub(r'[^a-z0-9]', ' ', t2.lower()).split())
    if not t1 or not t2: return 0
    return len(t1 & t2) / len(t1 | t2)

# Check by date
events_by_date = defaultdict(list)
for e in events:
    events_by_date[e['date']].append(e)

for date, daily_events in events_by_date.items():
    for e1, e2 in combinations(daily_events, 2):
        is_dupe = False
        
        # Check teams
        if e1.get('team_home') and e2.get('team_home') and e1.get('team_away') and e2.get('team_away'):
            # Using basic string match since we now have dictionary translation, 
            # but maybe one wasn't translated
            home1, away1 = e1['team_home'].lower(), e1['team_away'].lower()
            home2, away2 = e2['team_home'].lower(), e2['team_away'].lower()
            
            if (home1 == home2 and away1 == away2) or (home1 == away2 and away1 == home2):
                is_dupe = True
            elif similarity(home1, home2) > 0.5 and similarity(away1, away2) > 0.5:
                is_dupe = True
        else:
            # Check title similarity for non-team events
            if similarity(e1['title'], e2['title']) > 0.6:
                is_dupe = True
                
        if is_dupe:
            # Check if time diff is less than 3 hours
            diff = abs(e1['startTimestamp'] - e2['startTimestamp'])
            if diff <= 10800: # 3 hours
                duplicates.append((e1, e2))

print(f"Ditemukan {len(duplicates)} potensi duplikasi:")
for e1, e2 in duplicates:
    print("-" * 50)
    print(f"Event 1 [{', '.join(e1['sources'])}]: {e1['time_utc']} - {e1['title']} (League: {e1.get('league')})")
    print(f"Event 2 [{', '.join(e2['sources'])}]: {e2['time_utc']} - {e2['title']} (League: {e2.get('league')})")

