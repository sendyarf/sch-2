import json

with open("merged_schedule.json", "r", encoding="utf-8") as f:
    d = json.load(f)

# Find Morocco events
for e in d["events"]:
    if "morocco" in e["title"].lower() or "ecuador" in e["title"].lower():
        print(f"Title: {e['title']}")
        print(f"  Date: {e['date']}, Time UTC: {e['time_utc']}, Time UTC+1: {e['time_utc1']}")
        print(f"  Home: {e.get('team_home')}, Away: {e.get('team_away')}")
        print(f"  Sources: {e['sources']}")
        print(f"  League: {e['league']}")
        print()

# Find F1 events
print("--- F1 events ---")
for e in d["events"]:
    if "formula" in e["title"].lower() or "f1" in e["title"].lower():
        print(f"Title: {e['title']}")
        print(f"  Date: {e['date']}, Time UTC: {e['time_utc']}, Time UTC+1: {e['time_utc1']}")
        print(f"  Sources: {e['sources']}")
        print()

# Find Tennis events
print("--- Tennis events ---")
for e in d["events"]:
    if "tennis" in e["title"].lower() or "miami" in e["title"].lower():
        print(f"Title: {e['title']}")
        print(f"  Date: {e['date']}, Time UTC: {e['time_utc']}, Time UTC+1: {e['time_utc1']}")
        print(f"  Sources: {e['sources']}")
        print()
