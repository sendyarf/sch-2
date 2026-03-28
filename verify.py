import json

with open("merged_schedule.json", "r", encoding="utf-8") as f:
    d = json.load(f)

both = sum(1 for e in d["events"] if len(e["sources"]) > 1)
s1 = sum(1 for e in d["events"] if e["sources"] == ["cartelive"])
s2 = sum(1 for e in d["events"] if e["sources"] == ["sportsonline"])

lines = []
lines.append(f"Total: {d['total_events']}")
lines.append(f"Both sources: {both}")
lines.append(f"Only cartelive: {s1}")
lines.append(f"Only sportsonline: {s2}")
lines.append("")
lines.append("=== Matched events ===")
for e in d["events"]:
    if len(e["sources"]) > 1:
        langs = list(e["streams_by_language"].keys())
        n = sum(len(v) for v in e["streams_by_language"].values())
        lines.append(f"  {e['date']} {e['time_utc']} | {e['title'][:60]} | {n} streams ({', '.join(langs)})")

lines.append("")
lines.append("=== Cartelive only ===")
for e in d["events"]:
    if e["sources"] == ["cartelive"]:
        lines.append(f"  {e['date']} {e['time_utc']} | {e['title'][:60]} | {e['league']}")

lines.append("")
lines.append("=== Sportsonline only (first 15) ===")
count = 0
for e in d["events"]:
    if e["sources"] == ["sportsonline"]:
        lines.append(f"  {e['date']} {e['time_utc']} | {e['title'][:60]} | {e['league']}")
        count += 1
        if count >= 15:
            lines.append("  ... (truncated)")
            break

with open("stats.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print("Written to stats.txt")
