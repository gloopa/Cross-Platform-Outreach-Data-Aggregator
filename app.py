import sqlite3
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

class outreach:
    def __init__( self, db_path:str = "contacts.db"):
        self.db_path= db_path
        self.conn = None
        self.init_db()

    def init_db(self): #creating sqlite datbase withevents table
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            name TEXT,
            platform TEXT NOT NULL,
            type TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            reply_text TEXT,
            campaign_name TEXT
        )
        """)
        self.conn.commit()

    def heyreach(self, event:Dict[str, Any]) -> Dict[str, Any]:
        return {
            'id': f"heyreach_{event['id']}",
            'email': event['prospect_email'].lower().strip(),
            'name': event.get('prospect_name', ''),
            'platform': 'heyreach',
            'type': event['type'],
            'timestamp': event['at'],
            'reply_text': event.get('text', ''),
            'campaign_name': event.get('campaign', '')
            
        }
    def salesforge(self, event:Dict[str, Any]) -> Dict[str, Any]:
        return {
            'id': f"salesforge_{event['id']}",
            'email': event['email'].lower().strip(),
            'name': event.get('full_name', ''),
            'platform': 'salesforge',
            'type': event['type'],
            'timestamp': event['at'],
            'reply_text': event.get('text', ''),
            'campaign_name': event.get('sequence', '')
        }
    def instantly(self, event:Dict[str, Any]) -> Dict[str, Any]:
        contact = event.get('contact', {})
        return {
            'id': f"instantly_{event['id']}",
            'email': event['contact']['email'].lower().strip(),
            'name': event['contact']['name'],
            'platform': 'instantly',
            'type': event['type'],
            'timestamp': event['timestamp'],
            'reply_text': event.get('body', ''),
            'campaign_name': event.get('campaign_name', '')
        }
    
    def load(self, filepath:str) -> List[Dict[str, Any]]:
        events = []
        with open(filepath, 'r') as file:
            for line in file:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
        return events
    
    def processevent(self, source:str, filepath:str):
        print(f"Processing {source} events from {filepath}")
        events = self.load(filepath)
        normalizers = {
            'heyreach': self.heyreach,
            'salesforge': self.salesforge,
            'instantly': self.instantly
        }
        normalizer = normalizers[source]
        normalized_events = [normalizer(event) for event in events]
        cursor = self.conn.cursor()
        for event in normalized_events:
            cursor.execute("""
            INSERT OR IGNORE INTO events (id, email, name, platform, type, timestamp, reply_text, campaign_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (event['id'], event['email'], event['name'], event['platform'], event['type'], event['timestamp'], event['reply_text'], event['campaign_name']))
        self.conn.commit()
        print(f"Processed {len(normalized_events)} events from {source}") 

    def generatecontacts(self) -> List[Dict[str, Any]]:
        print("Generating contacts from events")
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT email FROM events ORDER BY email")
        emails = [row[0] for row in cursor.fetchall()]
        contacts = []

        for email in emails:
            cursor.execute("""
                SELECT name, platform, type, timestamp, reply_text, campaign_name
                FROM events
                WHERE email = ?
                ORDER BY timestamp ASC
            """, (email,))
            events = cursor.fetchall()
            sources = sorted(list(set(event[1] for event in events)))
            latestevent = max(events, key=lambda x: x[3])
            latestname = latestevent[0] #recent name from recent event
            sequence_name = latestevent[5]
            sendevents = [event for event in events if event[2] == 'send']
            firstoutreach = min(sendevents, key=lambda x: x[3])[3] if sendevents else events[0][3] #earliest send event
            lasttouch = latestevent[3] #latest event
            replyevents = [event for event in events if event[2] == 'reply']
            replied = len(replyevents) > 0
            if replied:
                lastreply = max(replyevents, key=lambda x: x[3])
                lastreplyat = lastreply[3]
                lastreplytext = lastreply[4]
            else:
                lastreplyat = ''
                lastreplytext = ''

            sourceeventcount = len(events)
            updatedat = lasttouch
            contact = {
                'email': email,
                'full_name': latestname,
                'sources': json.dumps(sources, separators=(',', ':')),
                'sequence_name': sequence_name,
                'first_outreach_at': firstoutreach,
                'last_touch_at': lasttouch,
                'replied': 'true' if replied else 'false',
                'last_reply_at': lastreplyat,
                'last_reply_text': lastreplytext,
                'source_event_count': sourceeventcount,
                'updated_at': updatedat
            }
            contacts.append(contact)
        contacts.sort(key=lambda c: (c['email'].lower(), c['first_outreach_at']))
        print(f"Generated {len(contacts)} contacts")
        return contacts
    
    def export(self, contacts:List[Dict[str, Any]], output_path:str = "unified.csv"):
        print(f"Exporting {len(contacts)} contacts to {output_path}")
        fieldnames = ['email', 'full_name', 'sources', 'sequence_name', 'first_outreach_at', 'last_touch_at', 'replied', 'last_reply_at', 'last_reply_text', 'source_event_count', 'updated_at']
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(contacts)
        print(f"Exported to {output_path}")

    def run(self):
        print("Running the outreach platform")
        mock_dir = Path(__file__).parent / 'mock'
        self.processevent('heyreach', mock_dir / 'heyreach.jsonl')
        self.processevent('salesforge', mock_dir / 'salesforge.jsonl')
        self.processevent('instantly', mock_dir / 'instantly.jsonl')
        contacts = self.generatecontacts()
        self.export(contacts)
        print("Done")
    
    def close(self):
        if self.conn:
            self.conn.close()

def main():
    unified = outreach()
    try:
        unified.run()
    finally:
        unified.close()

if __name__ == "__main__":
    main()
