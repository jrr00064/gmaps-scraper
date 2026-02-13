"""
SQLite Database Handler (Alternative to DuckDB)
Streaming storage for Google Maps data
"""

import sqlite3
import json
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import os


@dataclass
class BusinessRecord:
    """Business record structure"""
    name: str
    phone: str
    address: str
    website: str
    rating: float
    reviews_count: int
    category: str
    hours: str  # JSON string
    latitude: float
    longitude: float
    place_id: str
    scraped_at: str  # ISO timestamp


class SQLiteStorage:
    """
    SQLite storage manager (fallback for DuckDB)
    Performance is still excellent for this use case
    """
    
    def __init__(self, db_path: str = "/tmp/gmaps-scraper/data/businesses.db"):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor = None
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    def __enter__(self):
        """Open connection"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection"""
        if self.conn:
            self.conn.commit()
            self.conn.close()
    
    def _create_tables(self):
        """Create tables if they don't exist"""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS businesses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT,
                address TEXT,
                website TEXT,
                rating REAL DEFAULT 0.0,
                reviews_count INTEGER DEFAULT 0,
                category TEXT,
                hours TEXT,
                latitude REAL,
                longitude REAL,
                place_id TEXT UNIQUE,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_latitude ON businesses(latitude)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_longitude ON businesses(longitude)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_category ON businesses(category)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_place_id ON businesses(place_id)")
        
        self.conn.commit()
    
    def insert_business(self, record: BusinessRecord) -> bool:
        """Insert single business record"""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO businesses 
                (name, phone, address, website, rating, reviews_count, category, 
                 hours, latitude, longitude, place_id, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                record.name, record.phone, record.address, record.website,
                record.rating, record.reviews_count, record.category,
                record.hours, record.latitude, record.longitude,
                record.place_id, record.scraped_at
            ])
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting record: {e}")
            return False
    
    def insert_many(self, records: List[BusinessRecord], batch_size: int = 100):
        """
        Batch insert records
        """
        if not records:
            return 0
        
        inserted = 0
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                data = [
                    (r.name, r.phone, r.address, r.website, r.rating, 
                     r.reviews_count, r.category, r.hours, r.latitude,
                     r.longitude, r.place_id, r.scraped_at)
                    for r in batch
                ]
                
                self.cursor.executemany("""
                    INSERT OR REPLACE INTO businesses 
                    (name, phone, address, website, rating, reviews_count, category,
                     hours, latitude, longitude, place_id, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, data)
                
                inserted += len(batch)
            
            self.conn.commit()
            return inserted
            
        except Exception as e:
            print(f"Error batch inserting: {e}")
            return inserted
    
    def get_count(self) -> int:
        """Get total record count"""
        result = self.cursor.execute("SELECT COUNT(*) FROM businesses").fetchone()
        return result[0] if result else 0
    
    def export_to_csv(self, filepath: str):
        """Export data to CSV"""
        import csv
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM businesses")
        rows = cursor.fetchall()
        
        headers = [description[0] for description in cursor.description]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        
        print(f"Exported {len(rows)} records to {filepath}")
    
    def export_to_json(self, filepath: str):
        """Export data to JSON"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM businesses")
        rows = cursor.fetchall()
        headers = [description[0] for description in cursor.description]
        
        data = [dict(zip(headers, row)) for row in rows]
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported {len(rows)} records to {filepath}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        stats = {}
        
        # Total count
        stats["total_businesses"] = self.get_count()
        
        # Top categories
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT category, COUNT(*) as count 
            FROM businesses 
            WHERE category != '' 
            GROUP BY category 
            ORDER BY count DESC 
            LIMIT 10
        """)
        stats["top_categories"] = [
            {"category": row[0], "count": row[1]} for row in cursor.fetchall()
        ]
        
        # Rating distribution
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN rating >= 4.5 THEN '4.5-5.0'
                    WHEN rating >= 4.0 THEN '4.0-4.4'
                    WHEN rating >= 3.0 THEN '3.0-3.9'
                    ELSE '<3.0'
                END as rating_range,
                COUNT(*) as count
            FROM businesses WHERE rating > 0
            GROUP BY rating_range
        """)
        stats["rating_distribution"] = [
            {"rating_range": row[0], "count": row[1]} for row in cursor.fetchall()
        ]
        
        # Reviews
        cursor.execute("SELECT SUM(reviews_count), AVG(reviews_count) FROM businesses")
        result = cursor.fetchone()
        if result:
            stats["total_reviews"] = result[0] or 0
            stats["avg_reviews_per_business"] = round(result[1], 2) if result[1] else 0
        
        return stats


# Use SQLite as default since DuckDB fails to install
try:
    import duckdb
    from database import DuckDBStorage as StorageBackend
    BusinessRecord = BusinessRecord  # Same dataclass
except ImportError:
    # Fallback to SQLite
    StorageBackend = SQLiteStorage


__all__ = ['SQLiteStorage', 'BusinessRecord', 'StorageBackend']
