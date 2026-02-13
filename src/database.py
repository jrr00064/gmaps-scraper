"""
DuckDB Database Handler
Streaming storage for Google Maps data
"""

import duckdb
import pandas as pd
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import json
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


class DuckDBStorage:
    """
    DuckDB storage manager
    - Streaming inserts (sector by sector)
    - Low memory footprint
    - Export to CSV/Excel/JSON
    """
    
    def __init__(self, db_path: str = "/tmp/gmaps-scraper/data/businesses.duckdb"):
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    def __enter__(self):
        """Open connection"""
        self.conn = duckdb.connect(self.db_path)
        self._create_tables()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection"""
        if self.conn:
            self.conn.close()
    
    def _create_tables(self):
        """Create tables if they don't exist"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS businesses (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                phone VARCHAR,
                address VARCHAR,
                website VARCHAR,
                rating DOUBLE DEFAULT 0.0,
                reviews_count INTEGER DEFAULT 0,
                category VARCHAR,
                hours VARCHAR,  -- JSON string
                latitude DOUBLE,
                longitude DOUBLE,
                place_id VARCHAR UNIQUE,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for faster queries
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_latitude ON businesses(latitude)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_longitude ON businesses(longitude)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_category ON businesses(category)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_place_id ON businesses(place_id)")
        
        self.conn.commit()
    
    def insert_business(self, record: BusinessRecord) -> bool:
        """Insert single business record"""
        try:
            self.conn.execute("""
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
            return True
        except Exception as e:
            print(f"Error inserting record: {e}")
            return False
    
    def insert_many(self, records: List[BusinessRecord], batch_size: int = 100):
        """
        Batch insert records
        More efficient than individual inserts
        """
        if not records:
            return 0
        
        inserted = 0
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                # Convert to tuples
                data = [
                    (r.name, r.phone, r.address, r.website, r.rating, 
                     r.reviews_count, r.category, r.hours, r.latitude,
                     r.longitude, r.place_id, r.scraped_at)
                    for r in batch
                ]
                
                # Execute batch insert
                self.conn.executemany("""
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
        result = self.conn.execute("SELECT COUNT(*) FROM businesses").fetchone()
        return result[0] if result else 0
    
    def get_by_category(self, category: str) -> pd.DataFrame:
        """Get businesses by category"""
        return self.conn.execute(
            f"SELECT * FROM businesses WHERE category ILIKE '%{category}%'"
        ).fetchdf()
    
    def get_by_location(self, lat: float, lng: float, radius_km: float = 5) -> pd.DataFrame:
        """Get businesses near a location"""
        # Simple distance calculation (approximate)
        lat_range = radius_km / 111.0  # 1 degree ≈ 111km
        lng_range = radius_km / (111.0 * abs(lat) * 3.14159 / 180)
        
        return self.conn.execute(f"""
            SELECT * FROM businesses 
            WHERE latitude BETWEEN {lat - lat_range} AND {lat + lat_range}
            AND longitude BETWEEN {lng - lng_range} AND {lng + lng_range}
        """).fetchdf()
    
    def export_to_csv(self, filepath: str, chunk_size: int = 10000):
        """Export data to CSV"""
        total = self.get_count()
        print(f"Exporting {total} records to CSV...")
        
        # DuckDB can export directly
        self.conn.execute(f"""
            COPY (SELECT * FROM businesses) TO '{filepath}' (HEADER, DELIMITER ',')
        """)
        print(f"Exported to {filepath}")
    
    def export_to_json(self, filepath: str):
        """Export data to JSON"""
        df = self.conn.execute("SELECT * FROM businesses").fetchdf()
        df.to_json(filepath, orient="records", indent=2)
        print(f"Exported to {filepath}")
    
    def export_to_excel(self, filepath: str):
        """Export data to Excel"""
        df = self.conn.execute("SELECT * FROM businesses").fetchdf()
        df.to_excel(filepath, index=False)
        print(f"Exported to {filepath}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        stats = {}
        
        # Total count
        stats["total_businesses"] = self.get_count()
        
        # By category
        cat_df = self.conn.execute("""
            SELECT category, COUNT(*) as count 
            FROM businesses 
            WHERE category != '' 
            GROUP BY category 
            ORDER BY count DESC 
            LIMIT 10
        """).fetchdf()
        stats["top_categories"] = cat_df.to_dict("records")
        
        # Rating distribution
        rating_df = self.conn.execute("""
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
        """).fetchdf()
        stats["rating_distribution"] = rating_df.to_dict("records")
        
        # Reviews count
        reviews = self.conn.execute("""
            SELECT 
                SUM(reviews_count) as total_reviews,
                AVG(reviews_count) as avg_reviews
            FROM businesses
        """).fetchone()
        if reviews:
            stats["total_reviews"] = reviews[0]
            stats["avg_reviews_per_business"] = round(reviews[1], 2)
        
        return stats
    
    def vacuum(self):
        """Optimize database"""
        self.conn.execute("VACUUM")
        self.conn.execute("ANALYZE")
