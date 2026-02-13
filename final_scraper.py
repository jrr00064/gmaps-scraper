#!/usr/bin/env python3
"""
⚡ Google Maps Business Scraper - FINAL VERSION ⚡
Flexible: Works with or without proxies

MODES:
- FAST: With proxies → 35k businesses in ~2 minutes (90 concurrent)
- SLOW: Without proxies → ~100-500 sectors per IP (5 concurrent, 2-5s delays)

Based on edu_seo_scraper technique
"""

import asyncio
import aiohttp
import json
import re
import random
import gc
import os
import sys
import argparse
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from grid import GeoGrid
from sqlite_storage import SQLiteStorage, BusinessRecord


@dataclass
class Place:
    """Business place data"""
    name: str
    address: str
    phone: str
    website: str
    rating: float
    reviews: int
    category: str
    lat: float
    lng: float
    place_id: str
    hours: Dict


class Config:
    """Configuration presets"""
    
    # FAST mode - With proxies (like edu_seo_scraper)
    FAST = {
        "max_concurrent": 90,
        "delay_range": (0.05, 0.15),
        "pool_size": 150,
        "batch_size": 50,
        "gc_every": 20,
        "description": "FAST mode - Requires proxies"
    }
    
    # MEDIUM mode - Limited testing without proxies
    MEDIUM = {
        "max_concurrent": 10,
        "delay_range": (1.0, 3.0),
        "pool_size": 50,
        "batch_size": 20,
        "gc_every": 10,
        "description": "MEDIUM mode - Limited/no proxies"
    }
    
    # SLOW mode - No proxies, long delays
    SLOW = {
        "max_concurrent": 3,
        "delay_range": (2.0, 5.0),
        "pool_size": 20,
        "batch_size": 10,
        "gc_every": 5,
        "description": "SLOW mode - No proxies, safe from blocking"
    }
    
    @classmethod
    def auto_detect(cls, proxy_count: int) -> Dict:
        """Auto-configure based on proxy availability"""
        if proxy_count >= 50:
            return cls.FAST
        elif proxy_count >= 5:
            return cls.MEDIUM
        else:
            return cls.SLOW


class ProxyRotator:
    """Rotate through proxy list"""
    
    def __init__(self, proxies: List[str]):
        self.proxies = proxies or []
        self.current = 0
        self.failed = set()
        
    def get_next(self) -> Optional[str]:
        if not self.proxies:
            return None
        
        attempts = 0
        while attempts < len(self.proxies):
            proxy = self.proxies[self.current % len(self.proxies)]
            self.current += 1
            if proxy not in self.failed:
                return proxy
            attempts += 1
        return None
    
    def mark_failed(self, proxy: str):
        self.failed.add(proxy)
        print(f"    ⚠️ Proxy failed: {proxy[:30]}...")


class BusinessSpider:
    """Core spider with proxy support"""
    
    def __init__(self, proxies: List[str], config: Dict):
        self.proxy_rotator = ProxyRotator(proxies)
        self.config = config
        self.semaphore = asyncio.Semaphore(config["max_concurrent"])
        
        self.stats = {"requests": 0, "success": 0, "blocked": 0, "retry": 0, "places": 0}
    
    def _headers(self) -> Dict:
        uas = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        ]
        return {
            "User-Agent": random.choice(uas),
            "Accept": "text/html,*/*;q=0.8",
            "Accept-Language": random.choice(["es-ES", "en-US", "en-GB"]),
        }
    
    async def search(self, lat: float, lng: float, query: str, connector) -> List[Place]:
        async with self.semaphore:
            await asyncio.sleep(random.uniform(*self.config["delay_range"]))
            
            for attempt in range(3):
                proxy = self.proxy_rotator.get_next()
                result = await self._request(lat, lng, query, proxy, connector)
                
                if result is not None:
                    return result
                else:
                    self.stats["retry"] += 1
                    await asyncio.sleep(2 ** attempt)
            
            return []
    
    async def _request(self, lat: float, lng: float, query: str,
                       proxy: Optional[str], connector) -> Optional[List[Place]]:
        
        urls = ["https://www.google.com/search", "https://www.google.es/search"]
        params = f"?tbm=map&tch=1&q={query.replace(' ', '%20')}%20@{lat},{lng}&hl=es"
        url = random.choice(urls) + params
        
        async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30)) as session:
            proxy_url = f"http://{proxy}" if proxy and not proxy.startswith("http") else proxy
            
            try:
                self.stats["requests"] += 1
                async with session.get(url, headers=self._headers(), proxy=proxy_url) as resp:
                    text = await resp.text()
                    
                    if resp.status == 200:
                        self.stats["success"] += 1
                        places = self._parse(text, lat, lng)
                        self.stats["places"] += len(places)
                        return places
                    elif resp.status == 429:
                        self.stats["blocked"] += 1
                        if proxy:
                            self.proxy_rotator.mark_failed(proxy)
                        return None
                    else:
                        return None
            except:
                return None
    
    def _parse(self, html: str, base_lat: float, base_lng: float) -> List[Place]:
        places = []
        
        # Parse patterns
        patterns = [
            r'AF_initDataCallback\s*\([^}]*data\s*:\s*(\[[^\]]+\])',
            r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html, re.DOTALL)
            for match in matches[:1]:
                try:
                    data = json.loads(match.replace("'", '"'))
                    places.extend(self._extract(data, base_lat, base_lng))
                except:
                    pass
        
        seen = set()
        unique = []
        for p in places:
            if p.place_id not in seen:
                seen.add(p.place_id)
                unique.append(p)
        return unique
    
    def _extract(self, data: Any, base_lat: float, base_lng: float) -> List[Place]:
        places = []
        
        def search(obj, depth=0):
            if depth > 15 or not isinstance(obj, (dict, list)):
                return
            
            if isinstance(obj, dict):
                name = obj.get("title") or obj.get("name")
                if name and isinstance(name, str):
                    lat = obj.get("lat") or obj.get("latitude")
                    lng = obj.get("lng") or obj.get("longitude")
                    if lat and lng:
                        try:
                            places.append(Place(
                                name=name,
                                address=obj.get("address", ""),
                                phone=obj.get("phone", ""),
                                website=obj.get("website", ""),
                                rating=float(obj.get("rating", 0) or 0),
                                reviews=int(obj.get("reviews", 0) or 0),
                                category=obj.get("category", ""),
                                lat=float(lat) if lat else base_lat,
                                lng=float(lng) if lng else base_lng,
                                place_id=obj.get("placeId", "") or f"lat{lat}lng{lng}",
                                hours=obj.get("hours", {}),
                            ))
                        except:
                            pass
                for v in obj.values():
                    search(v, depth + 1)
            elif isinstance(obj, list):
                for item in obj:
                    search(item, depth + 1)
        
        search(data)
        return places


class Scraper:
    """Main scraper orchestrator"""
    
    def load_proxies(self, path: str) -> List[str]:
        if not os.path.exists(path):
            print(f"  ℹ️ No proxy file: {path}")
            return []
        
        proxies = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    proxies.append(line)
        return proxies
    
    async def run(self, country: str = "Spain", query: str = "negocios",
                  proxy_file: Optional[str] = None, mode: str = "auto",
                  max_sectors: Optional[int] = None, grid_size: int = 165) -> int:
        
        print("\n" + "="*70)
        print("  🗺️  Google Maps Business Scraper")
        print("="*70)
        
        # Load proxies
        proxies = self.load_proxies(proxy_file) if proxy_file else []
        
        # Determine config
        if mode == "auto":
            config = Config.auto_detect(len(proxies))
        elif mode == "fast":
            config = Config.FAST
        elif mode == "medium":
            config = Config.MEDIUM
        else:
            config = Config.SLOW
        
        print(f"\n[Mode] {config['description']}")
        print(f"  Concurrent: {config['max_concurrent']}")
        print(f"  Delay: {config['delay_range'][0]}-{config['delay_range'][1]}s")
        print(f"  Proxies: {len(proxies)} loaded")
        
        # Grid
        print("\n[1/3] Building grid...")
        grid = GeoGrid(country=country, grid_size=grid_size)
        grid.generate()
        sectors = grid.filter_water_sectors()
        
        stats = grid.get_stats()
        print(f"  Total: {stats['total_sectors']:,} | Land: {stats['land_sectors']:,}")
        print(f"  Water eliminated: {stats['water_elimination']:.1%}")
        
        if max_sectors:
            sectors = sectors[:max_sectors]
        total = len(sectors)
        print(f"  Sectors to scrape: {total:,}")
        
        # Setup spider
        spider = BusinessSpider(proxies, config)
        connector = aiohttp.TCPConnector(
            limit=config["pool_size"],
            limit_per_host=50,
        )
        
        # Scrape
        print(f"\n[2/3] Scraping...")
        all_places = []
        batch_size = config["batch_size"]
        start = datetime.now()
        
        for i in range(0, total, batch_size):
            batch = sectors[i:i+batch_size]
            tasks = [spider.search(s.lat, s.lng, query, connector) for s in batch]
            results = await asyncio.gather(*tasks)
            
            for places in results:
                all_places.extend(places)
            
            elapsed = (datetime.now() - start).total_seconds()
            progress = min(i + batch_size, total)
            rate = progress / elapsed if elapsed > 0 else 0
            
            print(f"  [{progress:6,}/{total:,}] ({progress/total*100:5.1f}%) | "
                  f"Found: {len(all_places):,} | Rate: {rate:.1f} sec/s")
            
            if (i // batch_size) % config["gc_every"] == 0:
                gc.collect()
        
        await connector.close()
        
        # Save
        print(f"\n[3/3] Saving {len(all_places):,} businesses...")
        await self._save(all_places, country)
        
        elapsed = (datetime.now() - start).total_seconds()
        print(f"\n{'='*70}")
        print(f"  Complete: {len(all_places):,} businesses in {elapsed:.0f}s")
        print(f"  Requests: {spider.stats['requests']} | Success: {spider.stats['success']} | Blocked: {spider.stats['blocked']}")
        
        return len(all_places)
    
    async def _save(self, places: List[Place], country: str):
        os.makedirs("/tmp/gmaps-scraper/data", exist_ok=True)
        
        records = [
            BusinessRecord(
                name=p.name, phone=p.phone, address=p.address, website=p.website,
                rating=p.rating, reviews_count=p.reviews, category=p.category,
                hours=json.dumps(p.hours), latitude=p.lat, longitude=p.lng,
                place_id=p.place_id, scraped_at=datetime.now().isoformat(),
            )
            for p in places
        ]
        
        db_path = f"/tmp/gmaps-scraper/data/{country.lower()}_businesses.db"
        with SQLiteStorage(db_path) as db:
            db.insert_many(records)
            db.export_to_csv(f"/tmp/gmaps-scraper/data/{country.lower()}_businesses.csv")
            db.export_to_json(f"/tmp/gmaps-scraper/data/{country.lower()}_businesses.json")
        
        print(f"  ✓ CSV: {country.lower()}_businesses.csv")


def main():
    parser = argparse.ArgumentParser(description="Google Maps Business Scraper")
    parser.add_argument("--country", default="Spain")
    parser.add_argument("--query", default="negocios")
    parser.add_argument("--proxy-file", default="/tmp/gmaps-scraper/proxies.txt")
    parser.add_argument("--mode", choices=["auto", "fast", "medium", "slow"], default="auto")
    parser.add_argument("--max-sectors", type=int, default=None)
    parser.add_argument("--grid-size", type=int, default=165)
    parser.add_argument("--test", action="store_true", help="Test mode (20 sectors)")
    
    args = parser.parse_args()
    
    if args.test:
        args.max_sectors = 20
        args.grid_size = 20
        print("\n⚡ TEST MODE: 20 sectors\n")
    
    scraper = Scraper()
    count = asyncio.run(scraper.run(
        country=args.country,
        query=args.query,
        proxy_file=args.proxy_file,
        mode=args.mode,
        max_sectors=args.max_sectors,
        grid_size=args.grid_size,
    ))
    
    print(f"\nDone! Total: {count:,} businesses")


if __name__ == "__main__":
    main()
