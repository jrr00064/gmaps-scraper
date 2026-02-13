#!/usr/bin/env python3
"""
Google Maps Scraper v2 - OpenStreetMap Version
NO API key required - NO proxies required
Uses OpenStreetMap Overpass API (free, open data)

This version works reliably without proxies because OSM doesn't block.
Coverage: ~10-20% of Google Maps (still thousands of businesses)
"""

import asyncio
import aiohttp
import json
import gc
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from grid import GeoGrid
from sqlite_storage import SQLiteStorage, BusinessRecord


@dataclass
class Place:
    name: str
    address: str
    phone: str
    website: str
    category: str
    lat: float
    lng: float
    place_id: str


class OSMScraper:
    """
    Scraper using OpenStreetMap (Overpass API)
    100% free, no blocking, no proxies needed
    """
    
    OVERPASS_URL = "https://overpass-api.de/api/interpreter"
    
    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.stats = {"requests": 0, "success": 0, "places": 0}
    
    async def search_sector(self, lat: float, lng: float, radius: int = 2000) -> List[Place]:
        """Search for businesses using OSM"""
        async with self.semaphore:
            # Rate limit for OSM polite usage
            await asyncio.sleep(0.5)
            
            query = f"""[out:json][timeout:25];
            (
              node["name"]["amenity"](around:{radius},{lat},{lng});
              way["name"]["amenity"](around:{radius},{lat},{lng});
              node["name"]["shop"](around:{radius},{lat},{lng});
              way["name"]["shop"](around:{radius},{lat},{lng});
            );
            out body;"""
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.OVERPASS_URL, data={"data": query}) as resp:
                        self.stats["requests"] += 1
                        
                        if resp.status == 200:
                            data = await resp.json()
                            self.stats["success"] += 1
                            places = self._parse(data, lat, lng)
                            self.stats["places"] += len(places)
                            return places
                        return []
            except:
                return []
    
    def _parse(self, data: Dict, base_lat: float, base_lng: float) -> List[Place]:
        """Parse OSM response"""
        places = []
        seen = set()
        
        for element in data.get("elements", []):
            tags = element.get("tags", {})
            name = tags.get("name", "").strip()
            
            if not name or name in seen:
                continue
            seen.add(name)
            
            # Build address
            parts = []
            if tags.get("addr:street"):
                street = tags["addr:street"]
                if tags.get("addr:housenumber"):
                    street += f" {tags['addr:housenumber']}"
                parts.append(street)
            if tags.get("addr:postcode"):
                parts.append(tags["addr:postcode"])
            if tags.get("addr:city"):
                parts.append(tags["addr:city"])
            
            address = ", ".join(parts)
            
            # Category
            category = tags.get("amenity") or tags.get("shop") or "business"
            
            # Coordinates
            lat = element.get("lat", base_lat)
            lng = element.get("lon", base_lng)
            
            places.append(Place(
                name=name,
                address=address,
                phone=tags.get("phone", ""),
                website=tags.get("website", ""),
                category=category,
                lat=float(lat) if lat else base_lat,
                lng=float(lng) if lng else base_lng,
                place_id=f"osm_{element.get('id', '')}",
            ))
        
        return places


async def main():
    print("\n" + "="*60)
    print("  OpenStreetMap Business Scraper")
    print("  FREE - NO proxies - NO API key")
    print("="*60)
    
    # Grid
    grid = GeoGrid("Spain", grid_size=60)
    grid.generate()
    sectors = grid.filter_water_sectors()
    
    stats = grid.get_stats()
    print(f"\nGrid: {stats['total_sectors']:,} sectors → {stats['land_sectors']:,} land")
    
    # Scrape
    scraper = OSMScraper(max_concurrent=10)
    all_places = []
    
    print(f"\nScraping {len(sectors)} sectors...")
    
    for i, sector in enumerate(sectors):
        places = await scraper.search_sector(sector.lat, sector.lng)
        all_places.extend(places)
        
        if (i + 1) % 50 == 0:
            print(f"  [{i+1:,}/{len(sectors):,}] Found: {len(all_places):,} businesses")
            gc.collect()
    
    print(f"\n✓ Total: {len(all_places):,} businesses found")
    
    # Save
    if all_places:
        print("\nSaving...")
        os.makedirs("data", exist_ok=True)
        
        records = [
            BusinessRecord(
                name=p.name, phone=p.phone, address=p.address, website=p.website,
                rating=0.0, reviews_count=0, category=p.category, hours="{}",
                latitude=p.lat, longitude=p.lng, place_id=p.place_id,
                scraped_at=datetime.now().isoformat(),
            )
            for p in all_places
        ]
        
        with SQLiteStorage("data/osm_businesses.db") as db:
            db.insert_many(records)
            db.export_to_csv("data/osm_businesses.csv")
            db.export_to_json("data/osm_businesses.json")
        
        print(f"✓ Saved to data/osm_businesses.csv")
    
    print(f"\nStats: {scraper.stats['requests']} requests, {scraper.stats['success']} success")


if __name__ == "__main__":
    import os
    asyncio.run(main())
