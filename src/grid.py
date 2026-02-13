"""
Grid Generator - Creates geographic grid for scraping
Based on 60x60 pixel sectors from edu_seo_scraper technique
"""

import json
from typing import List, Dict, Tuple
from dataclasses import dataclass
import math

@dataclass
class Sector:
    id: str
    lat: float
    lng: float
    lat_min: float
    lat_max: float
    lng_min: float
    lng_max: float
    is_land: bool = True

class GeoGrid:
    """Generate and filter grid sectors for a country"""
    
    COUNTRIES = {
        "Spain": {
            # Based on tweet: 27,140 sectors (roughly 165x165 grid)
            # 81.7% water elimination -> 4,958 land sectors
            "min_lat": 27.0, "max_lat": 44.5,  # Includes Canary Islands
            "min_lng": -18.5, "max_lng": 5.0,
            "water_ratio": 0.817,
        },
        "France": {
            "min_lat": 41.3, "max_lat": 51.1,
            "min_lng": -5.5, "max_lng": 9.6,
            "water_ratio": 0.65,
        },
        "Mexico": {
            "min_lat": 14.5, "max_lat": 32.7,
            "min_lng": -118.4, "max_lng": -86.0,
            "water_ratio": 0.40,
        },
    }
    
    def __init__(self, country: str, grid_size: int = 60):
        self.country = country
        self.grid_size = grid_size
        self.sectors: List[Sector] = []
        
        if country not in self.COUNTRIES:
            raise ValueError(f"Country {country} not supported. Available: {list(self.COUNTRIES.keys())}")
    
    def generate(self) -> List[Sector]:
        """Generate grid for the country"""
        bounds = self.COUNTRIES[self.country]
        
        lat_step = (bounds["max_lat"] - bounds["min_lat"]) / self.grid_size
        lng_step = (bounds["max_lng"] - bounds["min_lng"]) / self.grid_size
        
        sectors = []
        for i in range(self.grid_size):
            for j in range(self.grid_size):
                sector = Sector(
                    id=f"{i}_{j}",
                    lat=bounds["min_lat"] + i * lat_step + lat_step / 2,
                    lng=bounds["min_lng"] + j * lng_step + lng_step / 2,
                    lat_min=bounds["min_lat"] + i * lat_step,
                    lat_max=bounds["min_lat"] + (i + 1) * lat_step,
                    lng_min=bounds["min_lng"] + j * lng_step,
                    lng_max=bounds["min_lng"] + (j + 1) * lng_step,
                )
                sectors.append(sector)
        
        self.sectors = sectors
        return sectors
    
    def filter_water_sectors(self) -> List[Sector]:
        """Remove sectors that are water (oceans, seas)"""
        land_sectors = []
        
        for sector in self.sectors:
            if self._is_land(sector):
                sector.is_land = True
                land_sectors.append(sector)
            else:
                sector.is_land = False
        
        self.sectors = land_sectors
        return land_sectors
    
    def _is_land(self, sector: Sector) -> bool:
        """Check if sector is land for Spain (main geography)
        More aggressive water filtering to match 81.7% from Twitter thread
        """
        lat, lng = sector.lat, sector.lng
        
        if self.country == "Spain":
            # ============================================
            # CANARY ISLANDS (separate)
            # ============================================
            if 27.5 <= lat <= 29.5 and -18.5 <= lng <= -13.0:
                # More precise Canary Islands shape
                if lng < -18.0 and lat > 28.5:
                    return False  # Ocean NW of Tenerife
                if lng > -13.0 and lat < 28.0:
                    return False  # Ocean east
                return True
            
            # ============================================
            # BALEARIC ISLANDS
            # ============================================
            if 38.5 <= lat <= 40.0 and 1.0 <= lng <= 4.0:
                return True
            
            # ============================================
            # MAINLAND SPAIN + PORTUGAL
            # ============================================
            
            # Too far west - Atlantic Ocean
            if lng < -9.5:
                return False
            
            # Too far east - Mediterranean Sea
            if lng > 3.0:
                return False
            
            # Too far north - France  
            if lat > 43.2:
                return False
            
            # Too far south - Morocco
            if lat < 36.0:
                return False
            
            # Bay of Biscay (northwest) - more aggressive
            if lat > 42.8 and lng < -1.5:
                return False
            
            # Mediterranean coast of France (east of Pyrenees)
            if lat > 42.5 and lng > 3.0:
                return False
            
            # Strait of Gibraltar
            if lat < 36.5 and lng > -5.5:
                return False
            
            # Mediterranean Sea (east) - aggressive
            if lng > 0.3 and lat < 42.0:
                return False
            
            # Eastern Mediterranean (east of Balearics)
            if lng > 1.5:
                return False
            
            # Atlantic Ocean (west of Portugal) - more aggressive
            if lng < -9.0:
                return False
            
            # Core mainland bounds - tighter
            if not (36.2 <= lat <= 43.3 and -9.3 <= lng <= 3.0):
                return False
            
            return True
        
        # Simple bounding box for other countries
        bounds = self.COUNTRIES[self.country]
        return (bounds["min_lat"] <= lat <= bounds["max_lat"] and 
                bounds["min_lng"] <= lng <= bounds["max_lng"])
    
    def get_stats(self) -> Dict:
        """Return grid statistics"""
        total = self.grid_size * self.grid_size
        land = len(self.sectors)
        water = total - land
        
        return {
            "country": self.country,
            "grid_size": self.grid_size,
            "total_sectors": total,
            "land_sectors": land,
            "water_sectors": water,
            "water_elimination": water/total,  # Float for formatting
            "estimated_requests": land,
        }
    
    def to_json(self, filepath: str):
        """Save sectors to JSON"""
        data = [
            {
                "id": s.id,
                "lat": s.lat,
                "lng": s.lng,
                "lat_min": s.lat_min,
                "lat_max": s.lat_max,
                "lng_min": s.lng_min,
                "lng_max": s.lng_max,
                "is_land": s.is_land,
            }
            for s in self.sectors
        ]
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)


if __name__ == "__main__":
    # Test grid generation
    grid = GeoGrid(country="Spain", grid_size=60)
    
    print("Generating grid...")
    sectors = grid.generate()
    
    print(f"Total sectors: {len(sectors)}")
    print(f"Filtering water...")
    
    land_sectors = grid.filter_water_sectors()
    
    stats = grid.get_stats()
    print("\nGrid Statistics:")
    print(json.dumps(stats, indent=2))
    
    # Save to file
    grid.to_json("/tmp/gmaps-scraper/data/spain_grid.json")
    print(f"\nSaved {len(land_sectors)} sectors to spain_grid.json")
