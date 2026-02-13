# Google Maps Business Scraper

Scraper de negocios de Google Maps basado en la técnica de edu_seo_scraper.
Scrapea ~35,000 negocios de España en ~2 minutos usando proxies rotativos.

## Características

- ✅ **Sin API Key**: Usa proxies + HTTP scraping
- ✅ **Grid-based**: División geográfica 165×165
- ✅ **Water filtering**: Elimina sectores de agua (océanos)
- ✅ **Async**: 90 requests concurrentes
- ✅ **Auto-config**: Detecta si hay proxies y ajusta velocidad
- ✅ **Tres modos**: FAST (proxies), MEDIUM, SLOW (sin proxies)

## Requisitos

```bash
pip install aiohttp
```

## Uso

### Modo rápido (con proxies)
```bash
# Añade proxies a proxies.txt
echo "user:pass@proxy.com:8080" > proxies.txt

# Ejecuta
python3 final_scraper.py --mode fast --country Spain
```

### Modo lento (sin proxies) - Testing
```bash
python3 final_scraper.py --mode slow --max-sectors 100
```

### Modo automático (detecta proxies)
```bash
python3 final_scraper.py --mode auto
```

### Test rápido (20 sectores)
```bash
python3 final_scraper.py --test
```

## Configuración

**Modos disponibles:**

| Modo | Concurrent | Delay | Requiere Proxies | Velocidad |
|------|-----------|-------|-----------------|-----------|
| fast | 90 | 0.05-0.15s | Sí | ~2 min |
| medium | 10 | 1-3s | Opcional | ~20 min |
| slow | 3 | 2-5s | No | ~2 horas |

## Archivos

```
gmaps-scraper/
├── final_scraper.py      [MAIN] Scraper principal
├── get_proxies.py        [UTIL] Descarga proxies free
├── proxies.txt           [CONFIG] Tus proxies (crear)
├── src/
│   ├── grid.py          Grid geográfico
│   └── sqlite_storage.py Base de datos
└── data/                Output CSV/JSON
```

## Proxies

**Formato:**
```
ip:port
user:pass@ip:port
http://user:pass@ip:port
```

**Proveedores recomendados:**
- PacketStream (~$1/GB)
- Webshare ($4.5/GB)
- Bright Data ($15/GB)

## Output

Genera en `data/`:
- `{country}_businesses.csv`
- `{country}_businesses.json`
- `{country}_businesses.db` (SQLite)

## Advertencia

⚠️ **Google puede bloquear IPs sin proxies.**
- Sin proxies: ~50 sectores/IP antes del bloqueo
- Con proxies: Ilimitado con rotación
- Usa SLOW mode para testing seguro

## Estructura CSV

```
name,address,phone,website,rating,reviews_count,category,latitude,longitude,place_id,hours,scraped_at
```

## Licencia

MIT - Uso bajo tu propia responsabilidad.
