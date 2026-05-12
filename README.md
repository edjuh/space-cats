# space-cats
Astronomical object lists in normalized JSON format.

## Catalogs

- `catalogs/messier.json`
- `catalogs/caldwell.json`
- `catalogs/herschel400.json`
- `catalogs/sharpless.json`
- `catalogs/bennett.json`
- `catalogs/dunlop.json`
- `catalogs/bambury.json`
- `catalogs/whitman.json`

Coordinates are J2000 decimal degrees:

- `ra`: right ascension in degrees
- `dec`: declination in degrees
- `duration`: suggested imaging block in seconds
- `priority`: optional scheduler weight

The files use `schema/space-cats-v1.schema.json`.

## Build

The generated catalogs can be rebuilt from a local NGC/IC/Messier master file:

```bash
python3 tools/build_deepsky_catalogs.py --master catalogs/ngc-ic-messier-catalog.json
```

`caldwell`, `herschel400`, `sharpless`, `bennett`, `dunlop`, `bambury`, and `whitman` require network access for their source tables.
