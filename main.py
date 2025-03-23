import json
import sqlite3
from shapely.geometry import shape, MultiPolygon
from shapely.wkb import dumps as wkb_dumps
from shapely.validation import explain_validity

# 1. 建立 SQLite 資料庫並啟用空間擴展
def create_spatial_database(db_path):
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)
    conn.execute("SELECT load_extension('mod_spatialite')")
    conn.execute("SELECT InitSpatialMetaData(1)")
    
    conn.execute('''CREATE TABLE IF NOT EXISTS site_effects
                 (town_name TEXT PRIMARY KEY, site_value REAL)''')
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS town_boundaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )
    """)
    conn.execute("""
        SELECT AddGeometryColumn('town_boundaries', 'geometry', 4326, 'MULTIPOLYGON', 'XY')
    """)
    return conn

# 2. 將 GeoJSON 轉換並插入 SQLite
def geojson_to_sqlite(geojson_path, db_path):
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)

    conn = create_spatial_database(db_path)
    cursor = conn.cursor()

    for feature in geojson_data['features']:
        id = feature.get('id', '')
        props = feature.get('properties', {})
        county_name = props.get('COUNTYNAME', 'unnamed')
        town_name = props.get('TOWNNAME', 'unnamed')
        name = county_name + town_name

        # 轉換幾何
        geom = shape(feature['geometry'])
        if geom.geom_type == 'Polygon':
            geom = MultiPolygon([geom])
        elif geom.geom_type != 'MultiPolygon':
            print(f"跳過不支持的幾何類型: {geom.geom_type}")
            continue

        if not geom.is_valid:
            print(f"修復前: {name}, 類型 = {geom.geom_type}, 有效性 = {geom.is_valid}, 原因 = {explain_validity(geom)}")
            geom = geom.buffer(0)
            print(f"修復後: {name}, 類型 = {geom.geom_type}, 有效性 = {geom.is_valid}")
        if geom.is_empty or not geom.is_valid:
            print(f"跳過無法修復的幾何: {name}, 原因: {explain_validity(geom)}")
            continue
        if geom.geom_type != 'MultiPolygon':
            print(f"類型不符: {name}, 修復後類型 = {geom.geom_type}")
            # 嘗試轉換回 MultiPolygon
            if geom.geom_type == 'Polygon':
                geom = MultiPolygon([geom])
            else:
                print(f"跳過不支持的修復後類型: {geom.geom_type} ({name})")
                continue

        wkb_geom = wkb_dumps(geom)

        cursor.execute("""
            INSERT INTO town_boundaries (id, name, geometry)
            VALUES (?, ?, GeomFromWKB(?, 4326))
        """, (id, name, wkb_geom))

    conn.commit()
    cursor.execute("SELECT CreateSpatialIndex('town_boundaries', 'geometry')")
    conn.commit()
    conn.close()
    print(f"成功將 GeoJSON 轉換到 {db_path}")

# 3. 時空查詢函數
def coords_to_town(db_path, lat, lon):
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)
    conn.execute("SELECT load_extension('mod_spatialite')")
    cursor = conn.cursor()

    query = """
        SELECT name
        FROM town_boundaries
        WHERE ST_Contains(geometry, MakePoint(?, ?, 4326))
    """
    cursor.execute(query, (lon, lat))
    result = cursor.fetchone()
    
    conn.close()
    return result[0] if result else None

if __name__ == "__main__":
    geojson_file = "taiwan_town.geojson"
    sqlite_db = "geo_seismic_data.db"

    #geojson_to_sqlite(geojson_file, sqlite_db)

    latitude = 120.354952
    longitude = 23.760542
    nearby_town = coords_to_town(sqlite_db, latitude, longitude)
    print(f"附近鄉鎮: {nearby_town}")