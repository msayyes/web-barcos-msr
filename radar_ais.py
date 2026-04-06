import asyncio
import websockets
import json
import requests
from datetime import datetime, timezone
import threading
import http.server
import socketserver
import os

# ==========================================
# 1. CONFIGURACIÓN (TUS 3 LLAVES)
# ==========================================
SUPABASE_URL = "https://pozwondqqzurujbsanhn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBvendvbmRxcXp1cnVqYnNhbmhuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY4MDI2MiwiZXhwIjoyMDg4MjU2MjYyfQ.7sa0HnppwjWlZhh_cZRqcW-qMmlAex8vY3-4dNWFcRU"
AIS_API_KEY = "ac1dbf25f46949cd4312c94235e5ccedb843a9a3"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

# ==========================================
# 2. EL DISFRAZ (Para que el servidor sea GRATIS)
# ==========================================
def servidor_web_fantasma():
    puerto = int(os.environ.get("PORT", 10000))
    Handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", puerto), Handler) as httpd:
        print(f"🌍 Servidor web camuflado activo en el puerto {puerto}...")
        httpd.serve_forever()

# ==========================================
# 3. EL RADAR AIS GLOBAL
# ==========================================
async def radar_global_ais():
    print("🌍 Iniciando Radar AIS Global en la Nube...")
    
    while True: # Bucle maestro por si hay micro-cortes en el servidor
        try:
            r = requests.get(f"{SUPABASE_URL}/rest/v1/buques?mmsi=not.is.null&select=id,nombre,mmsi", headers=HEADERS)
            flota = r.json()
            barcos_conocidos = {b["mmsi"]: b for b in flota if b.get("mmsi")}
            
            if not barcos_conocidos:
                print("⚠️ No hay barcos con MMSI. Reintentando en 60s...")
                await asyncio.sleep(60)
                continue

            print(f"🔍 Escuchando satélites para {len(barcos_conocidos)} barcos...")

            url_ais = "wss://stream.aisstream.io/v0/stream"
            suscripcion = {
                "APIKey": AIS_API_KEY,
                "BoundingBoxes": [[[-90.0, -180.0],[90.0, 180.0]]],
                "FilterMessageTypes": ["PositionReport"]
            }

            async with websockets.connect(url_ais) as websocket:
                print("✅ ¡Túnel AIS abierto en la nube! Recibiendo datos...")
                await websocket.send(json.dumps(suscripcion))
                
                while True:
                    mensaje = await websocket.recv()
                    datos = json.loads(mensaje)
                    
                    if "error" in datos:
                        print(f"❌ Error de Aisstream: {datos['error']}")
                        await asyncio.sleep(10)
                        break
                        
                    if datos.get("MessageType") == "PositionReport":
                        mmsi = datos["MetaData"]["MMSI"]
                        
                        if mmsi in barcos_conocidos:
                            barco = barcos_conocidos[mmsi]
                            rep = datos["Message"]["PositionReport"]
                            lat = rep["Latitude"]
                            lon = rep["Longitude"]
                            rumbo = rep["TrueHeading"] if rep["TrueHeading"] != 511 else 0
                            ahora = datetime.now(timezone.utc).isoformat()
                            
                            print(f"🎯 [BIP AIS] {barco['nombre']} -> Lat: {lat}, Lon: {lon}")
                            
                            url_update = f"{SUPABASE_URL}/rest/v1/buques?id=eq.{barco['id']}"
                            requests.patch(url_update, headers=HEADERS, json={
                                "latitud": lat, "longitud": lon, "rumbo": rumbo, "ultima_senal": ahora
                            })
                            
        except Exception as e:
            print(f"⚠️ Reconectando satélite en 5s... ({e})")
            await asyncio.sleep(5)

if __name__ == "__main__":
    # Arrancamos la web fantasma en segundo plano
    hilo = threading.Thread(target=servidor_web_fantasma)
    hilo.daemon = True
    hilo.start()
    
    # Arrancamos el radar
    asyncio.run(radar_global_ais())