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
AIS_API_KEY = "20d9c426a6993500fd41857b0753aee5c2a6a6ed"

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
# 3. HISTORIAL — control de última inserción
# ==========================================
INTERVALO_HISTORIAL_HORAS = 6
ultimo_guardado_historial = {}  # buque_id → datetime UTC

# ==========================================
# 4. HELPER ETA AIS
# ==========================================
def construir_eta_ais(eta_obj):
    if not eta_obj:
        return None
    try:
        month  = eta_obj.get("Month", 0)
        day    = eta_obj.get("Day", 0)
        hour   = eta_obj.get("Hour", 0)
        minute = eta_obj.get("Minute", 0)
        if month == 0 or day == 0:
            return None
        ahora = datetime.now(timezone.utc)
        eta = datetime(ahora.year, month, day, hour, minute, tzinfo=timezone.utc)
        if eta < ahora:
            eta = datetime(ahora.year + 1, month, day, hour, minute, tzinfo=timezone.utc)
        return eta.isoformat()
    except Exception:
        return None

# ==========================================
# 5. EL RADAR AIS GLOBAL
# ==========================================
async def radar_global_ais():
    print("🌍 Iniciando Radar AIS Global en la Nube...")
    
    while True:
        try:
            r = requests.get(f"{SUPABASE_URL}/rest/v1/buques?mmsi=not.is.null&select=id,nombre,mmsi", headers=HEADERS)
            flota = r.json()
            barcos_conocidos = {b["mmsi"]: b for b in flota if b.get("mmsi")}
            
            if not barcos_conocidos:
                print("⚠️ No hay barcos con MMSI. Reintentando en 60s...")
                await asyncio.sleep(60)
                continue

            print(f"🔍 Escuchando satélites para {len(barcos_conocidos)} barcos...")

            suscripcion = {
                "APIKey": AIS_API_KEY,
                "BoundingBoxes": [[[-90.0, -180.0],[90.0, 180.0]]],
                "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
            }

            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                print("✅ ¡Túnel AIS abierto! Recibiendo datos...")
                await websocket.send(json.dumps(suscripcion))
                
                while True:
                    mensaje = await websocket.recv()
                    datos = json.loads(mensaje)
                    
                    if "error" in datos:
                        print(f"❌ Error de Aisstream: {datos['error']}")
                        await asyncio.sleep(10)
                        break

                    tipo = datos.get("MessageType")
                    mmsi = datos.get("MetaData", {}).get("MMSI")
                    if not mmsi or mmsi not in barcos_conocidos:
                        continue
                    barco = barcos_conocidos[mmsi]

                    if tipo == "PositionReport":
                        rep   = datos["Message"]["PositionReport"]
                        lat   = rep["Latitude"]
                        lon   = rep["Longitude"]
                        rumbo = rep["TrueHeading"] if rep["TrueHeading"] != 511 else 0
                        ahora     = datetime.now(timezone.utc)
                        ahora_iso = ahora.isoformat()
                        
                        print(f"🎯 [BIP AIS] {barco['nombre']} -> Lat: {lat}, Lon: {lon}")
                        
                        requests.patch(
                            f"{SUPABASE_URL}/rest/v1/buques?id=eq.{barco['id']}",
                            headers=HEADERS,
                            json={"latitud": lat, "longitud": lon, "rumbo": rumbo, "ultima_senal": ahora_iso}
                        )

                        # Historial cada 6 horas
                        buque_id = barco["id"]
                        ultimo   = ultimo_guardado_historial.get(buque_id)
                        horas    = (ahora - ultimo).total_seconds() / 3600 if ultimo else 999
                        if horas >= INTERVALO_HISTORIAL_HORAS:
                            requests.post(
                                f"{SUPABASE_URL}/rest/v1/historial_posiciones",
                                headers=HEADERS,
                                json={"buque_id": buque_id, "latitud": lat, "longitud": lon,
                                      "rumbo": rumbo, "timestamp": ahora_iso}
                            )
                            ultimo_guardado_historial[buque_id] = ahora
                            print(f"📍 [HISTORIAL] {barco['nombre']} guardado.")

                    elif tipo == "ShipStaticData":
                        static  = datos["Message"]["ShipStaticData"]
                        destino = static.get("Destination", "").strip().upper()
                        if not destino or destino in ("", "NONE", ".", "NOT DEFINED", "@@@@@@@@@"):
                            continue
                        eta_iso = construir_eta_ais(static.get("Eta"))
                        payload = {"destino_declarado": destino}
                        if eta_iso:
                            payload["eta_declarada"] = eta_iso
                        requests.patch(
                            f"{SUPABASE_URL}/rest/v1/buques?id=eq.{barco['id']}",
                            headers=HEADERS, json=payload
                        )
                        print(f"🧭 [DESTINO AIS] {barco['nombre']} → {destino} | ETA: {eta_iso}")

        except Exception as e:
            print(f"⚠️ Reconectando satélite en 5s... ({e})")
            await asyncio.sleep(5)

if __name__ == "__main__":
    hilo = threading.Thread(target=servidor_web_fantasma)
    hilo.daemon = True
    hilo.start()
    asyncio.run(radar_global_ais())
