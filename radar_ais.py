import asyncio
import websockets
import json
import requests
from datetime import datetime, timezone, timedelta
import threading
import http.server
import socketserver
import os

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
SUPABASE_URL = "https://pozwondqqzurujbsanhn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBvendvbmRxcXp1cnVqYnNhbmhuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY4MDI2MiwiZXhwIjoyMDg4MjU2MjYyfQ.7sa0HnppwjWlZhh_cZRqcW-qMmlAex8vY3-4dNWFcRU"
AIS_API_KEY   = "ac1dbf25f46949cd4312c94235e5ccedb843a9a3"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

INTERVALO_HISTORIAL_HORAS = 6

# Cache en memoria para no hacer consultas en cada señal AIS
# Se consulta la BD la PRIMERA vez por barco y luego se mantiene en memoria
ultimo_guardado_historial = {}   # buque_id → datetime UTC

# ==========================================
# 2. SERVIDOR WEB FANTASMA (para Render gratis)
# ==========================================
def servidor_web_fantasma():
    puerto = int(os.environ.get("PORT", 10000))
    Handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", puerto), Handler) as httpd:
        print(f"🌍 Servidor web activo en el puerto {puerto}...")
        httpd.serve_forever()

# ==========================================
# 3. HELPERS
# ==========================================
def construir_eta_ais(eta_obj):
    if not eta_obj:
        return None
    try:
        month  = eta_obj.get("Month",  0)
        day    = eta_obj.get("Day",    0)
        hour   = eta_obj.get("Hour",   0)
        minute = eta_obj.get("Minute", 0)
        if month == 0 or day == 0:
            return None
        ahora = datetime.now(timezone.utc)
        eta   = datetime(ahora.year, month, day, hour, minute, tzinfo=timezone.utc)
        if eta < ahora:
            eta = datetime(ahora.year + 1, month, day, hour, minute, tzinfo=timezone.utc)
        return eta.isoformat()
    except Exception:
        return None

def obtener_ultimo_historial_bd(buque_id):
    """Consulta la BD para saber cuándo se guardó el último punto del historial."""
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/historial_posiciones"
            f"?buque_id=eq.{buque_id}&select=timestamp&order=timestamp.desc&limit=1",
            headers=HEADERS,
            timeout=5
        )
        data = r.json()
        if data and len(data) > 0:
            ts = data[0].get("timestamp")
            if ts:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception as e:
        print(f"⚠️ Error consultando historial para {buque_id}: {e}")
    return None

def debe_guardar_historial(buque_id, ahora):
    """
    Devuelve True si han pasado >= 6h desde el último punto guardado.
    Consulta la BD si no tenemos el dato en memoria (p.ej. tras reinicio de Render).
    """
    ultimo = ultimo_guardado_historial.get(buque_id)

    if ultimo is None:
        # Primera vez que vemos este barco en esta sesión → consultar BD
        ultimo = obtener_ultimo_historial_bd(buque_id)
        if ultimo is None:
            # Nunca guardado → guardar ahora
            return True
        # Guardar en memoria para no volver a consultar la BD
        ultimo_guardado_historial[buque_id] = ultimo

    horas_transcurridas = (ahora - ultimo).total_seconds() / 3600
    return horas_transcurridas >= INTERVALO_HISTORIAL_HORAS

# ==========================================
# 4. RADAR AIS GLOBAL
# ==========================================
async def radar_global_ais():
    print("🌍 Iniciando Radar AIS Global en la Nube...")
    
    while True:
        try:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/buques?mmsi=not.is.null&select=id,nombre,mmsi",
                headers=HEADERS, timeout=10
            )
            flota = r.json()
            barcos_conocidos = {b["mmsi"]: b for b in flota if b.get("mmsi")}
            
            if not barcos_conocidos:
                print("⚠️ No hay barcos con MMSI. Reintentando en 60s...")
                await asyncio.sleep(60)
                continue

            print(f"🔍 Escuchando satélites para {len(barcos_conocidos)} barcos...")

            suscripcion = {
                "APIKey": AIS_API_KEY,
                "BoundingBoxes": [[[-90.0, -180.0], [90.0, 180.0]]],
                "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
            }

            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                print("✅ ¡Túnel AIS abierto! Recibiendo datos...")
                await websocket.send(json.dumps(suscripcion))
                
                while True:
                    mensaje = await websocket.recv()
                    datos   = json.loads(mensaje)
                    
                    if "error" in datos:
                        print(f"❌ Error de Aisstream: {datos['error']}")
                        await asyncio.sleep(10)
                        break

                    tipo = datos.get("MessageType")
                    mmsi = datos.get("MetaData", {}).get("MMSI")
                    if not mmsi or mmsi not in barcos_conocidos:
                        continue
                    barco = barcos_conocidos[mmsi]

                    # ── POSICIÓN ──────────────────────────────────────────
                    if tipo == "PositionReport":
                        rep   = datos["Message"]["PositionReport"]
                        lat   = rep["Latitude"]
                        lon   = rep["Longitude"]
                        rumbo = rep["TrueHeading"] if rep["TrueHeading"] != 511 else 0
                        ahora     = datetime.now(timezone.utc)
                        ahora_iso = ahora.isoformat()

                        # Actualizar posición en buques
                        requests.patch(
                            f"{SUPABASE_URL}/rest/v1/buques?id=eq.{barco['id']}",
                            headers=HEADERS,
                            json={"latitud": lat, "longitud": lon,
                                  "rumbo": rumbo, "ultima_senal": ahora_iso},
                            timeout=5
                        )
                        print(f"🎯 [AIS] {barco['nombre']} → {lat:.4f}, {lon:.4f}")

                        # ── HISTORIAL (persiste entre reinicios de Render) ──
                        buque_id = barco["id"]
                        if debe_guardar_historial(buque_id, ahora):
                            resp = requests.post(
                                f"{SUPABASE_URL}/rest/v1/historial_posiciones",
                                headers=HEADERS,
                                json={"buque_id": buque_id, "latitud": lat,
                                      "longitud": lon, "rumbo": rumbo,
                                      "timestamp": ahora_iso},
                                timeout=5
                            )
                            if resp.status_code in (200, 201):
                                ultimo_guardado_historial[buque_id] = ahora
                                print(f"📍 [HISTORIAL] {barco['nombre']} guardado.")
                            else:
                                print(f"⚠️ Error guardando historial: {resp.status_code} {resp.text[:100]}")

                    # ── DATOS ESTÁTICOS (destino declarado) ───────────────
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
                            headers=HEADERS, json=payload, timeout=5
                        )
                        print(f"🧭 [DESTINO] {barco['nombre']} → {destino} | ETA: {eta_iso}")

        except Exception as e:
            print(f"⚠️ Reconectando en 5s... ({e})")
            await asyncio.sleep(5)

# ==========================================
# 5. ARRANQUE
# ==========================================
if __name__ == "__main__":
    hilo = threading.Thread(target=servidor_web_fantasma)
    hilo.daemon = True
    hilo.start()
    asyncio.run(radar_global_ais())
