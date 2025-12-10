import asyncio
import os
import shutil
import time
import re
import json
import base64
import requests
import gspread
import pandas as pd
from datetime import datetime, timedelta, timezone
from playwright.async_api import async_playwright
from oauth2client.service_account import ServiceAccountCredentials
from PIL import Image, ImageChops

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================

DOWNLOAD_DIR = "/tmp"
SCREENSHOT_PATH = "looker_evidence.png"
SCREENSHOT_PATH_EXTRA = "looker_evidence_extra.png"

FUSO_BR = timezone(timedelta(hours=-3))

ID_PLANILHA_DADOS = "1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU"
ID_PLANILHA_INBOUND = "1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU"
ID_PLANILHA_DESTINO_SCRIPT = "1lTL4DVBHPfG9OaSO_ePDsP0hWEm_tCnyNd4UqeVzLFI"

REPORT_URL_T1 = "https://lookerstudio.google.com/s/jrComoFYUHY"
REPORT_URL_T2 = "https://lookerstudio.google.com/s/sS1xru_0LU"
REPORT_URL_T3 = "https://lookerstudio.google.com/s/nps1V7Dtudo"
REPORT_URL_EXTRA = "https://lookerstudio.google.com/s/pg9Ho6yKSdk"

WEBHOOK_URL_MAIN = "https://openapi.seatalk.io/webhook/group/ATSiL-5DRiGnHdV0t2XLlg"
WEBHOOK_URL_EXTRA = "https://openapi.seatalk.io/webhook/group/ATSiL-5DRiGnHdV0t2XLlg"

# ==============================================================================
# FUNÇÕES DE ENVIO SEPARADAS (DEF SEPARADOS)
# ==============================================================================

def enviar_reporte_principal(mensagem, caminho_imagem):
    """Envia texto e imagem para o webhook PRINCIPAL"""
    url = WEBHOOK_URL_MAIN
    try:
        print(f"\n📤 [Webhook Principal] Enviando reporte...")
        # Texto
        requests.post(url, json={"tag": "text", "text": {"format": 1, "content": mensagem}})
        # Imagem
        if os.path.exists(caminho_imagem):
            with open(caminho_imagem, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode('utf-8')
            res = requests.post(url, json={"tag": "image", "image_base64": {"content": img_b64}})
            print(f"✅ Principal: Status {res.status_code}")
        else:
            print(f"❌ Erro: Imagem principal não encontrada.")
    except Exception as e:
        print(f"❌ Erro no Webhook Principal: {e}")

def enviar_reporte_extra(mensagem, caminho_imagem):
    """Envia texto e imagem para o webhook EXTRA"""
    url = WEBHOOK_URL_EXTRA
    try:
        print(f"\n📤 [Webhook Extra] Enviando reporte...")
        # Texto
        requests.post(url, json={"tag": "text", "text": {"format": 1, "content": mensagem}})
        # Imagem
        if os.path.exists(caminho_imagem):
            with open(caminho_imagem, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode('utf-8')
            res = requests.post(url, json={"tag": "image", "image_base64": {"content": img_b64}})
            print(f"✅ Extra: Status {res.status_code}")
        else:
            print(f"❌ Erro: Imagem extra não encontrada.")
    except Exception as e:
        print(f"❌ Erro no Webhook Extra: {e}")

# ==============================================================================
# LÓGICA DE CAPTURA (Navegador Aberto uma Única Vez)
# ==============================================================================

async def processar_evidencias_unificado():
    print("\n>>> INICIANDO CAPTURA DAS IMAGENS <<<")
    auth_json = os.environ.get("LOOKER_COOKIES")
    if not auth_json: return

    url_princ, label_princ = escolher_report_por_turno()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            storage_state=json.loads(auth_json),
            viewport={'width': 2200, 'height': 3000}
        )
        page = await context.new_page()

        # 1. CAPTURA PRINCIPAL
        print(f"Capturando Principal: {url_princ}")
        await page.goto(url_princ)
        await asyncio.sleep(20) # Carregamento
        
        # Faxina CSS
        await page.evaluate("""() => { document.querySelectorAll('header, .ga-sidebar, #align-lens-view, .bottomContent').forEach(el => el.style.display = 'none'); }""")
        
        container = await buscar_container(page)
        if container:
            await container.screenshot(path=SCREENSHOT_PATH)
        else:
            await page.screenshot(path=SCREENSHOT_PATH, full_page=True)
            smart_crop_padded(SCREENSHOT_PATH)
        
        # 2. CAPTURA EXTRA
        print(f"Capturando Extra: {REPORT_URL_EXTRA}")
        await page.goto(REPORT_URL_EXTRA)
        await asyncio.sleep(20)
        
        await page.evaluate("""() => { document.querySelectorAll('header, .ga-sidebar, #align-lens-view, .bottomContent').forEach(el => el.style.display = 'none'); }""")
        
        container = await buscar_container(page)
        if container:
            await container.screenshot(path=SCREENSHOT_PATH_EXTRA)
        else:
            await page.screenshot(path=SCREENSHOT_PATH_EXTRA, full_page=True)
            smart_crop_padded(SCREENSHOT_PATH_EXTRA)

        await browser.close()

    # --- AGORA OS ENVIOS SEPARADOS COM DELAY ---
    enviar_reporte_principal(f"Segue reporte operacional ({label_princ}):", SCREENSHOT_PATH)
    
    print("\n⏳ Aguardando 5 segundos entre envios de webhooks...")
    time.sleep(5)
    
    enviar_reporte_extra("Segue reporte adicional:", SCREENSHOT_PATH_EXTRA)

# ==============================================================================
# FUNÇÕES DE SUPORTE (MANTIDAS)
# ==============================================================================

async def buscar_container(page):
    for frame in page.frames:
        cand = frame.locator("div.ng2-canvas-container.grid")
        if await cand.count() > 0:
            return cand.first
    return None

def escolher_report_por_turno():
    now = datetime.now(FUSO_BR)
    minutos_do_dia = now.hour * 60 + now.minute
    if 6 * 60 + 16 <= minutos_do_dia <= 14 * 60 + 15:
        return REPORT_URL_T1, "T1 (06:00–13:59)"
    elif 14 * 60 + 6 <= minutos_do_dia <= 22 * 60 + 15:
        return REPORT_URL_T2, "T2 (14:00–21:59)"
    else:
        return REPORT_URL_T3, "T3 (22:00–05:59)"

def smart_crop_padded(image_path):
    try:
        if not os.path.exists(image_path): return
        im = Image.open(image_path)
        bg = Image.new(im.mode, im.size, im.getpixel((10, 10)))
        diff = ImageChops.difference(im, bg)
        diff = ImageChops.add(diff, diff, 2.0, 0)
        bbox = diff.getbbox()
        if bbox:
            left, top, right, bottom = bbox
            im.crop((max(0, left-20), top, min(im.width, right+20), min(im.height, bottom+50))).save(image_path)
    except: pass

# ==============================================================================
# MAIN (RODA DADOS + CHAMADA EVIDÊNCIA)
# ==============================================================================

async def main():
    # ... Parte 1 (Upload Shopee/Sheets) rodará normalmente aqui antes da Fase 2 ...
    # Assumindo que você manteve o código da main anterior aqui

    now_check = datetime.now(FUSO_BR)
    if 1 <= now_check.minute <= 59:
        await processar_evidencias_unificado()
    else:
        print(f"🚫 Fora da janela de imagem ({now_check.minute} min).")

if __name__ == "__main__":
    asyncio.run(main())
