
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

# Fuso Horário (Brasília UTC-3)
FUSO_BR = timezone(timedelta(hours=-3))

# IDs das Planilhas
ID_PLANILHA_DADOS = "1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU"
ID_PLANILHA_INBOUND = "1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU"
ID_PLANILHA_DESTINO_SCRIPT = "1lTL4DVBHPfG9OaSO_ePDsP0hWEm_tCnyNd4UqeVzLFI"

# URLs do Looker (Turnos)
REPORT_URL_T1 = "https://lookerstudio.google.com/s/jrComoFYUHY"   # 06h–14h
REPORT_URL_T2 = "https://lookerstudio.google.com/s/sS1xru1_0LU"   # 14h–21h
REPORT_URL_T3 = "https://lookerstudio.google.com/s/nps1V7Dtudo"   # demais horários

# URLs do Looker (Extra)
REPORT_URL_EXTRA = "https://lookerstudio.google.com/s/pg9Ho6yKSdk"

# Webhooks
WEBHOOK_URL_MAIN = os.environ.get("WEBHOOK_URL") or "https://openapi.seatalk.io/webhook/group/ks-dZEaLQt-1xCOAp54hLQ"
WEBHOOK_URL_EXTRA = "https://openapi.seatalk.io/webhook/group/6968RfmNTh-rKeNcNevEkg"

# Mapa de Colunas
MAPA_HORAS = {
    6:  {'cols': [('F', 'D'), ('D', 'C')], 'label': ('C1', 'Setor 6H')},
    7:  {'cols': [('G', 'F'), ('D', 'E')], 'label': ('E1', 'Setor 7H')},
    8:  {'cols': [('H', 'H'), ('D', 'G')], 'label': ('G1', 'Setor 8H')},
    9:  {'cols': [('I', 'J'), ('D', 'I')], 'label': ('I1', 'Setor 9H')},
    10: {'cols': [('J', 'L'), ('D', 'K')], 'label': ('K1', 'Setor 10H')},
    11: {'cols': [('K', 'N'), ('D', 'M')], 'label': ('M1', 'Setor 11H')},
    12: {'cols': [('L', 'P'), ('D', 'O')], 'label': ('O1', 'Setor 12H')},
    13: {'cols': [('M', 'R'), ('D', 'Q')], 'label': ('Q1', 'Setor 13H')},
    14: {'cols': [('N', 'T'), ('D', 'S')], 'label': ('S1', 'Setor 14H')},
    15: {'cols': [('O', 'V'), ('D', 'U')], 'label': ('U1', 'Setor 15H')},
    16: {'cols': [('P', 'X'), ('D', 'W')], 'label': ('W1', 'Setor 16H')},
    17: {'cols': [('Q', 'Z'), ('D', 'Y')], 'label': ('Y1', 'Setor 17H')},
    18: {'cols': [('R', 'AB'), ('D', 'AA')], 'label': ('AA1', 'Setor 18H')},
    19: {'cols': [('S', 'AD'), ('D', 'AC')], 'label': ('AC1', 'Setor 19H')},
    20: {'cols': [('T', 'AF'), ('D', 'AE')], 'label': ('AE1', 'Setor 20H')},
    21: {'cols': [('U', 'AH'), ('D', 'AG')], 'label': ('AG1', 'Setor 21H')},
    22: {'cols': [('V', 'AJ'), ('D', 'AI')], 'label': ('AI1', 'Setor 22H')},
    23: {'cols': [('W', 'AL'), ('D', 'AK')], 'label': ('AK1', 'Setor 23H')},
    0:  {'cols': [('X', 'AN'), ('D', 'AM')], 'label': ('AM1', 'Setor 00H')},
    1:  {'cols': [('Y', 'AP'), ('D', 'AO')], 'label': ('AO1', 'Setor 01H')},
    2:  {'cols': [('Z', 'AR'), ('D', 'AQ')], 'label': ('AQ1', 'Setor 02H')},
    3:  {'cols': [('AA', 'AT'), ('D', 'AS')], 'label': ('AS1', 'Setor 03H')},
    4:  {'cols': [('AB', 'AV'), ('D', 'AU')], 'label': ('AU1', 'Setor 04H')},
    5:  {'cols': [('AC', 'AX'), ('D', 'AW')], 'label': ('AW1', 'Setor 05H')},
}

# ==============================================================================
# FUNÇÕES DE SUPORTE
# ==============================================================================

def get_creds():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    return ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)

def rename_downloaded_file(download_dir, download_path, prefix):
    try:
        current_hour = datetime.now(FUSO_BR).strftime("%H")
        new_file_name = f"{prefix}-{current_hour}.csv"
        new_file_path = os.path.join(download_dir, new_file_name)
        if os.path.exists(new_file_path): os.remove(new_file_path)
        shutil.move(download_path, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
        return new_file_path
    except Exception as e:
        print(f"Erro ao renomear {prefix}: {e}")
        return None

def update_sheet(csv_path, sheet_id, tab_name):
    try:
        if not os.path.exists(csv_path): return
        client = gspread.authorize(get_creds())
        sheet = client.open_by_key(sheet_id)
        ws = sheet.worksheet(tab_name)
        df = pd.read_csv(csv_path).fillna("")
        ws.clear()
        ws.update(values=[df.columns.values.tolist()] + df.values.tolist())
        print(f"Upload OK: {tab_name}")
        time.sleep(2)
    except Exception as e:
        print(f"Erro no upload {tab_name}: {e}")

def limpar_base_se_necessario():
    now = datetime.now(FUSO_BR)
    if now.hour == 6 and 12 <= now.minute <= 16:
        print(f"🧹 Horário de limpeza detectado ({now.strftime('%H:%M')}).")
        try:
            client = gspread.authorize(get_creds())
            spreadsheet = client.open_by_key(ID_PLANILHA_DESTINO_SCRIPT)
            ws_destino = spreadsheet.worksheet('Base Script')
            ws_destino.batch_clear(["C2:AX"]) 
            print("✅ 'Base Script' (C2:AX) limpa com sucesso!")
            time.sleep(2)
        except Exception as e:
            print(f"❌ Erro ao limpar a base: {e}")

def executar_logica_hora_local(horas_para_executar):
    print("\n--- Iniciando manipulação de colunas ---")
    try:
        client = gspread.authorize(get_creds())
        spreadsheet = client.open_by_key(ID_PLANILHA_DESTINO_SCRIPT)
        ws_origem = spreadsheet.worksheet('Base Esteiras')
        ws_destino = spreadsheet.worksheet('Base Script')

        for hora in horas_para_executar:
            print(f"⚙️ Processando lógica da hora: {hora}H...")
            config = MAPA_HORAS.get(hora)
            if not config: continue

            for col_origem_letra, col_destino_letra in config['cols']:
                dados = ws_origem.get(f"{col_origem_letra}:{col_origem_letra}")
                ws_destino.update(values=dados, range_name=f"{col_destino_letra}1", value_input_option='USER_ENTERED')
                time.sleep(1)

            celula, texto = config['label']
            ws_destino.update_acell(celula, texto)
            print(f"   -> Label '{texto}' atualizado.")
            
        print("✅ Lógica local finalizada.")
    except Exception as e:
        print(f"❌ Erro na lógica local: {e}")

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
            final_box = (max(0, left-20), top, min(im.width, right+20), min(im.height, bottom+50))
            im.crop(final_box).save(image_path)
            print(f"Recorte inteligente aplicado em {image_path}.")
    except Exception as e: print(f"Erro no crop: {e}")

def enviar_webhook_final(mensagem, caminho_imagem, url_webhook):
    """Envia texto e imagem para o webhook especificado"""
    try:
        # Envia Texto
        print(f"📤 Enviando texto para Webhook: ...{url_webhook[-10:]}")
        res_txt = requests.post(url_webhook, json={"tag": "text", "text": {"format": 1, "content": mensagem}})
        print(f"✅ Status Envio Texto: {res_txt.status_code}")
        res_txt.raise_for_status()
        
        # Envia Imagem
        if os.path.exists(caminho_imagem):
            print(f"📤 Enviando imagem {caminho_imagem}...")
            with open(caminho_imagem, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode('utf-8')
            
            res_img = requests.post(url_webhook, json={"tag": "image", "image_base64": {"content": img_b64}})
            print(f"✅ Status Envio Imagem: {res_img.status_code}")
            if res_img.status_code != 200:
                print(f"⚠️ Resposta do servidor: {res_img.text}")
        else:
            print(f"❌ Arquivo de imagem não encontrado: {caminho_imagem}")

    except Exception as e:
        print(f"❌ Erro fatal no envio do Webhook: {e}")

# ==============================================================================
# LÓGICA DE GERAÇÃO DE EVIDÊNCIA (UNIFICADA)
# ==============================================================================

async def processar_evidencias_unificado():
    """Gera os dois screenshots em uma única sessão de navegador."""
    print("\n>>> FASE 2.1: GERAÇÃO UNIFICADA DE EVIDÊNCIAS <<<")
    
    auth_json = os.environ.get("LOOKER_COOKIES")
    if not auth_json:
        print("⚠️ LOOKER_COOKIES não encontrado. Pulando geração de evidência.")
        return False

    url_princ, label_princ = escolher_report_por_turno()
    
    tarefas = [
        {"nome": "Principal", "url": url_princ, "path": SCREENSHOT_PATH},
        {"nome": "Extra", "url": REPORT_URL_EXTRA, "path": SCREENSHOT_PATH_EXTRA}
    ]

    try:
        async with async_playwright() as p:
            print("🚀 Iniciando navegador para capturas...")
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                storage_state=json.loads(auth_json),
                viewport={'width': 2200, 'height': 3000}
            )
            page = await context.new_page()
            page.set_default_timeout(120000)

            for tarefa in tarefas:
                print(f"\n--- Gerando Screenshot: {tarefa['nome']} ---")
                
                await page.goto(tarefa['url'])
                await page.wait_for_load_state("domcontentloaded")
                print("Aguardando renderização (20s)...")
                await asyncio.sleep(20)

                # Tentativa de Refresh/Edição (para garantir dados atualizados)
                try:
                    edit_btn = page.get_by_role("button", name="Editar", exact=True).or_(page.get_by_role("button", name="Edit", exact=True))
                    if await edit_btn.count() > 0 and await edit_btn.first.is_visible():
                        await edit_btn.first.click()
                        await asyncio.sleep(10)
                        leitura_btn = page.get_by_role("button", name="Leitura").or_(page.get_by_text("Leitura")).or_(page.get_by_label("Modo de leitura"))
                        if await leitura_btn.count() > 0:
                            await leitura_btn.first.click()
                            await asyncio.sleep(10)
                except: pass

                # Limpeza CSS
                await page.evaluate("""() => {
                    const selectors = ['header', '.ga-sidebar', '#align-lens-view', '.bottomContent', '.paginationPanel', '.feature-content-header', '.lego-report-header', '.header-container', 'div[role="banner"]', '.page-navigation-panel'];
                    selectors.forEach(sel => { document.querySelectorAll(sel).forEach(el => el.style.display = 'none'); });
                    document.body.style.backgroundColor = '#eeeeee';
                }""")
                await asyncio.sleep(3)

                # Screenshot
                used_container = False
                container = None
                for frame in page.frames:
                    cand = frame.locator("div.ng2-canvas-container.grid")
                    if await cand.count() > 0:
                        container = cand.first
                        break
                
                if container:
                    await container.scroll_into_view_if_needed()
                    await asyncio.sleep(1)
                    await container.screenshot(path=tarefa['path'])
                    used_container = True
                else:
                    await page.screenshot(path=tarefa['path'], full_page=True)
                
                print(f"✅ Screenshot {tarefa['nome']} salvo em: {tarefa['path']}")

                # Crop (Se necessário)
                if not used_container:
                    smart_crop_padded(tarefa['path'])

            await browser.close()
            print("🏁 Sessão do navegador finalizada.")
            return True
    except Exception as e:
        print(f"❌ FALHA na Geração Unificada de Evidências: {e}")
        return False

# ==============================================================================
# LÓGICA DE ENVIO DE EVIDÊNCIA (SEPARADA)
# ==============================================================================

async def enviar_evidencia_principal_async():
    """Envia o primeiro print pelo webhook principal."""
    url_princ, label_princ = escolher_report_por_turno()
    msg = f"Segue reporte operacional ({label_princ}):"
    print("\n--- INICIANDO ENVIO: PRINCIPAL ---")
    enviar_webhook_final(msg, SCREENSHOT_PATH, WEBHOOK_URL_MAIN)

async def enviar_evidencia_extra_async():
    """Envia o segundo print pelo webhook extra, após um delay."""
    msg = "Segue reporte adicional:"
    print("\n--- INICIANDO ENVIO: EXTRA ---")
    enviar_webhook_final(msg, SCREENSHOT_PATH_EXTRA, WEBHOOK_URL_EXTRA)


# ==============================================================================
# MAIN (ORQUESTRA TUDO)
# ==============================================================================

async def main():       
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    final_path1 = final_path2 = final_path3 = None
    
    print(f"Tempo atual: {datetime.now(FUSO_BR).strftime('%H:%M:%S')}")
    print(">>> FASE 1: ATUALIZAÇÃO DE DADOS (Downloads e Uploads) <<<")
    # ... (Fluxo de download e upload permanece aqui) ...
    # (Removi para brevidade, mas o código completo mantém a FASE 1 intacta)
    
    # --- SIMULAÇÃO DA FASE 1 PARA TESTE ---
    # Coloquei essa parte para garantir que a lógica de Evidência rode,
    # mas o código completo no seu GitHub deve ter a FASE 1 completa aqui.
    try:
        # Apenas para testar a FASE 2, assumindo que FASE 1 foi OK
        # O código completo deve ter o fluxo real de FASE 1 aqui
        print("Executando a lógica de Lixeira e Lógica Local...")
        limpar_base_se_necessario()
        now_br = datetime.now(FUSO_BR)
        horas = [now_br.hour]
        if now_br.minute <= 10:
            prev = now_br.hour - 1
            horas.insert(0, 23 if prev < 0 else prev)
        executar_logica_hora_local(horas)
        print("Fase 1 (dados e lógica) concluída.")
        fase1_sucesso = True
    except Exception as e:
        print(f"❌ Erro na FASE 1 (dados): {e}")
        fase1_sucesso = False
    # ----------------------------------------


    # --- FASE 2: VERIFICAÇÃO E ENVIO DE EVIDÊNCIAS (SEPARADO) ---
    if fase1_sucesso:
        now_check = datetime.now(FUSO_BR)
        minuto_atual = now_check.minute
        JANELA_INICIO = 7
        JANELA_FIM = 13
        
        if JANELA_INICIO <= minuto_atual <= JANELA_FIM:
            print(f"✅ Dentro da janela de imagem ({JANELA_INICIO}-{JANELA_FIM} min).")
            
            # 2.1: Geração dos dois arquivos (Sessão Única)
            sucesso_geracao = await processar_evidencias_unificado()
            
            if sucesso_geracao:
                print("\n>>> FASE 2.2: ENVIOS SEPARADOS <<<")
                
                # 2.2a: Envia Principal
                await enviar_evidencia_principal_async()
                
                # 2.2b: DELAY E ENVIO EXTRA
                print(f"\n--- PAUSA DE 5 SEGUNDOS PARA EVITAR CONFLITO DE WEBHOOK ---")
                await asyncio.sleep(5)
                
                await enviar_evidencia_extra_async()
                
            else:
                print("⚠️ Envio cancelado devido à falha na Geração das Evidências.")
        else:
            print(f"🚫 Fora da janela de imagem ({minuto_atual} min).")

if __name__ == "__main__":
    # O seu código original deve ter o fluxo de download e upload completo aqui na FASE 1.
    # Certifique-se de que a FASE 1 está completa no seu arquivo de trabalho.
    asyncio.run(main())
