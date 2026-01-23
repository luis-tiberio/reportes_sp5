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
# CONFIGURAÇÕES GERAIS
# ==============================================================================

DOWNLOAD_DIR = "/tmp"
SCREENSHOT_PATH = "looker_evidence.png"
SCREENSHOT_PATH_EXTRA = "looker_evidence_extra.png" 

# Fuso Horário (Brasília UTC-3)
FUSO_BR = timezone(timedelta(hours=-3))

# IDs das Planilhas
ID_PLANILHA_DADOS = "1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU"
# ID_PLANILHA_INBOUND = "1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU"
ID_PLANILHA_DESTINO_SCRIPT = "1lTL4DVBHPfG9OaSO_ePDsP0hWEm_tCnyNd4UqeVzLFI"

# URLs do Looker (Turnos)
REPORT_URL_T1 = "https://lookerstudio.google.com/s/jrComoFYUHY"   # 06h–14h
REPORT_URL_T2 = "https://lookerstudio.google.com/s/sS1xru1_0LU"   # 14h–21h
REPORT_URL_T3 = "https://lookerstudio.google.com/s/nps1V7Dtudo"   # demais horários

# Webhook Principal
WEBHOOK_URL = os.environ.get("https://openapi.seatalk.io/webhook/group/ATSiL-5DRiGnHdV0t2XLlg")

# --- CONFIGURAÇÃO DO SEGUNDO PRINT (EXTRA) ---
REPORT_URL_EXTRA = "https://lookerstudio.google.com/s/pg9Ho6yKSdk"
WEBHOOK_URL_EXTRA = os.environ.get("https://openapi.seatalk.io/webhook/group/ATSiL-5DRiGnHdV0t2XLlg")
# ---------------------------------------------

# Mapa de Colunas (Lógica das Horas)
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
# FUNÇÕES AUXILIARES
# ==============================================================================

def get_creds():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    return ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)

def rename_downloaded_file(download_dir, download_path, prefix):
    try:
        current_hour = datetime.now(FUSO_BR).strftime("%H")
        new_file_name = f"{prefix}-{current_hour}.csv"
        new_file_path = os.path.join(download_dir, new_file_name)
        if os.path.exists(new_file_path):
            os.remove(new_file_path)
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

# --- FUNÇÃO DE LIMPEZA ---
def limpar_base_se_necessario():
    now = datetime.now(FUSO_BR)
    # Roda somente às 06h, entre os minutos 12 e 16
    if now.hour == 6 and 12 <= now.minute <= 16:
        print(f"🧹 Horário de limpeza detectado ({now.strftime('%H:%M')}). Iniciando limpeza da Base Script...")
        try:
            client = gspread.authorize(get_creds())
            spreadsheet = client.open_by_key(ID_PLANILHA_DESTINO_SCRIPT)
            ws_destino = spreadsheet.worksheet('Base Script')
            ws_destino.batch_clear(["C2:AX"]) 
            print("✅ 'Base Script' (C2:AX) limpa com sucesso!")
            time.sleep(2)
        except Exception as e:
            print(f"❌ Erro ao limpar a base: {e}")
# ----------------------------------------------

def executar_logica_hora_local(horas_para_executar):
    print("\n--- Iniciando manipulação de colunas (Lógica Local) ---")
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

def enviar_webhook_generico(mensagem, url_webhook):
    try:
        print(f"Disparando Webhook Texto para: ...{url_webhook[-8:]}")
        requests.post(url_webhook, json={"tag": "text", "text": {"format": 1, "content": mensagem}})
    except Exception as e: print(f"Erro webhook texto: {e}")

def enviar_imagem_generico(caminho_imagem, url_webhook):
    try:
        print(f"Disparando Webhook Imagem para: ...{url_webhook[-8:]}")
        with open(caminho_imagem, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode('utf-8')
        requests.post(url_webhook, json={"tag": "image", "image_base64": {"content": img_b64}})
        print(f"Imagem enviada com sucesso!")
    except Exception as e: print(f"Erro webhook imagem: {e}")

def smart_crop_padded(image_path):
    try:
        im = Image.open(image_path)
        bg = Image.new(im.mode, im.size, im.getpixel((10, 10)))
        diff = ImageChops.difference(im, bg)
        diff = ImageChops.add(diff, diff, 2.0, 0)
        bbox = diff.getbbox()
        if bbox:
            left, top, right, bottom = bbox
            final_box = (max(0, left-20), top, min(im.width, right+20), min(im.height, bottom+50))
            im.crop(final_box).save(image_path)
            print("Recorte inteligente aplicado.")
    except Exception as e: print(f"Erro no crop: {e}")

def escolher_report_por_turno():
    now = datetime.now(FUSO_BR)
    minutos_do_dia = now.hour * 60 + now.minute
    
    if 6 * 60 + 16 <= minutos_do_dia <= 14 * 60 + 15:
        return REPORT_URL_T1, "T1 (06:00–13:59)"
    elif 14 * 60 + 6 <= minutos_do_dia <= 22 * 60 + 15:
        return REPORT_URL_T2, "T2 (14:00–21:59)"
    else:
        return REPORT_URL_T3, "T3 (22:00–05:59)"

# ==============================================================================
# FUNÇÃO PARA CAPTURAR EVIDÊNCIA (GENÉRICA)
# ==============================================================================

async def capturar_looker(url_report, path_salvar, auth_json):
    print(f"Acessando Looker: {url_report}")
    
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True)
            # Viewport fixo e alto, mas não exagerado para não quebrar o layout
            context = await browser.new_context(
                storage_state=json.loads(auth_json),
                viewport={'width': 1920, 'height': 1200} 
            )
            page = await context.new_page()
            page.set_default_timeout(100000)

            await page.goto(url_report)
            await page.wait_for_load_state("networkidle") # Espera a rede acalmar
            await asyncio.sleep(15) # Espera inicial rigorosa

            # --- Lógica de Refresh / Modo Leitura ---
            try:
                # Tenta sair do modo edição se estiver nele
                edit_btn = page.get_by_role("button", name="Editar").or_(page.get_by_role("button", name="Edit"))
                if await edit_btn.count() > 0 and await edit_btn.first.is_visible():
                    await edit_btn.first.click()
                    await asyncio.sleep(10)
            except: pass

            # --- LIMPEZA VISUAL ---
            await page.evaluate("""() => {
                const selectors = ['header', '.ga-sidebar', '#align-lens-view', '.bottomContent', '.paginationPanel', 
                                   '.feature-content-header', '.lego-report-header', '.header-container', 
                                   'div[role="banner"]', '.page-navigation-panel'];
                selectors.forEach(sel => { 
                    document.querySelectorAll(sel).forEach(el => el.style.display = 'none'); 
                });
                document.body.style.backgroundColor = '#eeeeee';
            }""")

            # --- O SEGREDO: SCROLL GRADUAL (HUMANO) ---
            print("Iniciando scroll gradual para forçar renderização...")
            
            # Pega a altura total da página
            total_height = await page.evaluate("document.body.scrollHeight")
            viewport_height = 1000
            scrolled = 0
            
            # Desce de 500 em 500 pixels
            while scrolled < total_height:
                scrolled += 500
                await page.mouse.wheel(0, 500) # Simula a roda do mouse (melhor que scrollTo)
                await asyncio.sleep(1.5) # Espera 1.5s a cada descida para o gráfico "acordar"
            
            # Espera final lá embaixo para garantir os últimos gráficos
            await asyncio.sleep(3)
            
            # Volta para o topo (opcional, mas às vezes realinha o layout)
            # Se ao voltar para o topo os de baixo ficarem cinza de novo, comente as duas linhas abaixo
            await page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(2)

            # --- CAPTURA ---
            # Vamos tentar pegar o container específico primeiro
            container = page.locator("div.ng2-canvas-container").first
            if await container.count() == 0:
                 # Fallback para outros seletores comuns do Looker
                 container = page.locator(".lego-report-view").first

            if await container.count() > 0:
                print(f"Container encontrado. Salvando em {path_salvar}")
                try:
                    # O container.screenshot recorta exatamente a área do relatório
                    await container.screenshot(path=path_salvar)
                    used_container = True
                except:
                    # Se falhar, vai de full page
                    print("Falha no container, usando full page.")
                    await page.screenshot(path=path_salvar, full_page=True)
                    used_container = False
            else:
                print("Container não achado, usando full page.")
                await page.screenshot(path=path_salvar, full_page=True)
                used_container = False

            await browser.close()
            return True, used_container

        except Exception as e:
            print(f"❌ Erro ao capturar Looker ({url_report}): {e}")
            return False, False

async def gerar_e_enviar_evidencia_principal():
    print("\n--- Evidência Principal ---")
    auth_json = os.environ.get("LOOKER_COOKIES")
    if not auth_json:
        print("⚠️ LOOKER_COOKIES não encontrado.")
        return

    report_url, turno_label = escolher_report_por_turno()
    sucesso, used_container = await capturar_looker(report_url, SCREENSHOT_PATH, auth_json)
    
    if sucesso:
        if not used_container: smart_crop_padded(SCREENSHOT_PATH)
        enviar_webhook_generico(f"Segue reporte operacional:", WEBHOOK_URL)
        enviar_imagem_generico(SCREENSHOT_PATH, WEBHOOK_URL)

async def gerar_e_enviar_evidencia_extra():
    print("\n--- Evidência Extra (Segundo Print) ---")
    auth_json = os.environ.get("LOOKER_COOKIES")
    
    # Executa direto sem verificações de string
    sucesso, used_container = await capturar_looker(REPORT_URL_EXTRA, SCREENSHOT_PATH_EXTRA, auth_json)
    
    if sucesso:
        if not used_container: smart_crop_padded(SCREENSHOT_PATH_EXTRA)
        enviar_webhook_generico("Segue reporte operacional:", WEBHOOK_URL_EXTRA)
        enviar_imagem_generico(SCREENSHOT_PATH_EXTRA, WEBHOOK_URL_EXTRA)


# ==============================================================================
# MAIN (ORQUESTRA TUDO)
# ==============================================================================

async def main():       
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    final_path1 = None
    final_path2 = None
    # final_path3 = None
    
    print(">>> FASE 1: ATUALIZAÇÃO DE DADOS <<<")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"])
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            print("🔑 Login Shopee...")
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=30000)
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill('Ops10919')
            await page.locator('xpath=//*[@placeholder="Senha"]').fill('@Shopee1234')
            await page.locator('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button').click()
            await page.wait_for_timeout(10000)
            
            try: await page.locator('.ssc-dialog-close').click(timeout=5000)
            except: pass

            print("⏳ Verificando horário seguro (0-2 min)...")
            while True:
                if datetime.now(FUSO_BR).minute <= 2:
                    print("🛑 Aguardando virada do horário seguro (30s)...")
                    time.sleep(30)
                else:
                    break

            # DOWNLOAD 1
            print("Baixando Produtividade...")
            await page.goto("https://spx.shopee.com.br/#/dashboard/toProductivity?page_type=Outbound")
            
            export_btn_xpath = "//button[contains(normalize-space(),'Exportar')]"
            try:
                await page.wait_for_selector(f"xpath={export_btn_xpath}", state="visible", timeout=60000)
                await page.locator(f"xpath={export_btn_xpath}").click()
            except Exception as e:
                print(f"⚠️ Erro ao clicar em Exportar (1): {e}")
                raise e

            await page.wait_for_timeout(5000)
            await page.locator("div").filter(has_text=re.compile("^Exportar$")).click()
            
            async with page.expect_download() as dl_info:
                await page.get_by_role("button", name="Baixar").nth(0).click()
            file1 = await dl_info.value
            path1 = os.path.join(DOWNLOAD_DIR, file1.suggested_filename)
            await file1.save_as(path1)
            final_path1 = rename_downloaded_file(DOWNLOAD_DIR, path1, "PROD")

            # DOWNLOAD 2
            print("Baixando WS Assignment...")
            await page.goto("https://spx.shopee.com.br/#/workstation-assignment")
            await page.wait_for_timeout(8000)
            await page.keyboard.press('Escape') 
            
            try:
                await page.locator('xpath=/html[1]/body[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[3]/span[1]').click()
                await page.wait_for_timeout(2000)
                d1 = (datetime.now(FUSO_BR) - timedelta(days=1)).strftime("%Y/%m/%d")
                date_input = page.get_by_role("textbox", name="Escolha a data de início").nth(0)
                await date_input.click(force=True)
                await date_input.fill(d1)
                await page.locator('xpath=/html[1]/body[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[6]/form[1]/div[4]/button[1]').click()
                await page.locator('xpath=/html[1]/body[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[8]/div[1]/button[1]').click()
                await page.wait_for_timeout(5000)
            except: print("Erro na navegação do DL 2")

            async with page.expect_download() as dl_info:
                await page.locator('xpath=/html/body/span/div/div[1]/div/span/div/div[2]/div[2]/div[1]/div/div[1]/div/div[1]/div[2]/button').click()
            file2 = await dl_info.value
            path2 = os.path.join(DOWNLOAD_DIR, file2.suggested_filename)
            await file2.save_as(path2)
            final_path2 = rename_downloaded_file(DOWNLOAD_DIR, path2, "WS")
        
        except Exception as e:
            print(f"Erro no fluxo de download: {e}")
        finally:
            await browser.close()

    # UPLOAD E LÓGICA LOCAL
    if final_path1 and final_path2:
        update_sheet(final_path1, ID_PLANILHA_DADOS, "PROD")
        update_sheet(final_path2, ID_PLANILHA_DADOS, "WS T1")
        print("Sincronizando (20s)...")
        time.sleep(20)

        limpar_base_se_necessario()

        # Definição das horas
        now_br = datetime.now(FUSO_BR)
        horas = [now_br.hour]
        if now_br.minute <= 10:
            prev = now_br.hour - 1
            horas.insert(0, 23 if prev < 0 else prev)
        
        executar_logica_hora_local(horas)
    else:
        print("⚠️ Upload cancelado pois um ou mais arquivos não foram baixados.")

    print("\n>>> FASE 2: VERIFICAÇÃO DE EVIDÊNCIA <<<")
    
    now_check = datetime.now(FUSO_BR)
    minuto_atual = now_check.minute
    
    # JANELA DE EVIDÊNCIA: 7 a 13
    JANELA_INICIO = 1
    JANELA_FIM = 59
    
    if JANELA_INICIO <= minuto_atual <= JANELA_FIM:
        print(f"✅ Dentro da janela ({JANELA_INICIO}-{JANELA_FIM} min).")
        
        # 1. Print Principal
        await gerar_e_enviar_evidencia_principal()
        
        # Pausa para garantir que o navegador liberou recursos
        print("Aguardando 5s para iniciar segundo print...")
        time.sleep(5)
        
        # 2. Print Extra (AGORA COM TRY/CATCH para debug)
        try:
            await gerar_e_enviar_evidencia_extra()
        except Exception as e:
            print(f"❌ Erro fatal ao rodar Evidência Extra: {e}")
            
    else:
        print(f"🚫 Fora da janela de imagem ({minuto_atual} min). A imagem só é gerada entre {JANELA_INICIO} e {JANELA_FIM} da hora.")
        print("Script finalizado.")

if __name__ == "__main__":
    asyncio.run(main())
