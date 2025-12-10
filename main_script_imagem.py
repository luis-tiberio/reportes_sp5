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
WEBHOOK_URL_EXTRA = os.environ.get("WEBHOOK_URL_EXTRA") or "https://openapi.seatalk.io/webhook/group/6968RfmNTh-rKeNcNevEkg"

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
    # Roda somente às 06h, entre os minutos 12 e 16
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
# LÓGICA DE EVIDÊNCIA UNIFICADA (SESSÃO ÚNICA)
# ==============================================================================

async def processar_evidencias_unificado():
    print("\n>>> FASE 2: VERIFICAÇÃO DE EVIDÊNCIA (SESSÃO ÚNICA) <<<")
    
    auth_json = os.environ.get("LOOKER_COOKIES")
    if not auth_json:
        print("⚠️ LOOKER_COOKIES não encontrado. Pulando evidência.")
        return

    # 1. Definir lista de tarefas: (URL, CaminhoImagem, Webhook, Mensagem)
    url_princ, label_princ = escolher_report_por_turno()
    
    tarefas = [
        {
            "nome": "Principal",
            "url": url_princ,
            "path": SCREENSHOT_PATH,
            "webhook": WEBHOOK_URL_MAIN,
            "msg": f"Segue reporte operacional ({label_princ}):"
        },
        {
            "nome": "Extra",
            "url": REPORT_URL_EXTRA,
            "path": SCREENSHOT_PATH_EXTRA,
            "webhook": WEBHOOK_URL_EXTRA,
            "msg": "Segue reporte adicional:"
        }
    ]

    # 2. Abrir Navegador UMA VEZ
    async with async_playwright() as p:
        print("🚀 Iniciando navegador para capturas...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            storage_state=json.loads(auth_json),
            viewport={'width': 2200, 'height': 3000}
        )
        page = await context.new_page()
        page.set_default_timeout(120000) # Timeout generoso

        # 3. Iterar sobre as tarefas
        for tarefa in tarefas:
            print(f"\n--- Processando Evidência: {tarefa['nome']} ---")
            print(f"Acessando: {tarefa['url']}")
            
            try:
                await page.goto(tarefa['url'])
                await page.wait_for_load_state("domcontentloaded")
                print("Aguardando renderização (20s)...")
                await asyncio.sleep(20)

                # Tentativa de Refresh
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
                    try:
                        await container.scroll_into_view_if_needed()
                        await asyncio.sleep(1)
                        await container.screenshot(path=tarefa['path'])
                        used_container = True
                        print(f"Screenshot salvo: {tarefa['path']}")
                    except:
                        await page.screenshot(path=tarefa['path'], full_page=True)
                else:
                    await page.screenshot(path=tarefa['path'], full_page=True)
                
                # Crop (Se necessário)
                if not used_container:
                    smart_crop_padded(tarefa['path'])

                # Envio Webhook (Imediato)
                enviar_webhook_final(tarefa['msg'], tarefa['path'], tarefa['webhook'])

            except Exception as e:
                print(f"❌ FALHA ao processar {tarefa['nome']}: {e}")

        await browser.close()
        print("🏁 Sessão do navegador finalizada.")

# ==============================================================================
# MAIN
# ==============================================================================

async def main():       
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # Inicializa vars
    final_path1 = None
    final_path2 = None
    final_path3 = None
    
    print(">>> FASE 1: ATUALIZAÇÃO DE DADOS <<<")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"])
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            print("🔑 Login Shopee...")
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=40000)
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
                else: break

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

            # DOWNLOAD 3
            print("Baixando Produtividade 2...")
            await page.goto("https://spx.shopee.com.br/#/dashboard/toProductivity")
            btn3_xpath = '/html/body/div[1]/div/div[2]/div[2]/div/div/div/div[2]/div[1]/div/div[1]/div[2]/div[3]/span/span/span/button'
            await page.wait_for_selector(f"xpath={btn3_xpath}", state="visible", timeout=60000)
            await page.locator(f"xpath={btn3_xpath}").click()
            await page.wait_for_timeout(2000)
            await page.locator("div").filter(has_text=re.compile("^Exportar$")).click()
            async with page.expect_download() as dl_info:
                await page.locator('xpath=/html/body/span/div/div[1]/div/span/div/div[2]/div[2]/div[1]/div/div[1]/div/div[1]/div[2]/button').click()
            file3 = await dl_info.value
            path3 = os.path.join(DOWNLOAD_DIR, file3.suggested_filename)
            await file3.save_as(path3)
            final_path3 = rename_downloaded_file(DOWNLOAD_DIR, path3, "IN")

        except Exception as e:
            print(f"Erro no fluxo de download: {e}")
        finally:
            await browser.close()

    if final_path1 and final_path2 and final_path3:
        update_sheet(final_path1, ID_PLANILHA_DADOS, "PROD")
        update_sheet(final_path2, ID_PLANILHA_DADOS, "WS T1")
        update_sheet(final_path3, ID_PLANILHA_INBOUND, "INBOUND")
        print("Sincronizando (10s)...")
        time.sleep(10)

        limpar_base_se_necessario()

        now_br = datetime.now(FUSO_BR)
        horas = [now_br.hour]
        if now_br.minute <= 10:
            prev = now_br.hour - 1
            horas.insert(0, 23 if prev < 0 else prev)
        executar_logica_hora_local(horas)
    else:
        print("⚠️ Upload cancelado pois um ou mais arquivos não foram baixados.")

    # --- VERIFICAÇÃO DE EVIDÊNCIA (Sessão Única) ---
    now_check = datetime.now(FUSO_BR)
    minuto_atual = now_check.minute
    JANELA_INICIO = 7
    JANELA_FIM = 11
    
    if JANELA_INICIO <= minuto_atual <= JANELA_FIM:
        print(f"✅ Dentro da janela ({JANELA_INICIO}-{JANELA_FIM} min). Processando evidências...")
        await processar_evidencias_unificado()
    else:
        print(f"🚫 Fora da janela de imagem ({minuto_atual} min).")

if __name__ == "__main__":
    asyncio.run(main())
