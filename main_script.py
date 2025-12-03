import asyncio
from playwright.async_api import async_playwright
from datetime import datetime, timedelta, timezone
import time
import os
import shutil
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import re

DOWNLOAD_DIR = "/tmp"

# --- CONFIGURAÇÃO DE FUSO HORÁRIO (BRASÍLIA UTC-3) ---
FUSO_BR = timezone(timedelta(hours=-3))

# --- CONFIGURAÇÃO DAS PLANILHAS ---
ID_PLANILHA_DADOS = "1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU"
ID_PLANILHA_INBOUND = "1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU"
ID_PLANILHA_DESTINO_SCRIPT = "1lTL4DVBHPfG9OaSO_ePDsP0hWEm_tCnyNd4UqeVzLFI"

# --- MAPA DE CÓPIA DAS HORAS ---
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

# --- FUNÇÕES DE CREDENCIAIS ---
def get_creds():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets", 
        "https://www.googleapis.com/auth/drive"
    ]
    return ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)

# --- FUNÇÕES DE RENOMEAR ---
def rename_downloaded_file(download_dir, download_path):
    try:
        current_hour = datetime.now(FUSO_BR).strftime("%H")
        new_file_name = f"PROD-{current_hour}.csv"
        new_file_path = os.path.join(download_dir, new_file_name)
        if os.path.exists(new_file_path):
            os.remove(new_file_path)
        shutil.move(download_path, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
        return new_file_path
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        return None
    
def rename_downloaded_file2(download_dir, download_path2):
    try:
        current_hour = datetime.now(FUSO_BR).strftime("%H")
        new_file_name2 = f"WS-{current_hour}.csv"
        new_file_path2 = os.path.join(download_dir, new_file_name2)
        if os.path.exists(new_file_path2):
            os.remove(new_file_path2)
        shutil.move(download_path2, new_file_path2)
        print(f"Arquivo salvo como: {new_file_path2}")
        return new_file_path2
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        return None

def rename_downloaded_file3(download_dir, download_path3):
    try:
        current_hour = datetime.now(FUSO_BR).strftime("%H")
        new_file_name3 = f"IN-{current_hour}.csv"
        new_file_path3 = os.path.join(download_dir, new_file_name3)
        if os.path.exists(new_file_path3):
            os.remove(new_file_path3)
        shutil.move(download_path3, new_file_path3)
        print(f"Arquivo salvo como: {new_file_path3}")
        return new_file_path3
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        return None

# --- FUNÇÕES DE UPLOAD ---
def update_packing_google_sheets(csv_file_path):
    try:
        if not os.path.exists(csv_file_path): return
        client = gspread.authorize(get_creds())
        sheet1 = client.open_by_key(ID_PLANILHA_DADOS)
        worksheet1 = sheet1.worksheet("PROD")
        df = pd.read_csv(csv_file_path).fillna("")
        worksheet1.clear()
        worksheet1.update(values=[df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo enviado com sucesso para a aba 'PROD'.")
        time.sleep(2)
    except Exception as e:
        print(f"Erro PROD: {e}")

def update_packing_google_sheets2(csv_file_path2):
    try:
        if not os.path.exists(csv_file_path2): return
        client = gspread.authorize(get_creds())
        sheet1 = client.open_by_key(ID_PLANILHA_DADOS)
        worksheet1 = sheet1.worksheet("WS T1")
        df = pd.read_csv(csv_file_path2).fillna("")
        worksheet1.clear()
        worksheet1.update(values=[df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo enviado com sucesso para a aba 'WS T1'.")
        time.sleep(2)
    except Exception as e:
        print(f"Erro WS T1: {e}")

def update_packing_google_sheets3(csv_file_path3):
    try:
        if not os.path.exists(csv_file_path3): return
        client = gspread.authorize(get_creds())
        sheet1 = client.open_by_key(ID_PLANILHA_INBOUND)
        worksheet1 = sheet1.worksheet("INBOUND")
        df = pd.read_csv(csv_file_path3).fillna("")
        worksheet1.clear()
        worksheet1.update(values=[df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo enviado com sucesso para a aba 'INBOUND'.")
        time.sleep(2)
    except Exception as e:
        print(f"Erro INBOUND: {e}")

# --- LÓGICA LOCAL PYTHON ---
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
            if not config:
                print(f"⚠️ Nenhuma configuração mapeada para {hora}H. Pulando.")
                continue

            for col_origem_letra, col_destino_letra in config['cols']:
                dados_coluna = ws_origem.get(f"{col_origem_letra}:{col_origem_letra}")
                ws_destino.update(
                    values=dados_coluna,
                    range_name=f"{col_destino_letra}1",
                    value_input_option='USER_ENTERED'
                )
                print(f"   -> Copiado {col_origem_letra} para {col_destino_letra}")
                time.sleep(1) 

            celula, texto = config['label']
            ws_destino.update_acell(celula, texto)
            print(f"   -> Label '{texto}' atualizado em {celula}")
            
        print("✅ Lógica local finalizada com sucesso.")

    except Exception as e:
        print(f"❌ Erro na execução da lógica local: {e}")

# --- MAIN ---
async def main():       
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"])
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            # 1. LOGIN
            print("🔑 Realizando Login...")
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=15000)
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill('Ops10919')
            await page.locator('xpath=//*[@placeholder="Senha"]').fill('@Shopee1234')
            await page.locator('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button').click()
            await page.wait_for_timeout(15000)
            try:
                await page.locator('.ssc-dialog-close').click(timeout=5000)
            except:
                print("Nenhum pop-up foi encontrado.")
                await page.keyboard.press("Escape")

            # =================================================================
            # 2. VERIFICAÇÃO DE HORÁRIO (ANTES DE BAIXAR)
            # =================================================================
            print("⏳ Verificando se o horário é seguro para Download (evitar 0-2min)...")
            while True:
                now_br = datetime.now(FUSO_BR)
                minute_check = now_br.minute
                
                if minute_check <= 2:
                    print(f"🛑 Horário de virada detectado ({now_br.strftime('%H:%M')}). Bases podem estar vazias. Aguardando 30s...")
                    time.sleep(30)
                else:
                    print(f"✅ Horário seguro ({now_br.strftime('%H:%M')}). Iniciando Downloads...")
                    break
            # =================================================================

            # 3. DOWNLOAD 1
            await page.goto("https://spx.shopee.com.br/#/dashboard/toProductivity?page_type=Outbound")
            await page.wait_for_timeout(10000)
            await page.locator("//button[contains(normalize-space(),'Exportar')]").click()
            await page.wait_for_timeout(10000)
            await page.locator("div").filter(has_text=re.compile("^Exportar$")).click()
            await page.wait_for_timeout(10000)

            async with page.expect_download() as download_info:
                await page.get_by_role("button", name="Baixar").nth(0).click()
            download = await download_info.value
            download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path)
            new_file_path = rename_downloaded_file(DOWNLOAD_DIR, download_path)

            # 4. DOWNLOAD 2
            await page.goto("https://spx.shopee.com.br/#/workstation-assignment")
            await page.wait_for_timeout(10000)
            await page.keyboard.press('Escape');
            await page.locator('xpath=/html[1]/body[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[3]/span[1]').click()
            await page.wait_for_timeout(10000)

            d1 = (datetime.now(FUSO_BR) - timedelta(days=1)).strftime("%Y/%m/%d")
            
            date_input = page.get_by_role("textbox", name="Escolha a data de início").nth(0)
            await date_input.wait_for(state="visible", timeout=10000)
            await date_input.click(force=True)
            await date_input.fill(d1)
            await page.wait_for_timeout(5000)
            await page.locator('xpath=/html[1]/body[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[6]/form[1]/div[4]/button[1]').click()
            
            await page.locator('xpath=/html[1]/body[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[8]/div[1]/button[1]').click()
            await page.wait_for_timeout(10000)

            async with page.expect_download() as download_info:
                await page.locator('xpath=/html/body/span/div/div[1]/div/span/div/div[2]/div[2]/div[1]/div/div[1]/div/div[1]/div[2]/button').click()
            download = await download_info.value
            download_path2 = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path2)
            new_file_path2 = rename_downloaded_file2(DOWNLOAD_DIR, download_path2)            

            # 5. DOWNLOAD 3
            await page.goto("https://spx.shopee.com.br/#/dashboard/toProductivity")
            await page.wait_for_timeout(10000)
            await page.locator('xpath=/html/body/div[1]/div/div[2]/div[2]/div/div/div/div[2]/div[1]/div/div[1]/div[2]/div[3]/span/span/span/button').click()
            await page.wait_for_timeout(10000)
            await page.locator("div").filter(has_text=re.compile("^Exportar$")).click()
            await page.wait_for_timeout(10000)

            async with page.expect_download() as download_info:
                await page.locator('xpath=/html/body/span/div/div[1]/div/span/div/div[2]/div[2]/div[1]/div/div[1]/div/div[1]/div[2]/button').click()
            download = await download_info.value
            download_path3 = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path3)
            new_file_path3 = rename_downloaded_file3(DOWNLOAD_DIR, download_path3)
            
            # 6. ATUALIZAR SHEETS E EXECUTAR LÓGICA LOCAL
            if new_file_path:
                update_packing_google_sheets(new_file_path)
                update_packing_google_sheets2(new_file_path2)
                update_packing_google_sheets3(new_file_path3)
                print("Dados atualizados com sucesso.")
                
                print("⏳ Aguardando 10 segundos para garantir a sincronização dos dados...")
                time.sleep(10)
                
                # Definição das horas a rodar (Baseado no horário BR já validado)
                now_br = datetime.now(FUSO_BR)
                current_hour = now_br.hour
                current_minute = now_br.minute
                
                horas_para_rodar = []
                
                # Como já esperamos o tempo seguro (min > 2), esta lógica
                # vai capturar o momento correto (entre min 3 e 10).
                if current_minute <= 10:
                    previous_hour = current_hour - 1
                    if previous_hour < 0: previous_hour = 23
                    horas_para_rodar.append(previous_hour)
                    print(f"🕒 Janela 10min ({current_minute}m): Incluída hora anterior ({previous_hour}H)")
                
                horas_para_rodar.append(current_hour)
                
                executar_logica_hora_local(horas_para_rodar)

        except Exception as e:
            print(f"Erro durante o processo: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
