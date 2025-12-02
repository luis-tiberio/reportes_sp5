import asyncio
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
import time
import os
import shutil
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import re

DOWNLOAD_DIR = "/tmp"

# --- CONFIGURAÇÃO DO SCRIPT REMOTO ---
# ID fornecido por você
SCRIPT_ID = "1j4iLZWev24xFRExP3zIvtuWuQhbT7g1jFkhSkVurPutBLpVeS7Jrb9yq"

# --- FUNÇÕES DE RENOMEAR ---
def rename_downloaded_file(download_dir, download_path):
    try:
        current_hour = datetime.now().strftime("%H")
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
        current_hour = datetime.now().strftime("%H")
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
        current_hour = datetime.now().strftime("%H")
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

# --- FUNÇÕES DE UPLOAD (ATUALIZADAS COM NOVOS ESCOPOS) ---
def get_scope_and_creds():
    """Retorna credenciais com permissão para Planilhas, Drive e Execução de Scripts"""
    scope = [
        "https://www.googleapis.com/auth/spreadsheets", 
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/script.projects" # Permissão vital para rodar o script
    ]
    return ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)

def update_packing_google_sheets(csv_file_path):
    try:
        if not os.path.exists(csv_file_path):
            print(f"Arquivo {csv_file_path} não encontrado.")
            return
        creds = get_scope_and_creds()
        client = gspread.authorize(creds)
        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU/edit?pli=1&gid=0#gid=0")
        worksheet1 = sheet1.worksheet("PROD")
        df = pd.read_csv(csv_file_path)
        df = df.fillna("")
        worksheet1.clear()
        worksheet1.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo enviado com sucesso para a aba 'PROD'.")
        time.sleep(5)
    except Exception as e:
        print(f"Erro durante o processo: {e}")

def update_packing_google_sheets2(csv_file_path2):
    try:
        if not os.path.exists(csv_file_path2):
            print(f"Arquivo {csv_file_path2} não encontrado.")
            return
        creds = get_scope_and_creds()
        client = gspread.authorize(creds)
        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU/edit?pli=1&gid=0#gid=0")
        worksheet1 = sheet1.worksheet("WS T1")
        df = pd.read_csv(csv_file_path2)
        df = df.fillna("")
        worksheet1.clear()
        worksheet1.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo enviado com sucesso para a aba 'WS T1'.")
        time.sleep(5)
    except Exception as e:
        print(f"Erro durante o processo: {e}")

def update_packing_google_sheets3(csv_file_path3):
    try:
        if not os.path.exists(csv_file_path3):
            print(f"Arquivo {csv_file_path3} não encontrado.")
            return
        creds = get_scope_and_creds()
        client = gspread.authorize(creds)
        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU/edit?gid=1554772832#gid=1554772832")
        worksheet1 = sheet1.worksheet("INBOUND")
        df = pd.read_csv(csv_file_path3)
        df = df.fillna("")
        worksheet1.clear()
        worksheet1.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo enviado com sucesso para a aba 'INBOUND'.")
        time.sleep(5)
    except Exception as e:
        print(f"Erro durante o processo: {e}")

# --- LÓGICA DE EXECUÇÃO DO SCRIPT REMOTO ---

def get_function_name_by_hour(hour):
    """
    Formata o nome da função conforme seu Apps Script:
    - 00h a 05h: _00H, _01H... (com zero à esquerda)
    - 06h a 23h: _6H, _14H... (sem zero à esquerda para unigit, normal para dois digitos)
    """
    if 0 <= hour <= 5:
        return f"_{hour:02d}H"
    else:
        return f"_{hour}H"

def execute_remote_script_logic():
    print("\n--- Iniciando execução de scripts remotos ---")

    try:
        # Usa as mesmas credenciais já carregadas com o escopo correto
        creds = get_scope_and_creds()
        service = build('script', 'v1', credentials=creds)

        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        
        functions_to_run = []

        # === Lógica da Janela de 10 minutos ===
        # Se for minuto 00 a 10 (ex: 09:05), roda a hora anterior (08H) E a atual (09H)
        if current_minute <= 10:
            previous_hour = current_hour - 1
            if previous_hour < 0:
                previous_hour = 23 # Trata virada do dia (00:05 roda _23H e _00H)
            
            prev_func = get_function_name_by_hour(previous_hour)
            functions_to_run.append(prev_func)
            print(f"🕒 Janela de 10min ({current_minute}m): Incluindo hora anterior ({prev_func}).")

        # Sempre roda a hora atual
        curr_func = get_function_name_by_hour(current_hour)
        functions_to_run.append(curr_func)

        # Executa cada função na lista
        for func_name in functions_to_run:
            print(f"🚀 Executando função remota: {func_name} ...")
            
            request = {
                "function": func_name,
                "devMode": True 
            }
            
            try:
                # Chama a API usando o SCRIPT_ID definido no topo
                response = service.scripts().run(scriptId=SCRIPT_ID, body=request).execute()
                
                if 'error' in response:
                    print(f"❌ Erro no script {func_name}: {response['error']['details']}")
                else:
                    print(f"✅ Sucesso: {func_name} finalizado.")
                    
            except Exception as api_error:
                print(f"❌ Falha de comunicação com a API para {func_name}: {api_error}")
                
    except Exception as e:
        print(f"Erro geral no executor de scripts: {e}")


# --- MAIN ---
async def main():       
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"])
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            # LOGIN
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

            
            # DOWNLOAD 1
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

            # DOWNLOAD 2
            await page.goto("https://spx.shopee.com.br/#/workstation-assignment")
            await page.wait_for_timeout(10000)
            await page.keyboard.press('Escape');
            await page.locator('xpath=/html[1]/body[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[3]/span[1]').click()
            await page.wait_for_timeout(10000)

            d1 = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
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

            # DOWNLOAD 3
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

            
            # ATUALIZAR GOOGLE SHEETS E EXECUTAR SCRIPT
            if new_file_path:
                # 1. Sobe os dados para a planilha original
                update_packing_google_sheets(new_file_path)
                update_packing_google_sheets2(new_file_path2)
                update_packing_google_sheets3(new_file_path3)
                print("Dados atualizados com sucesso.")
                
                # 2. Chama o script na OUTRA planilha (ID correto configurado)
                execute_remote_script_logic()

        except Exception as e:
            print(f"Erro durante o processo: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
