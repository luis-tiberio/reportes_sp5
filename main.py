import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
import datetime
import os
import shutil

async def login(page):
    """Realiza o login no site Shopee."""
    await page.goto("https://spx.shopee.com.br/")
    try:
        await page.wait_for_selector('input[placeholder="Ops ID"]', timeout=15000)
        await page.fill('input[placeholder="Ops ID"]', 'Ops34139')
        await page.fill('input[placeholder="Senha"]', '@Shopee1234')
        await page.click('button[type="submit"]')
        await page.wait_for_timeout(15000)
        try:
            await page.click('.ssc-dialog-close', timeout=20000)
        except Exception:
            print("Nenhum pop-up foi encontrado.")
            await page.keyboard.press("Escape")
    except Exception as e:
        print(f"Erro no login: {e}")
        raise

async def get_data(page, download_dir):
    """Coleta os dados necessários e realiza o download."""
    try:
        await page.goto("https://spx.shopee.com.br/#/staging-area-management/list/outbound")
        await page.wait_for_timeout(5000)
        await page.click('button:has-text("Filtrar")')  # Ajuste para seletor baseado no texto, se possível
        await page.wait_for_timeout(5000)
        await page.click('li[role="option"]:nth-child(1)')  # Ajuste o seletor conforme necessário
        await page.wait_for_timeout(5000)

        await page.goto("https://spx.shopee.com.br/#/taskCenter/exportTaskCenter")
        await page.wait_for_timeout(10000)

        # Inicia o download
        async with page.expect_download() as download_info:
            await page.click('button:has-text("Exportar")')  # Ajuste o seletor para o botão de exportar
        download = await download_info.value

        # Salva o arquivo no diretório de download
        file_path = os.path.join(download_dir, download.suggested_filename)
        await download.save_as(file_path)
        await page.wait_for_timeout(15000)  # Tempo para o download ser concluído
        rename_downloaded_file(download_dir)

    except Exception as e:
        print(f"Erro ao coletar dados: {e}")
        raise

def rename_downloaded_file(download_dir):
    try:
        files = os.listdir(download_dir)
        files = [os.path.join(download_dir, f) for f in files if os.path.isfile(os.path.join(download_dir, f))]
        newest_file = max(files, key=os.path.getctime)

        current_hour = datetime.datetime.now().strftime("%H")
        new_file_name = f"EXP-{current_hour}.csv"
        new_file_path = os.path.join(download_dir, new_file_name)

        if os.path.exists(new_file_path):
            os.remove(new_file_path)

        shutil.move(newest_file, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")

    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")

def update_packing_google_sheets():
    try:
        current_hour = datetime.datetime.now().strftime("%H")
        csv_file_name = f"EXP-{current_hour}.csv"
        csv_folder_path = "/tmp"  # Diretório temporário
        csv_file_path = os.path.join(csv_folder_path, csv_file_name)

        if not os.path.exists(csv_file_path):
            print(f"Arquivo {csv_file_path} não encontrado.")
            return

        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file('/app/hxh.json', scopes=scopes)
        client = gspread.authorize(creds)

        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1hoXYiyuArtbd2pxMECteTFSE75LdgvA2Vlb6gPpGJ-g/edit#gid=0")
        worksheet1 = sheet1.worksheet("Base SPX")

        df = pd.read_csv(csv_file_path)
        df = df.fillna("")

        worksheet1.clear()
        worksheet1.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo {csv_file_name} enviado com sucesso para a aba 'Base SPX'.")

        time.sleep(5)

    except Exception as e:
        print(f"Erro durante o processo: {e}")

async def main():
    download_dir = "/tmp"

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await login(page)
            await get_data(page, download_dir)
            update_packing_google_sheets()
            print("Dados atualizados com sucesso.")
            await browser.close()

    except Exception as e:
        print(f"Erro durante o processo: {e}")

if __name__ == "__main__":
    asyncio.run(main())
