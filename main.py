import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import gspread
import time
import datetime
import os
import shutil
import subprocess
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials

async def login(page):
    await page.goto("https://spx.shopee.com.br/")
    try:
        await page.wait_for_selector('input[placeholder="Ops ID"]', timeout=15000)
        await page.fill('input[placeholder="Ops ID"]', 'Ops35683')
        await page.fill('input[placeholder="Senha"]', '@Shopee123')
        await page.click('._tYDNB')
        await page.wait_for_timeout(15000)
        try:
            await page.click('.ssc-dialog-close', timeout=20000)
        except:
            print("Nenhum pop-up foi encontrado.")
            await page.keyboard.press("Escape")
    except Exception as e:
        print(f"Erro no login: {e}")
        raise
"""
async def get_data(page, download_dir):
    try:
        await page.goto("https://spx.shopee.com.br/#/staging-area-management/list/outbound")
        await page.wait_for_timeout(5000)
        await page.locator('//button[@type="button"]//span[contains(text(),"Export")]').click()
        await page.wait_for_timeout(5000)
        await page.wait_for_selector('(//span[contains(text(),"Export")])[2]', timeout=5000)
        await page.click('(//span[contains(text(),"Export")])[2]')
        await page.wait_for_timeout(5000)
    except Exception as e:
        print(f"Erro ao coletar dados: {e}")
        raise
"""
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
        csv_folder_path = "/tmp"
        csv_file_path = os.path.join(csv_folder_path, csv_file_name)
        if not os.path.exists(csv_file_path):
            print(f"Arquivo {csv_file_path} não encontrado.")
            return
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)
        client = gspread.authorize(creds)
        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1hoXYiyuArtbd2pxMECteTFSE75LdgvA2Vlb6gPpGJ-g/edit?gid=0#gid=0")
        worksheet1 = sheet1.worksheet("Base SPX")
        df = pd.read_csv(csv_file_path)
        df = df.fillna("")
        worksheet1.clear()
        worksheet1.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo {csv_file_name} enviado com sucesso para a aba 'EXP'.")
        time.sleep(5)
    except Exception as e:
        print(f"Erro durante o processo: {e}")

async def main():
    download_dir = "/tmp"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            #await login(page)
            #await get_data(page, download_dir)
            print("Chamando Selenium...")
            subprocess.run(["python", "download.py"])
            update_packing_google_sheets()
            print("Dados atualizados com sucesso.")
            await browser.close()
    except Exception as e:
        print(f"Erro durante o processo: {e}")

if __name__ == "__main__":
    asyncio.run(main())
