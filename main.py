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

def update_packing_google_sheets_prod():
    try:
        current_hour = datetime.datetime.now().strftime("%H")
        csv_file_name = f"Prod-{current_hour}.csv"
        csv_folder_path = "/tmp"
        csv_file_path = os.path.join(csv_folder_path, csv_file_name)
        if not os.path.exists(csv_file_path):
            print(f"Arquivo {csv_file_path} não encontrado.")
            return
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)
        client = gspread.authorize(creds)
        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU/edit?gid=13626729#gid=13626729")
        worksheet1 = sheet1.worksheet("PROD")
        df = pd.read_csv(csv_file_path)
        df = df.fillna("")
        worksheet1.clear()
        worksheet1.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo {csv_file_name} enviado com sucesso para a aba 'EXP'.")
        time.sleep(5)
    except Exception as e:
        print(f"Erro durante o processo: {e}")

def update_packing_google_sheets_ws():
    try:
        current_hour = datetime.datetime.now().strftime("%H")
        csv_file_name = f"WS-{current_hour}.csv"
        csv_folder_path = "/tmp"
        csv_file_path = os.path.join(csv_folder_path, csv_file_name)
        if not os.path.exists(csv_file_path):
            print(f"Arquivo {csv_file_path} não encontrado.")
            return
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)
        client = gspread.authorize(creds)
        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1uN6ILlmVgLc_Y7Tv3t0etliMwUAiZM1zC-jhXT3CsoU/edit?gid=13626729#gid=13626729")
        worksheet1 = sheet1.worksheet("WS T1")
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
    download_dir2 = download_dir
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            # await login(page)
            # await get_data(page, download_dir)

            print("Chamando Prod...")
            await asyncio.to_thread(subprocess.run, ["python", "download_prod.py"])
            await asyncio.to_thread(update_packing_google_sheets_prod)
            await asyncio.to_thread(update_packing_google_sheets_ws)

            """
            print("Chamando WS...")
            await asyncio.to_thread(subprocess.run, ["python", "download_ws.py"])
            await asyncio.to_thread(update_packing_google_sheets_ws)
            """

            print("Dados atualizados com sucesso.")
            await browser.close()
    except Exception as e:
        print(f"Erro durante o processo: {e}")

if __name__ == "__main__":
    asyncio.run(main())
