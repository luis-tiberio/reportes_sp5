import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import gspread
import datetime
import os
import shutil
from oauth2client.service_account import ServiceAccountCredentials

async def login(page):
    """Realiza o login no site Shopee."""
    await page.goto("https://spx.shopee.com.br/")
    try:
        await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=15000)
        await page.fill('xpath=//*[@placeholder="Ops ID"]', 'Ops34139')
        await page.fill('xpath=//*[@placeholder="Senha"]', '@Shopee1234')
        await page.click('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button')
        await page.wait_for_timeout(15000)
        try:
            await page.click('xpath=//*[@class="ssc-dialog-close"]', timeout=20000)
        except:
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
        await page.click('xpath=/html/body/div[1]/div/div[2]/div[2]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div/div/span/span/button')
        await page.wait_for_timeout(5000)
        await page.click('xpath=/html/body/div[4]/ul/li[1]/span/div/div/span')
        await page.wait_for_timeout(5000)

        await page.goto("https://spx.shopee.com.br/#/taskCenter/exportTaskCenter")
        await page.wait_for_timeout(10000)

        # Inicia o download
        async with page.expect_download() as download_info:
            await page.click('xpath=/html/body/div[1]/div/div[2]/div[2]/div/div/div/div[1]/div[8]/div/div[1]/div/div[2]/div[1]/div[1]/div[2]/div/div/div/table/tbody[2]/tr[1]/td[7]/div/div/button/span')
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
        # Busca o arquivo mais recente no diretório
        files = os.listdir(download_dir)
        files = [os.path.join(download_dir, f) for f in files if os.path.isfile(os.path.join(download_dir, f))]
        newest_file = max(files, key=os.path.getctime)

        # Cria o novo nome do arquivo com base na hora atual
        current_hour = datetime.datetime.now().strftime("%H")
        new_file_name = f"EXP-{current_hour}.csv"
        new_file_path = os.path.join(download_dir, new_file_name)

        # Remove o arquivo existente se necessário
        if os.path.exists(new_file_path):
            os.remove(new_file_path)

        # Renomeia o arquivo baixado
        shutil.move(newest_file, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")

    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")

async def update_packing_google_sheets():
    try:
        current_hour = datetime.datetime.now().strftime("%H")
        csv_file_name = f"EXP-{current_hour}.csv"
        csv_folder_path = "/tmp"  # Diretório temporário no Render
        csv_file_path = os.path.join(csv_folder_path, csv_file_name)

        if not os.path.exists(csv_file_path):
            print(f"Arquivo {csv_file_path} não encontrado.")
            return

        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name('/app/hxh.json', scope)  # Confirme o caminho do arquivo JSON
        client = gspread.authorize(creds)

        # Acessa a planilha e a aba
        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1hoXYiyuArtbd2pxMECteTFSE75LdgvA2Vlb6gPpGJ-g/edit?gid=0")
        worksheet1 = sheet1.worksheet("Base SPX")

        df = pd.read_csv(csv_file_path)
        df = df.fillna("")

        worksheet1.clear()
        worksheet1.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Arquivo {csv_file_name} enviado com sucesso para a aba 'Base SPX'.")

        await asyncio.sleep(5)

    except Exception as e:
        print(f"Erro durante o processo: {e}")

async def main():
    download_dir = "/tmp"  # Diretório temporário para downloads
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await login(page)
            await get_data(page, download_dir)
            await update_packing_google_sheets()
            print("Dados atualizados com sucesso.")
            await browser.close()
    except Exception as e:
        print(f"Erro durante o processo: {e}")

if __name__ == "__main__":
    asyncio.run(main())
