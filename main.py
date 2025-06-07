import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import gspread
import time
import datetime
import os
import shutil

# Remova as importações e configurações do Selenium

async def login(page):
    """Realiza o login no site Shopee."""
    await page.goto("https://spx.shopee.com.br/")
    try:
        await page.wait_for_selector('input[placeholder="Ops ID"]', timeout=15000)
        await page.fill('input[placeholder="Ops ID"]', 'Ops34139')
        await page.fill('input[placeholder="Senha"]', '@Shopee1234')
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

async def get_data(page, download_dir):
    """Coleta os dados necessários e realiza o download."""
    try:
        await page.goto("https://spx.shopee.com.br/#/staging-area-management/list/outbound")
        await page.wait_for_timeout(5000)
        #await page.locator('button:has-text("Export")').click()
        await page.locator('//button[@type="button"]//span[contains(text(),"Export")]').click()
        #await page.wait_for_selector('li.ssc-react-rc-menu-item.ssc-react-rc-menu-item-active.ssc-react-menu-item span.ssc-react-menu-icon span', timeout=5000)
        await page.wait_for_timeout(5000)
        #await page.click('li.ssc-react-rc-menu-item.ssc-react-rc-menu-item-active.ssc-react-menu-item span.ssc-react-menu-icon span')
        await page.locator('/html[1]/body[1]/div[3]/ul[1]/li[1]/span[1]/div[1]/div[1]/span[1]').click()
        await page.wait_for_timeout(5000)  # ou ajustar para o tempo de resposta esperado
        await page.goto("https://spx.shopee.com.br/#/taskCenter/exportTaskCenter")
        await page.wait_for_timeout(10000)

        # Inicia o download
        async with page.expect_download() as download_info:
            await page.click('tr[class="ssc-table-row ssc-table-row-highlighted"] td[class="ssc-table-body-column-fixed ssc-table-body-column-fixed-right-first"] div div[class="ssc-table-header-column-container"] button[type="button"] span span')
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


def update_packing_google_sheets():
    try:
        # Nome do arquivo CSV baseado na hora atual
        current_hour = datetime.datetime.now().strftime("%H")
        csv_file_name = f"EXP-{current_hour}.csv"
        csv_folder_path = "/tmp"  # Use /tmp no Render
        csv_file_path = os.path.join(csv_folder_path, csv_file_name)

        # Verifica se o arquivo existe
        if not os.path.exists(csv_file_path):
            print(f"Arquivo {csv_file_path} não encontrado.")
            return

        # Configuração da API Google Sheets
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('/app/hxh.json', scope)  # Caminho no Render
        client = gspread.authorize(creds)

        # --------- PRIMEIRO UPLOAD NA PLANILHA 1 ---------
        # Acessa a primeira planilha e aba 'Base SPX'
        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1nMLHR6Xp5xzQjlhwXufecG1INSQS4KrHn41kqjV9Rmk/edit?gid=0#gid=0")
        worksheet1 = sheet1.worksheet("Base SPX")  # Acessa a aba 'Base SPX'

        # Carregar os dados do CSV
        df = pd.read_csv(csv_file_path)

        # Substituir valores NaN por string vazia
        df = df.fillna("")  # Substitui NaN por ""

        # Substituir os dados da aba atual
        worksheet1.clear()  # Limpa a aba antes de atualizar os dados
        worksheet1.update([df.columns.values.tolist()] + df.values.tolist())  # Atualiza os dados
        print(f"Arquivo {csv_file_name} enviado com sucesso para a aba 'EXP'.")

        # Adicionando uma pausa de 5 segundos (ajuste o tempo conforme necessário)
        time.sleep(5)


    except Exception as e:
        print(f"Erro durante o processo: {e}")


async def main():
    # Defina o diretório de download
    download_dir = "/tmp"  # Diretório temporário no Render

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Rodar sem interface gráfica
            page = await browser.new_page()
            await login(page)
            await get_data(page, download_dir)
            update_packing_google_sheets()
            print("Dados atualizados com sucesso.")

            await browser.close()

    except Exception as e:
        print(f"Erro durante o processo: {e}")

# Execute a função main
if __name__ == "__main__":
    asyncio.run(main())
