from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from google.oauth2.service_account import Credentials
import pandas as pd
import gspread
import time
import datetime
import os
import shutil

# Caminho para o ChromeDriver e Chrome Canary
chrome_driver_path = 'C:/Python/atualizacao-hxh/driver-canary/chromedriver.exe'
chrome_binary_path = r'C:\Python\atualizacao-hxh\chrome-win64\chrome.exe'
download_dir = r'C:\Python\atualizacao-hxh\EXP'  # Diretório onde os arquivos serão salvos

# Configuração do ChromeDriver com diretório de download
chrome_prefs = {
    "download.default_directory": download_dir,  # Define o diretório padrão de download
    "download.prompt_for_download": False,       # Desativa o pop-up de confirmação
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "safebrowsing.disable_download_protection": True  # Evita bloqueios de segurança
}

# Configurações do ChromeDriver
options = Options()
options.binary_location = chrome_binary_path
options.add_argument('--incognito')
options.add_argument('--start-maximized')
options.add_argument('--no-sandbox')
options.add_argument('--disable-extensions')
options.add_argument('--window-size=3000x3000')
options.add_argument('--force-device-scale-factor=0.9')
#options.add_argument('--headless')  # Executa em modo headless (remova se quiser ver a interface)
options.add_experimental_option("prefs", chrome_prefs)

service = Service(executable_path=chrome_driver_path)
driver = webdriver.Chrome(service=service, options=options)
time.sleep(2)
driver.set_window_size(3000, 3000)

def login(driver):
    """Realiza o login no site Shopee."""
    driver.get("https://spx.shopee.com.br/")
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '//*[@placeholder="Ops ID"]')))
        username_elem = driver.find_element(By.XPATH, '//*[@placeholder="Ops ID"]')
        password_elem = driver.find_element(By.XPATH, '//*[@placeholder="Senha"]')
        username_elem.send_keys('Ops34139')
        password_elem.send_keys('@Shopee1234')
        login_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button'))
        )
        login_button.click()
        time.sleep(15)
        try:
            popup = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@class="ssc-dialog-close"]')))
            popup.click()
        except:
            print("Nenhum pop-up foi encontrado.")
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
    except Exception as e:
        print(f"Erro no login: {e}")
        driver.quit()
        raise

def get_data(driver):
    """Coleta os dados necessários e realiza o download."""
    try:
        driver.get("https://spx.shopee.com.br/#/staging-area-management/list/outbound")
        time.sleep(5)
        driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div/div/span/span/button').click()
        time.sleep(5)
        driver.find_element(By.XPATH, '/html[1]/body[1]/div[4]/ul[1]/li[1]/span[1]/div[1]/div[1]/span[1]').click()
        time.sleep(5)

        driver.get("https://spx.shopee.com.br/#/taskCenter/exportTaskCenter")
        time.sleep(10)
        driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div/div/div/div[1]/div[8]/div/div[1]/div/div[2]/div[1]/div[1]/div[2]/div/div/div/table/tbody[2]/tr[1]/td[7]/div/div/button/span').click()
        time.sleep(15)  # Aguarda o download ser concluído
        rename_downloaded_file(download_dir)
        
    except Exception as e:
        print(f"Erro ao coletar dados: {e}")
        driver.quit()
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
        csv_folder_path = r"C:\Python\atualizacao-hxh\EXP"
        csv_file_path = os.path.join(csv_folder_path, csv_file_name)

        # Verifica se o arquivo existe
        if not os.path.exists(csv_file_path):
            print(f"Arquivo {csv_file_path} não encontrado.")
            return
        
        # Configuração da API Google Sheets
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file("hxh.json", scopes=scope)
        client = gspread.authorize(creds)


        # --------- PRIMEIRO UPLOAD NA PLANILHA 1 ---------
        # Acessa a primeira planilha e aba 'Base SPX'
        sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1hoXYiyuArtbd2pxMECteTFSE75LdgvA2Vlb6gPpGJ-g/edit?gid=0#gid=0")
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


def main():
    try:
        login(driver)
        get_data(driver)
        update_packing_google_sheets()
        print("Dados atualizados com sucesso.")

    except Exception as e:
        print(f"Erro durante o processo: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
