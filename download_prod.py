from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import datetime
import os
import shutil

# Diretório de download para GitHub Actions
download_dir = "/tmp"

# Cria o diretório, se não existir
os.makedirs(download_dir, exist_ok=True)

# Configurações do Chrome para ambiente headless do GitHub Actions
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--enable-features=NetworkService,NetworkServiceInProcess")
chrome_options.add_argument("--disable-popup-blocking")
chrome_options.add_argument("--remote-debugging-port=9222")  # Útil para debug, se precisar

# Configurações de download
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)

# Inicializa o driver
driver = webdriver.Chrome(options=chrome_options)

def login(driver):
    driver.get("https://spx.shopee.com.br/")
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '//*[@placeholder="Ops ID"]')))
        driver.find_element(By.XPATH, '//*[@placeholder="Ops ID"]').send_keys('Ops89726')
        driver.find_element(By.XPATH, '//*[@placeholder="Senha"]').send_keys('@Shopee123')
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button'))
        ).click()

        time.sleep(15)
        try:
            popup = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "ssc-dialog-close"))
            )
            popup.click()
        except:
            print("Nenhum pop-up foi encontrado.")
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
    except Exception as e:
        print(f"Erro no login: {e}")
        driver.quit()
        raise

def get_data(driver):
    try:
        driver.get("https://spx.shopee.com.br/#/dashboard/toProductivity?page_type=Outbound")
        time.sleep(8)
        driver.find_element(By.XPATH, '/html[1]/body[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[3]/span[1]').click()
        time.sleep(10)
        #botao = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".ssc-button.batch-actions-btn.ssc-btn-type-primary")))
        #botao.click()
        driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div/div/div/div[2]/div[2]/div/div[1]/div[2]/div[3]/span/span/span/button').click()
        time.sleep(10)
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, '/html[1]/body[1]/span[1]/div[1]/div[1]/div[1]'))
        ).click()
        #driver.find_element(By.XPATH, '//div[@class="ssc-dropdown-item"][normalize-space()="Export"]').click()
        time.sleep(5)

        
        driver.get("https://spx.shopee.com.br/#/taskCenter/exportTaskCenter")
        time.sleep(15)


        # 👉 Mantendo o botão de download exatamente como no seu código original:
        """
        driver.find_element(
            By.XPATH,
            '//*[@id="fms-container"]/div[2]/div[2]/div/div/div/div[1]/div[8]/div/div[1]/div/div[2]/div[1]/div[1]/div[2]/div/div/div/table/tbody[2]/tr[1]/td[7]/div/div/button/span/span'
        ).click()
        """
        botoes = driver.find_elements(By.CLASS_NAME, "ssc-button")
        for botao in botoes:
            if "Download" in botao.text:
                botao.click()
                break

        

        time.sleep(15)  # Aguarda o download
        rename_downloaded_file(download_dir)

    except Exception as e:
        print(f"Erro ao coletar dados: {e}")
        driver.quit()
        raise

def rename_downloaded_file(download_dir):
    try:
        files = [f for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]
        files = [os.path.join(download_dir, f) for f in files]
        newest_file = max(files, key=os.path.getctime)

        current_hour = datetime.datetime.now().strftime("%H")
        new_file_name = f"Prod-{current_hour}.csv"
        new_file_path = os.path.join(download_dir, new_file_name)

        if os.path.exists(new_file_path):
            os.remove(new_file_path)

        shutil.move(newest_file, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        
def get_data_2(driver):
    """Coleta os dados necessários e realiza o download."""
    try:
        driver.get("https://spx.shopee.com.br/#/workstation-assignment")
        time.sleep(5)
        driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div/div/div[1]/div[1]/div/div/div/div/div[3]/span').click()
        time.sleep(5)

        # Inserir a data de ontem no campo de input
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y/%m/%d")
        date_input = driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div/div/div[1]/div[2]/div[2]/div/div/div/div[6]/form/div[1]/div/span/div/div/div[1]/input')
        # Realizar o clique triplo para selecionar o conteúdo existente
        actions = ActionChains(driver)
        actions.click(date_input).click(date_input).click(date_input).perform()
        #time.sleep(1)  # Pequena pausa para garantir a seleção

        # Limpar o campo e inserir a data de ontem
        date_input.send_keys(yesterday)  # Inserir a data formatada
        time.sleep(5)  # Pequena pausa para garantir a inserção
        
        driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div/div/div[1]/div[2]/div[2]/div/div/div/div[8]/div/button').click()
        time.sleep(10)

        driver.get("https://spx.shopee.com.br/#/taskCenter/exportTaskCenter")
        time.sleep(15)


        # 👉 Mantendo o botão de download exatamente como no seu código original:
        """
        #driver.findElement(By.xpath("//*[@id="fms-container"]/div[2]/div[2]/div/div/div/div[1]/div[8]/div/div[1]/div/div[2]/div[1]/div[1]/div[2]/div/div/div/table/tbody[2]/tr[1]/td[7]/div/div/button/span/span"))
        driver.find_element(
            By.XPATH,
            '//*[@id="fms-container"]/div[2]/div[2]/div/div/div/div[1]/div[8]/div/div[1]/div/div[2]/div[1]/div[1]/div[2]/div/div/div/table/tbody[2]/tr[1]/td[7]/div/div/button/span/span'
        ).click()
        """
        botoes = driver.find_elements(By.CLASS_NAME, "ssc-button")
        for botao in botoes:
            if "Download" in botao.text:
                botao.click()
                break

        time.sleep(15)  # Aguarda o download
        rename_downloaded_file_2(download_dir)

    except Exception as e:
        print(f"Erro ao coletar dados: {e}")
        driver.quit()
        raise

def rename_downloaded_file_2(download_dir):
    try:
        files = [f for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]
        files = [os.path.join(download_dir, f) for f in files]
        newest_file = max(files, key=os.path.getctime)

        current_hour = datetime.datetime.now().strftime("%H")
        new_file_name = f"WS-{current_hour}.csv"
        new_file_path = os.path.join(download_dir, new_file_name)

        if os.path.exists(new_file_path):
            os.remove(new_file_path)

        shutil.move(newest_file, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        
def main():
    try:
        login(driver)
        get_data(driver)
        get_data_2(driver)
        print("Download finalizado com sucesso.")
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
