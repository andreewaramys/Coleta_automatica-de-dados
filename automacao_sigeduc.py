from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
import time
import datetime 


def configurar_webdriver():
    """Configura e retorna o WebDriver."""
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    return driver

def realizar_login(driver, username, password):
    """
    Realiza o login no portal.
    Você precisa identificar os seletores corretos dos campos de login e botão.
    """
    login_url = "https://parnamirim-rn.portalsigeduc.com.br/app/public/autenticacao.jsf"
    driver.get(login_url)
    wait = WebDriverWait(driver, 10)

    try:
        username_field = wait.until(EC.presence_of_element_located((By.ID, "Aramysandreew@gmail.com")))
        password_field = wait.until(EC.presence_of_element_located((By.ID, "Aramys01#")))
        login_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Entrar no Sistema')]")))

        username_field.send_keys(username)
        password_field.send_keys(password)
        login_button.click()

        time.sleep(3) 

        print("Login realizado com sucesso (ou tentativa de login).")
        return True
    except Exception as e:
        print(f"Erro ao tentar realizar login: {e}")
        return False

def conectar_banco_dados_pg(db_name, db_user, db_password, db_host='localhost', db_port='5432'):
    """
    Conecta ao banco de dados PostgreSQL e cria as tabelas se não existirem.
    """
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS escolas (
                id SERIAL PRIMARY KEY,
                nome_escola VARCHAR(255) UNIQUE NOT NULL,
                link_detalhes TEXT,
                data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alunos (
                id SERIAL PRIMARY KEY,
                nome TEXT NOT NULL,
                matricula VARCHAR(50) UNIQUE, -- Assumindo matrícula como identificador único
                cpf VARCHAR(14),
                data_nascimento DATE,
                data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS comunicados (
                id SERIAL PRIMARY KEY,
                titulo TEXT NOT NULL,
                data_publicacao DATE,
                conteudo TEXT,
                url_origem TEXT,
                data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        
        conn.commit()
        print(f"Conectado ao PostgreSQL '{db_name}' e tabelas verificadas/criadas.")
        return conn, cursor
    except Exception as e:
        print(f"Erro ao conectar ou configurar o PostgreSQL: {e}")
        return None, None

def extrair_escolas(driver, url_vinculos):
    """Navega para a página de vínculos e extrai informações das escolas."""
    driver.get(url_vinculos)
    wait = WebDriverWait(driver, 20)

    lista_escolas = []

    try:
        
        tabela_body = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.subFormulario tbody")))

        linhas_escolas = tabela_body.find_elements(By.TAG_NAME, "tr")

        print(f"Número total de linhas de escolas encontradas na tabela: {len(linhas_escolas)}")

        for linha in linhas_escolas:
            try:
                # Encontrar todas as células (<td>) dentro da linha.
                celulas = linha.find_elements(By.TAG_NAME, "td")

                # A informação da escola (nome e link) está na QUARTA coluna (índice 3).
                if len(celulas) > 3: 
                    celula_escola = celulas[3]
                    
                    # O nome da escola e o link estão dentro de um <a> na quarta célula
                    link_elemento = celula_escola.find_element(By.TAG_NAME, "a")
                    
                    nome_completo_lotacao = link_elemento.text.strip()
                    link_detalhes = link_elemento.get_attribute('href')
                    
                    # Extrair apenas o nome da escola (removendo "Lotação: ")
                    nome_escola = nome_completo_lotacao.replace("Lotação: ", "").strip()
                    
                    if nome_escola:
                        base_url = "https://parnamirim-rn.portalsigeduc.com.br"
                        if link_detalhes and link_detalhes.startswith("/"):
                            link_detalhes_absoluto = base_url + link_detalhes
                        else:
                            link_detalhes_absoluto = link_detalhes
                        
                        lista_escolas.append({'nome_escola': nome_escola, 'link_detalhes': link_detalhes_absoluto})
            except Exception as e:
                print(f"Erro ao processar linha de escola: {e}")
                continue 

        print(f"Extração de escolas concluída. Encontradas {len(lista_escolas)} escolas para salvar.")
        return lista_escolas

    except Exception as e:
        print(f"Erro ao extrair escolas da página {url_vinculos}: {e}")
        return []


def extrair_alunos(driver, url_alunos):
    """
    Navega para a página de alunos e extrai informações.
    VOCÊ PRECISA INSPECIONAR A PÁGINA DE ALUNOS PARA OBTER OS SELETORES CORRETOS!
    """
    driver.get(url_alunos)
    wait = WebDriverWait(driver, 15)

    lista_alunos = []

    try:
        tabela_alunos = wait.until(EC.presence_of_element_located((By.ID, "tabelaAlunos"))) 
        linhas_alunos = tabela_alunos.find_elements(By.TAG_NAME, "tr")

        print(f"Número total de linhas de alunos encontradas: {len(linhas_alunos)}")

        # Pular o cabeçalho se a tabela tiver <thead> (linhas_alunos[1:])
        for linha in linhas_alunos[1:]: 
            try:
                celulas = linha.find_elements(By.TAG_NAME, "td")
                if len(celulas) > 2: 
                    nome = celulas[0].text.strip()
                    matricula = celulas[1].text.strip()
                    cpf = celulas[2].text.strip() if len(celulas) > 2 else None
                    
                    
                    if nome and matricula:
                        lista_alunos.append({'nome': nome, 'matricula': matricula, 'cpf': cpf})
            except Exception as e:
                print(f"Erro ao processar linha de aluno: {e}")
                continue 

        print(f"Extração de alunos concluída. Encontrados {len(lista_alunos)} alunos para salvar.")
        return lista_alunos

    except Exception as e:
        print(f"Erro ao extrair alunos da página {url_alunos}: {e}")
        return []

def extrair_comunicados(driver, url_comunicados):
    """
    Navega para a página de comunicados e extrai informações.
    VOCÊ PRECISA INSPECIONAR A PÁGINA DE COMUNICADOS PARA OBTER OS SELETORES CORRETOS!
    """
    driver.get(url_comunicados)
    wait = WebDriverWait(driver, 15)

    lista_comunicados = []

    try:
        # --- EXEMPLO DE SELETORES PARA COMUNICADOS (AJUSTE CONFORME A PÁGINA REAL!) ---
        # Ex: Se comunicados estiverem em divs com classe 'card-comunicado'
        comunicados_container = wait.until(EC.presence_of_element_located((By.ID, "containerComunicados"))) # <--- AJUSTE ESTE SELETOR!
        itens_comunicado = comunicados_container.find_elements(By.CLASS_NAME, "card-comunicado") # <--- AJUSTE ESTE SELETOR!

        print(f"Número total de itens de comunicados encontrados: {len(itens_comunicado)}")

        for item in itens_comunicado:
            try:
                titulo = item.find_element(By.CLASS_NAME, "titulo-noticia").text.strip() # <--- AJUSTE ESTE SELETOR!
                data_str = item.find_element(By.CLASS_NAME, "data-publicacao").text.strip() # <--- AJUSTE ESTE SELETOR!
                conteudo = item.find_element(By.CLASS_NAME, "conteudo-resumido").text.strip() # <--- AJUSTE ESTE SELETOR!
                url_origem = item.find_element(By.TAG_NAME, "a").get_attribute('href') # Link para o comunicado completo

                # Tentar converter a data para o formato DATE do PostgreSQL
                data_publicacao = None
                try:
                    # Ajuste o formato da data conforme o site (ex: "%d/%m/%Y")
                    data_publicacao = datetime.datetime.strptime(data_str, "%d/%m/%Y").date()
                except ValueError:
                    print(f"Formato de data inválido para comunicado: {data_str}")
                
                if titulo:
                    lista_comunicados.append({
                        'titulo': titulo,
                        'data_publicacao': data_publicacao,
                        'conteudo': conteudo,
                        'url_origem': url_origem
                    })
            except Exception as e:
                print(f"Erro ao processar item de comunicado: {e}")
                continue 

        print(f"Extração de comunicados concluída. Encontrados {len(lista_comunicados)} comunicados para salvar.")
        return lista_comunicados

    except Exception as e:
        print(f"Erro ao extrair comunicados da página {url_comunicados}: {e}")
        return []


# --- FUNÇÕES PARA SALVAR NO BANCO DE DADOS ---
def salvar_escolas_no_banco_pg(cursor, escolas, conn):
    """Salva as informações das escolas no banco de dados PostgreSQL."""
    for escola in escolas:
        try:
            cursor.execute('''
                INSERT INTO escolas (nome_escola, link_detalhes)
                VALUES (%s, %s)
                ON CONFLICT (nome_escola) DO NOTHING;
            ''', (escola['nome_escola'], escola['link_detalhes']))
        except psycopg2.Error as e:
            print(f"Erro ao salvar escola '{escola.get('nome_escola', 'N/A')}': {e}")
            conn.rollback()
            continue
    conn.commit()
    print("Dados das escolas salvos/atualizados no PostgreSQL.")

def salvar_alunos_no_banco_pg(cursor, alunos, conn):
    """Salva as informações dos alunos no banco de dados PostgreSQL."""
    for aluno in alunos:
        try:
            cursor.execute('''
                INSERT INTO alunos (nome, matricula, cpf, data_nascimento)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (matricula) DO NOTHING;
            ''', (aluno['nome'], aluno['matricula'], aluno['cpf'], aluno.get('data_nascimento'))) 
        except psycopg2.Error as e:
            print(f"Erro ao salvar aluno '{aluno.get('nome', 'N/A')}': {e}")
            conn.rollback()
            continue
    conn.commit()
    print("Dados dos alunos salvos/atualizados no PostgreSQL.")

def salvar_comunicados_no_banco_pg(cursor, comunicados, conn):
    """Salva as informações dos comunicados no banco de dados PostgreSQL."""
    for comunicado in comunicados:
        try:
            cursor.execute('''
                INSERT INTO comunicados (titulo, data_publicacao, conteudo, url_origem)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (titulo) DO NOTHING;
            ''', (comunicado['titulo'], comunicado['data_publicacao'], comunicado['conteudo'], comunicado['url_origem']))
        except psycopg2.Error as e:
            print(f"Erro ao salvar comunicado '{comunicado.get('titulo', 'N/A')}': {e}")
            conn.rollback()
            continue
    conn.commit()
    print("Dados dos comunicados salvos/atualizados no PostgreSQL.")


# --- FUNÇÃO PRINCIPAL ---
def main():
    driver = configurar_webdriver()
    
    # --- CONFIGURAÇÕES DO POSTGRESQL ---
    DB_NAME = 'sigeduc_user'
    DB_USER = 'sigeduc_data'
    DB_PASSWORD = '?gTI.3012?'
    DB_HOST = 'localhost'
    DB_PORT = '5432'

    conn, cursor = conectar_banco_dados_pg(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    
    if not conn:
        print("Não foi possível conectar ao banco de dados. Encerrando.")
        if driver: driver.quit()
        return

    try:
        # --- ETAPA DE LOGIN (DESCOMENTE E AJUSTE SE NECESSÁRIO) ---
        # Substitua 'seu_usuario' e 'sua_senha' pelas credenciais reais
        # if not realizar_login(driver, 'seu_usuario', 'sua_senha'):
        #    print("Não foi possível realizar o login. Encerrando.")
        #    return
        # time.sleep(2) # Dar um tempo para a página carregar após o login

        # --- EXTRAÇÃO DE ESCOLAS ---
        url_vinculos_escolas = "https://parnamirim-rn.portalsigeduc.com.br/app/vinculos.jsf"
        escolas_encontradas = extrair_escolas(driver, url_vinculos_escolas)
        salvar_escolas_no_banco_pg(cursor, escolas_encontradas, conn)
        time.sleep(1) # Pequena pausa entre as navegações

        # --- EXTRAÇÃO DE ALUNOS (NECESSITA DA URL REAL E AJUSTE DOS SELETORES) ---
        # Você precisará descobrir a URL real da página de listagem de alunos
        # Exemplo: url_alunos = "https://parnamirim-rn.portalsigeduc.com.br/app/alunos.jsf"
        # Mude para a URL CORRETA que você encontrar no portal.
        url_alunos = "URL_DA_PAGINA_DE_ALUNOS_AQUI" # <--- AJUSTE ESTA URL!
        if url_alunos != "URL_DA_PAGINA_DE_ALUNOS_AQUI": # Só tenta se a URL for ajustada
            alunos_encontrados = extrair_alunos(driver, url_alunos)
            salvar_alunos_no_banco_pg(cursor, alunos_encontrados, conn)
            time.sleep(1)

        # --- EXTRAÇÃO DE COMUNICADOS (NECESSITA DA URL REAL E AJUSTE DOS SELETORES) ---
        # Você precisará descobrir a URL real da página de comunicados
        # Exemplo: url_comunicados = "https://parnamirim-rn.portalsigeduc.com.br/app/comunicados.jsf"
        # Mude para a URL CORRETA que você encontrar no portal.
        url_comunicados = "URL_DA_PAGINA_DE_COMUNICADOS_AQUI" # <--- AJUSTE ESTA URL!
        if url_comunicados != "URL_DA_PAGINA_DE_COMUNICADOS_AQUI": # Só tenta se a URL for ajustada
            comunicados_encontrados = extrair_comunicados(driver, url_comunicados)
            salvar_comunicados_no_banco_pg(cursor, comunicados_encontrados, conn)
            time.sleep(1)
            
    except Exception as e:
        print(f"Ocorreu um erro geral: {e}")
    finally:
        if driver:
            driver.quit()
            print("Navegador fechado.")
        if conn:
            cursor.close()
            conn.close()
            print("Conexão com o banco de dados encerrada.")

if __name__ == "__main__":
    main()