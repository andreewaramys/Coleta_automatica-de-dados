import asyncio
from playwright.async_api import async_playwright
import psycopg2
import time
import datetime


SIGEDUC_URL_HOME = 'https://parnamirim-rn.portalsigeduc.com.br/app/public/home.jsf'
SIGEDUC_URL_LOGIN = 'https://parnamirim-rn.portalsigeduc.com.br/app/mvc/verTelaLogin'
SIGEDUC_USERNAME = 'Andreewaramys@gmail.com' 
SIGEDUC_PASSWORD = '********'

DB_NAME = 'gti' 
DB_USER = 'gti' 
DB_PASSWORD = '***********'
DB_HOST = '************'
DB_PORT = '5432'

# --- FUNÇÕES DE SETUP E CONEXÃO --- (Mantidas iguais)
def conectar_banco_dados_pg(db_name, db_user, db_password, db_host, db_port):
    """
    Conecta ao banco de dados PostgreSQL e cria as tabelas necessárias.
    """
    try:
        print(f"Tentando conectar ao PostgreSQL: {db_user}@{db_host}:{db_port}/{db_name}")
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port,
            client_encoding='UTF8'
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
            CREATE TABLE IF NOT EXISTS resumo_escola (
                id SERIAL PRIMARY KEY,
                escola_id INTEGER REFERENCES escolas(id) ON DELETE CASCADE UNIQUE,
                total_estudantes INTEGER,
                total_professores INTEGER,
                total_turmas INTEGER,
                total_novos_estudantes INTEGER,
                estudantes_nao_alocados INTEGER,
                data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        
        conn.commit()
        print(f"Conectado ao PostgreSQL '{db_name}' e tabelas verificadas/criadas.")
        return conn, cursor
    except Exception as e:
        print(f"Erro ao conectar ou configurar o PostgreSQL: {e}")
        return None, None

# --- FUNÇÃO DE LOGIN --- (Mantida igual)
async def realizar_login(page, username, password):
    """
    Realiza o login no portal SIGEduc usando Playwright.
    """
    print(f"Navegando para a página de login: {SIGEDUC_URL_LOGIN}")
    await page.goto(SIGEDUC_URL_LOGIN, wait_until='domcontentloaded')

    try:
        username_field = page.locator("#userLogin") 
        password_field = page.locator("#inputPass") 
        login_button = page.locator("button:has-text('Entrar no Sistema')")

        print(f"Preenchendo usuário: '{username}'")
        await username_field.fill(username)
        print("Preenchendo senha...")
        await password_field.fill(password)
        
        print("Clicando no botão 'Entrar no Sistema'...")
        await login_button.click() 
        
        await page.wait_for_load_state('networkidle')
        time.sleep(3)

        if SIGEDUC_URL_LOGIN not in page.url:
            print(f"Login realizado com sucesso. Redirecionado para: {page.url}")
            return True
        else:
            print("Login falhou. Ainda na página de autenticação.")
            print("URL atual:", page.url)
            await page.screenshot(path="erro_login_falha.png")
            print("Screenshot 'erro_login_falha.png' salva.")
            return False
    except Exception as e:
        print(f"Erro inesperado ao tentar realizar login: {e}")
        await page.screenshot(path="erro_login_excecao.png")
        print("Screenshot 'erro_login_excecao.png' salva.")
        return False

# --- FUNÇÃO: EXTRAIR INFORMAÇÕES BÁSICAS DAS ESCOLAS --- (Mantida igual)
async def extrair_escolas(page, cursor, conn):
    """
    Navega para a página de vínculos e extrai informações básicas das escolas.
    Insere diretamente no banco de dados. Esta função é importante para popular a tabela 'escolas'
    que é usada para obter o 'escola_id' para o resumo da escola.
    """
    url_vinculos = "https://parnamirim-rn.portalsigeduc.com.br/app/vinculos.jsf"
    print(f"\nNavegando para a página de vínculos de escolas: {url_vinculos}")
    await page.goto(url_vinculos, wait_until='domcontentloaded')

    print(f"DEBUG: URL atual na extrair_escolas: {page.url}")
    if url_vinculos not in page.url:
        print("DEBUG: NÃO está na página de vínculos esperada. Pode ser um redirecionamento ou erro.")
        await page.screenshot(path="debug_nao_na_pagina_vinculos.png")
        return

    try:
        tabela_body = page.locator("table.subFormulario:has(caption:has-text('Permissões Concedidas')) tbody")
        await tabela_body.wait_for(timeout=30000) 
        print("DEBUG: Tabela de vínculos de escolas (Permissões Concedidas) encontrada!")
        
        linhas_escolas = await tabela_body.locator("tr").all()
        print(f"DEBUG: Número total de TRs (linhas) encontradas na tabela de vínculos: {len(linhas_escolas)}")

        if len(linhas_escolas) == 0:
            print("DEBUG: Nenhuma linha de dados encontrada na tabela de vínculos. A tabela pode estar vazia ou o seletor de linha está errado.")
            await page.screenshot(path="debug_tabela_vinculos_vazia.png")
            return

        escolas_salvas_count = 0
        for i, linha in enumerate(linhas_escolas):
            if i == 0 and await linha.locator("th").count() > 0:
                print(f"DEBUG: Pulando linha {i} (cabeçalho).")
                continue 
            
            try:
                celulas = await linha.locator("td").all()
                print(f"DEBUG: Linha {i} - Encontradas {len(celulas)} células.")
                
                if len(celulas) > 3: 
                    link_elemento = celulas[3].locator("a") 
                    
                    nome_completo_lotacao = await link_elemento.text_content()
                    link_detalhes = await link_elemento.get_attribute('href')
                    
                    nome_escola = nome_completo_lotacao.replace("Lotação: ", "").strip()
                    
                    if nome_escola:
                        print(f"DEBUG: Tentando salvar escola: '{nome_escola}' com link: '{link_detalhes}'")
                        
                        # Note: O link já é absoluto ou relativo que Playwright lida bem em .goto()
                        # Mas se for usar em outro lugar, pode precisar de base_url novamente.
                        
                        cursor.execute('''
                            INSERT INTO escolas (nome_escola, link_detalhes)
                            VALUES (%s, %s)
                            ON CONFLICT (nome_escola) DO NOTHING;
                        ''', (nome_escola, link_detalhes)) 
                        conn.commit()
                        escolas_salvas_count += 1
                        print(f"  -> Escola '{nome_escola}' salva/atualizada.")
                    else:
                        print(f"DEBUG: Linha {i} - Nome da escola vazio após limpeza.")
            except Exception as e:
                print(f"DEBUG: Erro ao processar linha de vínculo de escola ({i}): {e}")
                await page.screenshot(path=f"debug_erro_linha_escola_{i}.png")
                continue 

        print(f"Extração e salvamento de escolas concluído. Total de escolas salvas: {escolas_salvas_count}.")

    except Exception as e:
        print(f"Erro ao extrair escolas da página {url_vinculos}: {e}")
        await page.screenshot(path="erro_extracao_escolas.png")

# --- FUNÇÃO NOVA: EXTRAIR RESUMO DA ESCOLA --- (Mantida igual)
async def extrair_resumo_escola(page, cursor, conn, nome_escola_selecionada):
    """
    Extrai os totais de estudantes, professores, turmas etc. da página de resumo da escola.
    Assume que a página já está carregada após a seleção da escola.
    """
    print(f"\nIniciando extração do resumo da escola: '{nome_escola_selecionada}'")
    
    try:
        # Recuperar o ID da escola da tabela 'escolas' (precisa ser extraída antes)
        escola_id = None
        cursor.execute("SELECT id FROM escolas WHERE nome_escola = %s", (nome_escola_selecionada,))
        result = cursor.fetchone()
        if result:
            escola_id = result[0]
            print(f"  -> ID da escola '{nome_escola_selecionada}' encontrado: {escola_id}")
        else:
            print(f"  -> ERRO: Escola '{nome_escola_selecionada}' não encontrada na tabela 'escolas'. Pulando resumo.")
            return

        # Localizar a tabela de resumo (classe baseada no HTML que você forneceu)
        tabela_resumo = page.locator('table.formulario:has(td strong:has-text("Total de Estudantes"))')
        await tabela_resumo.wait_for(timeout=10000) 
        print("DEBUG: Tabela de resumo da escola encontrada!")

        # Dicionário para armazenar os totais
        resumo_dados = {
            'escola_id': escola_id,
            'total_estudantes': None,
            'total_professores': None,
            'total_turmas': None,
            'total_novos_estudantes': None,
            'estudantes_nao_alocados': None
        }

        # Percorrer as linhas da tabela para extrair os valores
        linhas = await tabela_resumo.locator('tr').all()
        print(f"DEBUG: Tabela de resumo possui {len(linhas)} linhas TRs (incluindo cabeçalhos ou vazias).")

        for i, linha in enumerate(linhas):
            print(f"DEBUG: Processando linha {i} do resumo.")
            
            if await linha.locator('td strong').count() == 0 or await linha.locator('td:last-child').count() == 0:
                print(f"DEBUG: Linha {i} não contém elementos de label/valor esperados. Pulando.")
                continue 

            try:
                label_element = linha.locator('td strong')
                value_element = linha.locator('td:last-child')

                await label_element.wait_for(timeout=5000) 
                await value_element.wait_for(timeout=5000) 

                label_element_text = await label_element.text_content()
                value_element_text = await value_element.text_content()

                print(f"DEBUG: Linha {i} - Label: '{label_element_text}', Valor: '{value_element_text}'")

                label_text = label_element_text.strip().replace(":", "")
                
                try:
                    value_num = int(value_element_text.strip()) 
                except ValueError:
                    print(f"  -> Aviso: Linha {i} - Valor '{value_element_text}' não numérico para '{label_text}'. Ignorando.")
                    continue 

                if "Total de Estudantes" in label_text:
                    resumo_dados['total_estudantes'] = value_num
                elif "Total de Professores" in label_text:
                    resumo_dados['total_professores'] = value_num
                elif "Total de Turmas" in label_text:
                    resumo_dados['total_turmas'] = value_num
                elif "Total de Novos Estudantes" in label_text:
                    resumo_dados['total_novos_estudantes'] = value_num
                elif "Estudantes NÃƒO alocados em Turmas" in label_text or \
                     "Estudantes NÃO alocados em Turmas" in label_text: 
                    resumo_dados['estudantes_nao_alocados'] = value_num
            except Exception as e:
                print(f"DEBUG: Erro ao processar linha de resumo ({i}): {e}")
                continue 

        cursor.execute('''
            INSERT INTO resumo_escola (escola_id, total_estudantes, total_professores, total_turmas, total_novos_estudantes, estudantes_nao_alocados)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (escola_id) DO UPDATE SET
                total_estudantes = EXCLUDED.total_estudantes,
                total_professores = EXCLUDED.total_professores,
                total_turmas = EXCLUDED.total_turmas,
                total_novos_estudantes = EXCLUDED.total_novos_estudantes,
                estudantes_nao_alocados = EXCLUDED.estudantes_nao_alocados,
                data_extracao = CURRENT_TIMESTAMP;
        ''', (
            resumo_dados['escola_id'],
            resumo_dados['total_estudantes'],
            resumo_dados['total_professores'],
            resumo_dados['total_turmas'],
            resumo_dados['total_novos_estudantes'],
            resumo_dados['estudantes_nao_alocados']
        ))
        conn.commit()
        print(f"Resumo da escola '{nome_escola_selecionada}' salvo/atualizado no BD.")

    except Exception as e:
        print(f"Erro geral ao extrair resumo da escola: {e}")
        await page.screenshot(path="erro_extracao_resumo_escola.png")

# --- FUNÇÃO PRINCIPAL QUE ORQUESTRA TUDO ---
async def main():
    browser = None
    conn = None
    cursor = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False, args=["--start-maximized"])
            context = await browser.new_context(no_viewport=True)
            page = await context.new_page()

            # --- CONFIGURAÇÃO DO BANCO DE DADOS ---
            conn, cursor = conectar_banco_dados_pg(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
            if not conn:
                print("Não foi possível conectar ao banco de dados. Encerrando.")
                return

            # --- ETAPA DE LOGIN ---
            print("\nIniciando login no portal SIGEduc...")
            if not await realizar_login(page, SIGEDUC_USERNAME, SIGEDUC_PASSWORD):
                print("Falha no login. Encerrando a automação.")
                return
            
            await page.wait_for_load_state('networkidle')
            time.sleep(3) 

            # --- EXTRAÇÃO DE ESCOLAS BÁSICAS (PARA POPULAR IDS) ---
            # Esta etapa é crucial para que 'extrair_resumo_escola' encontre o ID da escola.
            # E também para que o loop de todas as escolas tenha os nomes e links.
            print("\nIniciando extração básica das escolas (vínculos)...")
            await extrair_escolas(page, cursor, conn) 
            time.sleep(2)

            # --- LOOP PARA EXTRAIR RESUMO DE TODAS AS ESCOLAS ---
            print("\nIniciando extração do resumo para TODAS as escolas...")
            
            # 1. Recupera a lista de todas as escolas do BD
            escolas_do_bd = []
            cursor.execute("SELECT nome_escola, link_detalhes FROM escolas ORDER BY nome_escola;")
            for row in cursor.fetchall():
                escolas_do_bd.append({"nome_escola": row[0], "link_detalhes": row[1]})
            
            print(f"Encontradas {len(escolas_do_bd)} escolas no banco de dados para processar o resumo.")

            url_vinculos_page = "https://parnamirim-rn.portalsigeduc.com.br/app/vinculos.jsf" # URL da página de vínculos

            for i, escola in enumerate(escolas_do_bd):
                nome_escola_atual = escola["nome_escola"]
                link_detalhes_escola = escola["link_detalhes"]

                print(f"\n--- Processando resumo da escola {i+1}/{len(escolas_do_bd)}: '{nome_escola_atual}' ---")

                try:
                    # 1. Navegar de volta para a página de vínculos (onde todos os links de escola estão)
                    # Isso é crucial para poder clicar em um novo link de escola a cada iteração.
                    print(f"  -> Navegando de volta para a página de vínculos: {url_vinculos_page}")
                    await page.goto(url_vinculos_page, wait_until='domcontentloaded')
                    await page.wait_for_load_state('networkidle')
                    time.sleep(2) # Pausa para garantir que a página de vínculos carregue

                    # 2. Clicar no link para acessar a escola específica (o 1º clique na navegação)
                    # Usamos o link_detalhes para ser mais específico no clique.
                    # O Playwright pode clicar em um <a> com um href específico.
                    # Ou, se o link_detalhes for apenas o 'vinculo=X', podemos reconstruir
                    # o seletor baseado no nome da escola novamente.
                    
                    # Vamos tentar pelo texto completo da Lotação, que é mais robusto visualmente.
                    print(f"  -> Clicando no vínculo para acessar '{nome_escola_atual}'...")
                    link_lotacao = page.locator(f"a:has-text('Lotação: {nome_escola_atual}')")
                    await link_lotacao.click()
                    await page.wait_for_load_state('networkidle')
                    time.sleep(3) # Pausa para a página de resumo da escola carregar

                    # 3. Extrair o resumo da escola atual
                    await extrair_resumo_escola(page, cursor, conn, nome_escola_atual)
                    print(f"--- Resumo de '{nome_escola_atual}' processado. ---")

                except Exception as e:
                    print(f"### ERRO ao processar resumo para '{nome_escola_atual}': {e}")
                    await page.screenshot(path=f"erro_resumo_{nome_escola_atual.replace(' ', '_')}.png")
                    continue # Continua para a próxima escola mesmo que esta falhe

            print("\nProcesso de extração de resumo para TODAS as escolas concluído!")

    except Exception as e:
        print(f"\nOcorreu um erro geral na execução principal: {e}")
        if 'page' in locals() and page:
            await page.screenshot(path="erro_fatal_main.png")
            print("Screenshot 'erro_fatal_main.png' salva.")
    finally:
        if browser:
            await browser.close() 
            print("Navegador Playwright fechado.")
        if conn:
            cursor.close()
            conn.close()
            print("Conexão com o banco de dados encerrada.")

if __name__ == "__main__":
    asyncio.run(main())
