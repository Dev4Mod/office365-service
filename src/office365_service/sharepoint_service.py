import os
import time
from functools import wraps

from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.sharepoint.folders.folder import Folder


def handle_sharepoint_errors(max_retries=5, delay_seconds=3):
    """
    Decorador para tratar exceções de requisições do SharePoint, com lógica
    de nova autenticação para erros 403 e novas tentativas para erros 503.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    self.ctx.clear()
                    return func(self, *args, **kwargs)
                except ClientRequestException as e:
                    last_exception = e
                    if e.response.status_code == 403:
                        print("Erro 403 (Proibido) detectado. Tentando relogar...")
                        if not (self.username and self.password):
                            print("Credenciais não disponíveis para relogin. Abortando.")
                            raise e

                        if self.login(self.username, self.password):
                            print("Relogin bem-sucedido. Tentando a operação novamente.")
                            try:
                                # Tenta a operação mais uma vez após o relogin
                                return func(self, *args, **kwargs)
                            except ClientRequestException as e2:
                                print(f"A operação falhou mesmo após o relogin: {e2}")
                                raise e2
                        else:
                            print("Falha ao relogar. Abortando.")
                            raise e
                    # --- Erro de servidor (temporário) ---
                    elif e.response.status_code == 503:
                        print(
                            f"Erro 503 (Serviço Indisponível). Tentativa {attempt + 1}/{max_retries} em {delay_seconds}s...")
                        time.sleep(delay_seconds)
                        continue  # Próxima iteração do loop de retentativa
                    # --- Outros erros de cliente/servidor ---
                    else:
                        print(f"Erro não recuperável encontrado: {e}")
                        raise e
                except Exception as e:
                    # Captura outras exceções (ex: problemas de rede)
                    print(f"Uma exceção inesperada ocorreu: {e}. Tentando novamente em {delay_seconds}s...")
                    last_exception = e
                    time.sleep(delay_seconds)

            # Se todas as tentativas falharem, lança a última exceção capturada
            print(f"A operação '{func.__name__}' falhou após {max_retries} tentativas.")
            raise last_exception

        return wrapper

    return decorator


class SharepointService:

    def __init__(self, site_url: str):
        self.site_url = site_url
        self.ctx = ClientContext(self.site_url)
        self.username = None
        self.password = None

    def login(self, username, password):
        """Autentica no site do SharePoint usando as credenciais fornecidas."""
        print(f"Fazendo login no SharePoint com o usuário {username}...")
        self.username = username
        self.password = password
        try:
            self.ctx.with_credentials(UserCredential(self.username, self.password))
            self.ctx.load(self.ctx.web)
            self.ctx.execute_query()
            print("Login realizado com sucesso.")
            return True
        except ClientRequestException as e:
            print(f"Erro ao fazer login: {e}")
            return False

    @handle_sharepoint_errors()
    def obter_pasta(self, caminho_pasta: str) -> Folder | None:
        """Obtém um objeto Folder a partir do seu caminho relativo no servidor."""
        try:
            folder = self.ctx.web.get_folder_by_server_relative_url(caminho_pasta)
            folder.get().execute_query()
            return folder
        except ClientRequestException as e:
            if e.response.status_code == 404:
                return None
            raise e

    @handle_sharepoint_errors()
    def obter_arquivo(self, caminho_arquivo: str) -> File | None:
        """Obtém um objeto File a partir do seu caminho relativo no servidor."""
        try:
            file = self.ctx.web.get_file_by_server_relative_url(caminho_arquivo)
            file.get().execute_query()
            return file
        except ClientRequestException as e:
            if e.response.status_code == 404:
                return None
            raise e

    @handle_sharepoint_errors()
    def listar_arquivos(self, pasta_alvo: Folder | str):
        """Lista todos os arquivos dentro de uma pasta específica."""
        if isinstance(pasta_alvo, str):
            pasta = self.obter_pasta(pasta_alvo)
            if pasta is None:
                raise FileNotFoundError(f"A pasta '{pasta_alvo}' não foi encontrada.")
            pasta_alvo = pasta
        files = pasta_alvo.files
        files.expand(["ModifiedBy"]).get().execute_query()
        return files

    @handle_sharepoint_errors()
    def listar_pastas(self, pasta_pai: Folder | str):
        """Lista todas as subpastas dentro de uma pasta pai."""
        if isinstance(pasta_pai, str):
            pasta = self.obter_pasta(pasta_pai)
            if pasta is None:
                raise FileNotFoundError(f"A pasta '{pasta_pai}' não foi encontrada.")
            pasta_pai = pasta

        folders = pasta_pai.folders
        folders.expand(["ModifiedBy"]).get().execute_query()
        return folders

    @handle_sharepoint_errors()
    def criar_pasta(self, pasta_pai: Folder | str, nome_pasta: str):
        """
        Cria uma nova pasta no Sharepoint, suportando criação de subpastas com "/"

        Args:
            pasta_pai: Caminho ou objeto Folder onde a nova pasta será criada
            nome_pasta: Nome da nova pasta a ser criada, pode incluir "/" para criar subpastas

        Returns:
            O objeto da pasta criada

        Raises:
            Exception: Se a pasta pai não for encontrada
        """
        if isinstance(pasta_pai, str):
            pasta = self.obter_pasta(pasta_pai)
            if pasta is None:
                raise Exception(f"Pasta {pasta_pai} não encontrada")
            pasta_pai = pasta

        if "/" in nome_pasta:
            path_parts = nome_pasta.split("/")
            current_folder = pasta_pai

            for part in path_parts:
                if part:
                    current_folder = self.criar_pasta(current_folder, part)

            return current_folder
        else:
            subpastas = self.listar_pastas(pasta_pai)
            pasta = next((subpasta for subpasta in subpastas if nome_pasta in str(subpasta.name)), None)
            if pasta is not None:
                return pasta
            print("Criando pasta {0}...".format(nome_pasta))
            pasta = pasta_pai.folders.add(nome_pasta)
            pasta.execute_query()
            return pasta

    @handle_sharepoint_errors()
    def baixar_arquivo(self, arquivo_sp: File | str, caminho_download: str):
        """Baixa um arquivo do SharePoint para um caminho local."""
        if isinstance(arquivo_sp, str):
            file_to_download = self.ctx.web.get_file_by_server_relative_url(arquivo_sp)
        else:
            file_to_download = arquivo_sp

        with open(caminho_download, "wb") as local_file:
            file_to_download.download_session(local_file).execute_query()
        print(f"Arquivo '{os.path.basename(str(arquivo_sp))}' baixado para '{caminho_download}'.")

    @handle_sharepoint_errors()
    def enviar_arquivo(self, pasta_destino: Folder | str, arquivo_local: str, nome_arquivo_sp: str = None):
        """Envia um arquivo local para uma pasta no SharePoint."""
        if isinstance(pasta_destino, str):
            pasta = self.obter_pasta(pasta_destino)
            if pasta is None:
                raise FileNotFoundError(f"A pasta de destino '{pasta_destino}' não foi encontrada.")
            pasta_destino = pasta

        if not nome_arquivo_sp:
            nome_arquivo_sp = os.path.basename(arquivo_local)

        with open(arquivo_local, 'rb') as file_content:
            fbytes = file_content.read()

        print(f"Enviando arquivo '{nome_arquivo_sp}'...")
        arquivo = pasta_destino.upload_file(nome_arquivo_sp, fbytes).execute_query()
        print(f"Arquivo '{nome_arquivo_sp}' enviado com sucesso!")
        return arquivo

    @handle_sharepoint_errors()
    def mover_arquivo(self, arquivo_origem: File, pasta_destino: Folder | str):
        """Move um arquivo para outra pasta de forma atômica."""
        if isinstance(pasta_destino, str):
            pasta = self.obter_pasta(pasta_destino)
            if pasta is None:
                raise FileNotFoundError(f"A pasta de destino '{pasta_destino}' não foi encontrada.")
            pasta_destino = pasta

        print(f"Movendo '{arquivo_origem.name}' para '{pasta_destino.name}'...")

        novo_arquivo = arquivo_origem.moveto(pasta_destino, flag=1)
        novo_arquivo.execute_query()

        return novo_arquivo

    @handle_sharepoint_errors()
    def copiar_arquivo(self, arquivo_origem: File, pasta_destino: Folder | str):
        """Copia um arquivo para outra pasta."""
        if isinstance(pasta_destino, str):
            pasta = self.obter_pasta(pasta_destino)
            if pasta is None:
                raise FileNotFoundError(f"A pasta de destino '{pasta_destino}' não foi encontrada.")
            pasta_destino = pasta

        print(f"Copiando '{arquivo_origem.name}' para '{pasta_destino.name}'...")
        novo_arquivo = arquivo_origem.copyto(pasta_destino, True).execute_query()
        print("Arquivo copiado com sucesso.")
        return novo_arquivo

    @handle_sharepoint_errors()
    def renomear_arquivo(self, arquivo: File, novo_nome: str) -> File:
        """Renomeia um arquivo no SharePoint."""
        print(f"Renomeando '{arquivo.name}' para '{novo_nome}'...")
        novo_arquivo = arquivo.rename(novo_nome)
        novo_arquivo.execute_query()
        print("Arquivo renomeado com sucesso.")
        return novo_arquivo

    def obter_pasta_por_nome(self, pasta_raiz: Folder, nome):
        pastas = list(self.listar_pastas(pasta_raiz))
        pasta_encontrada = next((pasta for pasta in pastas if nome in pasta.name), None)
        return pasta_encontrada

    def obter_arquivo_por_nome(self, pasta: Folder, nome):
        arquivos = list(self.listar_arquivos(pasta))
        arquivo_encontrado = next((arquivo for arquivo in arquivos if nome in arquivo.name), None)
        return arquivo_encontrado
