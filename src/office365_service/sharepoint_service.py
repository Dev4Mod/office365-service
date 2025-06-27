import os
import time
from functools import wraps

import requests

from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.sharepoint.folders.folder import Folder


def handle_sharepoint_errors(max_retries: int = 5, delay_seconds: int = 3):
    """
    Decorador para tratar exceções de requisições do SharePoint, com lógica
    de nova autenticação e novas tentativas para erros comuns.

    Args:
        max_retries (int): Número máximo de tentativas para erros recuperáveis.
        delay_seconds (int): Atraso entre as tentativas.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None

            for attempt in range(max_retries):
                try:
                    # A chamada da função original acontece aqui.
                    return func(self, *args, **kwargs)

                except requests.exceptions.Timeout as e:
                    # Captura a exceção de timeout da biblioteca 'requests'.
                    print(
                        f"Erro: A operação '{func.__name__}' excedeu o tempo limite de {self.timeout} segundos. "
                        f"Tentativa {attempt + 1}/{max_retries}...")
                    last_exception = e
                    time.sleep(delay_seconds)
                    continue

                except ClientRequestException as e:
                    last_exception = e
                    if e.response.status_code == 429:  # Too Many Requests
                        # O SharePoint pode retornar um header 'Retry-After'
                        retry_after = int(e.response.headers.get("Retry-After", 60 * (attempt + 1)))
                        print(f"Erro 429 (Muitas solicitações) detectado. Aguardando {retry_after} segundos...")
                        time.sleep(retry_after)
                        continue
                    elif e.response.status_code == 403:  # Forbidden
                        print("Erro 403 (Proibido) detectado. Tentando relogar...")
                        if not (self.username and self.password):
                            print("Credenciais não disponíveis para relogin. Abortando.")
                            raise e

                        if self.login(self.username, self.password):
                            print("Relogin bem-sucedido. Tentando a operação novamente.")
                            continue
                        else:
                            print("Falha ao relogar. Abortando.")
                            raise e
                    elif e.response.status_code in [503, 504]:  # Service Unavailable / Gateway Timeout
                        print(
                            f"Erro {e.response.status_code} (Servidor indisponível/sobrecarregado). "
                            f"Tentativa {attempt + 1}/{max_retries} em {delay_seconds}s...")
                        time.sleep(delay_seconds)
                        continue
                    else:
                        print(f"Erro de cliente não recuperável encontrado: {e}")
                        raise e

                except Exception as e:
                    print(f"Uma exceção inesperada ocorreu: {e}. Tentando novamente em {delay_seconds}s...")
                    last_exception = e
                    time.sleep(delay_seconds)

            print(f"A operação '{func.__name__}' falhou após {max_retries} tentativas.")
            raise last_exception

        return wrapper

    return decorator


class SharepointService:
    def __init__(self, site_url: str, timeout_seconds: int = 90):
        """
        Inicializa o serviço do SharePoint.

        Args:
            site_url (str): A URL do site do SharePoint.
            timeout_seconds (int): O tempo em segundos para o timeout de cada requisição.
        """
        self.site_url = site_url
        self.ctx = ClientContext(self.site_url)
        self.username = None
        self.password = None
        self.timeout = timeout_seconds

        # **SOLUÇÃO**: Adiciona um manipulador de eventos para definir o timeout ANTES de cada requisição.
        self.ctx.pending_request().beforeExecute += self._set_request_timeout

    def _set_request_timeout(self, request_options):
        """
        Este método é chamado antes de cada requisição para injetar o parâmetro de timeout.
        """
        request_options.timeout = self.timeout

    def login(self, username: str, password: str) -> bool:
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
        except (ClientRequestException, requests.exceptions.Timeout) as e:
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
        """Cria uma nova pasta no Sharepoint, suportando criação de subpastas com "/" """
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
            subpastas = list(self.listar_pastas(pasta_pai))
            pasta = next((subpasta for subpasta in subpastas if nome_pasta == subpasta.name), None)
            if pasta is not None:
                return pasta
            print(f"Criando pasta {nome_pasta}...")
            pasta = pasta_pai.folders.add(nome_pasta).execute_query()
            return pasta

    @handle_sharepoint_errors()
    def baixar_arquivo(self, arquivo_sp: File | str, caminho_download: str):
        """Baixa um arquivo do SharePoint para um caminho local."""
        nome_arquivo = ""
        if isinstance(arquivo_sp, str):
            file_to_download = self.obter_arquivo(arquivo_sp)
            if file_to_download is None:
                raise FileNotFoundError(f"Arquivo '{arquivo_sp}' não encontrado no SharePoint.")
            nome_arquivo = file_to_download.name
        else:
            file_to_download = arquivo_sp
            nome_arquivo = arquivo_sp.name

        with open(caminho_download, "wb") as local_file:
            file_to_download.download(local_file).execute_query()
        print(f"Arquivo '{nome_arquivo}' baixado para '{caminho_download}'.")

    @handle_sharepoint_errors()
    def enviar_arquivo(self, pasta_destino: Folder | str, arquivo_local: str, nome_arquivo_sp: str = None):
        """Envia um arquivo local para uma pasta no SharePoint."""
        if isinstance(pasta_destino, str):
            pasta = self.obter_pasta(pasta_destino)
            if pasta is None:
                raise FileNotFoundError(f"A pasta de destino '{pasta_destino}' não foi encontrada.")
            pasta_destino = pasta

        nome_arquivo_sp = nome_arquivo_sp or os.path.basename(arquivo_local)

        with open(arquivo_local, 'rb') as file_content:
            print(f"Enviando arquivo '{nome_arquivo_sp}'...")
            arquivo = pasta_destino.files.upload(nome_arquivo_sp, file_content).execute_query()

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

        print(f"Movendo '{arquivo_origem.name}' para '{pasta_destino.serverRelativeUrl}'...")
        novo_caminho = f"{pasta_destino.serverRelativeUrl}/{arquivo_origem.name}"
        arquivo_origem.moveto(novo_caminho, 1).execute_query()
        print("Arquivo movido com sucesso.")

    @handle_sharepoint_errors()
    def copiar_arquivo(self, arquivo_origem: File, pasta_destino: Folder | str, novo_nome: str = None):
        """Copia um arquivo para outra pasta."""
        if isinstance(pasta_destino, str):
            pasta = self.obter_pasta(pasta_destino)
            if pasta is None:
                raise FileNotFoundError(f"A pasta de destino '{pasta_destino}' não foi encontrada.")
            pasta_destino = pasta

        nome_final = novo_nome or arquivo_origem.name
        print(f"Copiando '{arquivo_origem.name}' para '{pasta_destino.serverRelativeUrl}/{nome_final}'...")
        arquivo_origem.copyto(f"{pasta_destino.serverRelativeUrl}/{nome_final}", True).execute_query()
        print("Arquivo copiado com sucesso.")

    @handle_sharepoint_errors()
    def renomear_arquivo(self, arquivo: File, novo_nome: str):
        """Renomeia um arquivo no SharePoint."""
        print(f"Renomeando '{arquivo.name}' para '{novo_nome}'...")
        arquivo.rename(novo_nome).execute_query()
        print("Arquivo renomeado com sucesso.")

    @handle_sharepoint_errors()
    def compartilhar_item(self, item: File | Folder, tipo: int = 0):
        """Cria um link de compartilhamento para um item. tipo 0: View, 1: Edit"""
        resultado = item.share_link(tipo).execute_query()
        return resultado.value.sharingLinkInfo.Url

    def obter_pasta_por_nome(self, pasta_raiz: Folder, nome: str) -> Folder | None:
        """Busca uma subpasta pelo nome exato dentro de uma pasta raiz."""
        pastas = self.listar_pastas(pasta_raiz)
        pasta_encontrada = next((pasta for pasta in pastas if nome in pasta.name), None)
        return pasta_encontrada

    def obter_arquivo_por_nome(self, pasta: Folder, nome: str) -> File | None:
        """Busca um arquivo pelo nome exato dentro de uma pasta."""
        arquivos = self.listar_arquivos(pasta)
        arquivo_encontrado = next((arquivo for arquivo in arquivos if nome in arquivo.name), None)
        return arquivo_encontrado