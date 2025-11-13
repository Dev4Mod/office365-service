import os
import time
from functools import wraps

from msal import SerializableTokenCache, PublicClientApplication
from office365.runtime.auth.token_response import TokenResponse
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.sharepoint.folders.folder import Folder
from office365.runtime.http.request_options import RequestOptions


def handle_sharepoint_errors(max_retries: int = 5, delay_seconds: int = 3):
    """
    Decorador para tratar exceções de requisições do SharePoint, com lógica
    de nova autenticação, novas tentativas e controle de timeout.

    Args:
        max_retries (int): Número máximo de tentativas para erros recuperáveis.
        delay_seconds (int): Atraso entre as tentativas.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self: "SharepointService", *args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    self.ctx.clear()
                    result = func(self, *args, **kwargs)
                    if attempt > 0:
                        print(f"Operação concluída com sucesso. Na tenativa {attempt + 1}")
                    return result
                except ClientRequestException as e:
                    last_exception = e
                    if e.response.status_code == 429:
                        wait_time = 60 * (attempt + 1)
                        print(f"Erro 429 (Muitas solicitações) detectado. Aguardando {wait_time} segundos...")
                        time.sleep(wait_time)
                        continue
                    elif e.response.status_code == 403 or e.response.status_code == 401:
                        print("Erro 403 (Proibido) detectado. Tentando relogar...")
                        if self.using_device:
                            self.refresh_device_token()
                            continue
                        if not (self.username and self.password):
                            print("Credenciais não disponíveis para relogin. Abortando.")
                            raise e

                        if self.login(self.username, self.password):
                            print("Relogin bem-sucedido. Tentando a operação novamente.")
                            # Tenta novamente a operação dentro do mesmo loop
                            continue
                        else:
                            print("Falha ao relogar. Abortando.")
                            raise e
                    elif e.response.status_code == 503:
                        print(
                            f"Erro 503 (Serviço Indisponível). Tentativa {attempt + 1}/{max_retries} em {delay_seconds}s...")
                        time.sleep(delay_seconds)
                        continue
                    else:
                        print(f"Erro não recuperável encontrado: {e}")
                        raise e

                except Exception as e:
                    if "auth cookies" in str(e).lower():
                        print("Token expirado detectado. Tentando relogar...")
                        self.refresh_device_token()
                        continue
                    print(f"Uma exceção inesperada ocorreu: {e}. Tentando novamente em {delay_seconds}s...")
                    last_exception = e
                    time.sleep(delay_seconds)

            print(f"A operação '{func.__name__}' falhou após {max_retries} tentativas.")
            raise last_exception

        return wrapper

    return decorator


class SharepointService:
    CACHE_TOKEN = "cache_token.json"

    def __init__(self, site_url: str, timeout_seconds: int = 90):
        """
        Inicializa o serviço do SharePoint.

        Args:
            site_url (str): A URL do site do SharePoint.
            timeout_seconds (int): O tempo em segundos para o timeout de cada requisição.
        """
        self.scopes = None
        self.authority = None
        self.client_id = None
        self.using_device = False
        self.site_url = site_url
        self.ctx = ClientContext(self.site_url)
        self.username = None
        self.password = None
        self.timeout = timeout_seconds
        self.cache = SerializableTokenCache()
        self._load_token()
        self.ctx.pending_request().beforeExecute += self._set_request_timeout

    def _set_request_timeout(self, request_options: RequestOptions):
        """
        Este método é chamado antes de cada requisição para injetar o parâmetro de timeout.
        """
        request_options.timeout = self.timeout

    def login(self, username, password):
        """Autentica no site do SharePoint usando as credenciais fornecidas."""
        print(f"Fazendo login no SharePoint com o usuário {username}...")
        self.username = username
        self.password = password
        for attempt in range(3):
            try:
                self.ctx.clear()
                self.ctx.with_credentials(UserCredential(username, password))
                self.ctx.load(self.ctx.web)
                self.ctx.execute_query()
                print("Login realizado com sucesso.")
                return True
            except Exception as e:
                print(f"Erro ao fazer login: {e}")
                if attempt < 2:
                    print(f"Tentando novamente (tentativa {attempt + 1}/3)...")
                    time.sleep(3)
                else:
                    print("Falha após 3 tentativas. Abortando.")

        return False

    def _load_token(self):
        if os.path.exists(self.CACHE_TOKEN):
            self.cache.deserialize(open(self.CACHE_TOKEN, "r").read())

    def _save_cache(self):
        if self.cache.has_state_changed:
            with open(self.CACHE_TOKEN, "w") as f:
                f.write(self.cache.serialize())

    def refresh_device_token(self):
        if self.using_device and self.client_id and self.authority and self.scopes:
            self.login_device_code(self.client_id, self.authority, self.scopes)

    def login_device_code(self, client_id: str, authority: str, scopes: list[str]):
        self.using_device = True
        self.client_id = client_id
        self.authority = authority
        self.scopes = scopes
        self.ctx = ClientContext(self.site_url)
        self.ctx.with_access_token(self._refresh_token)
        self.ctx.pending_request().beforeExecute += self._set_request_timeout
        self.ctx.load(self.ctx.web)
        self.ctx.execute_query()

    def _refresh_token(self):
        app = PublicClientApplication(client_id=self.client_id, authority=self.authority, token_cache=self.cache)

        # tenta renovar silenciosamente
        accounts = app.get_accounts()
        result = None
        if accounts:
            result = app.acquire_token_silent(self.scopes, account=accounts[0])

        if not result:
            flow = app.initiate_device_flow(scopes=self.scopes)
            print(flow["message"])
            result = app.acquire_token_by_device_flow(flow)

        if "access_token" in result:
            self._save_cache()
            access_token = result["access_token"]
        else:
            raise Exception("Erro:", result)

        return TokenResponse(access_token=access_token, token_type="Bearer", expiresIn=result["expires_in"])

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
            nome_arquivo = os.path.basename(caminho_arquivo)
            caminho_arquivo = os.path.dirname(caminho_arquivo)
            folder = self.obter_pasta(caminho_arquivo)
            if folder is None:
                raise FileNotFoundError(f"A pasta '{caminho_arquivo}' não foi encontrada.")
            files = folder.files
            files.get().execute_query()
            if not files:
                raise FileNotFoundError(f"Nenhum arquivo encontrado na pasta '{caminho_arquivo}'.")
            for file in files:
                if file.name == nome_arquivo:
                    return file
            return None
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
            pasta = pasta_pai.folders.add(nome_pasta)
            pasta.execute_query()
            return pasta

    @handle_sharepoint_errors()
    def baixar_arquivo(self, arquivo_sp: File | str, caminho_download: str, max_tentativas: int = 3):
        """
        Baixa um arquivo do SharePoint para um caminho local, com verificação de integridade e novas tentativas.
        """
        if isinstance(arquivo_sp, str):
            file_to_download = self.obter_arquivo(arquivo_sp)
            if file_to_download is None:
                raise FileNotFoundError(f"Arquivo remoto '{arquivo_sp}' não encontrado.")
        else:
            file_to_download = arquivo_sp
            # Garante que os metadados do arquivo (como o tamanho) estão carregados
            file_to_download.get().execute_query()

        # O decorador @handle_sharepoint_errors em obter_arquivo já tratou erros de API aqui.
        tamanho_remoto = file_to_download.length
        unique_id = file_to_download.unique_id

        for tentativa in range(max_tentativas):
            print(f"Iniciando download de '{file_to_download.name}' (Tentativa {tentativa + 1}/{max_tentativas})...")

            with open(caminho_download, "wb") as local_file:
                response = self.ctx.execute_request_direct("/Web/GetFileById('{0}')/$value".format(unique_id))
                data = response.content
                local_file.write(data)

            # Verificação do tamanho do arquivo
            tamanho_local = os.path.getsize(caminho_download)

            if tamanho_local == tamanho_remoto:
                print(
                    f"Download de '{file_to_download.name}' concluído e verificado com sucesso. Tamanho: {tamanho_local} bytes.")
                return  # Sucesso, sai da função
            else:
                print(f"Falha na verificação de tamanho para '{file_to_download.name}'.")
                print(f"  -> Tamanho esperado: {tamanho_remoto} bytes")
                print(f"  -> Tamanho baixado:  {tamanho_local} bytes")

            if tentativa < max_tentativas - 1:
                print("Aguardando 5 segundos para tentar novamente...")
                time.sleep(5)

        # Se o loop terminar, todas as tentativas falharam.
        print(f"Falha ao baixar o arquivo '{file_to_download.name}' após {max_tentativas} tentativas.")

        # Tenta remover o arquivo parcial/corrompido
        try:
            if os.path.exists(caminho_download):
                os.remove(caminho_download)
                print(f"Arquivo parcial '{caminho_download}' removido.")
        except OSError as e:
            print(f"Não foi possível remover o arquivo parcial '{caminho_download}': {e}")

        raise IOError(
            f"Não foi possível baixar o arquivo '{file_to_download.name}' com o tamanho correto após {max_tentativas} tentativas.")

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

        arquivo = pasta_destino.upload_file(nome_arquivo_sp, fbytes).execute_query()
        return arquivo

    @handle_sharepoint_errors()
    def mover_arquivo(self, arquivo_origem: File, pasta_destino: Folder | str):
        """Move um arquivo para outra pasta de forma atômica."""
        if isinstance(pasta_destino, str):
            pasta = self.obter_pasta(pasta_destino)
            if pasta is None:
                raise FileNotFoundError(f"A pasta de destino '{pasta_destino}' não foi encontrada.")
            pasta_destino = pasta

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

        novo_arquivo = arquivo_origem.copyto(pasta_destino, True).execute_query()
        return novo_arquivo

    @handle_sharepoint_errors()
    def renomear_arquivo(self, arquivo: File, novo_nome: str) -> File:
        """Renomeia um arquivo no SharePoint."""
        novo_arquivo = arquivo.rename(novo_nome)
        novo_arquivo.execute_query()
        return novo_arquivo

    @handle_sharepoint_errors()
    def compartilhar_item(self, item: File | Folder, tipo: int):
        resultado = item.share_link(tipo)
        resultado.execute_query()
        return resultado.value.sharingLinkInfo.Url

    def obter_pasta_por_nome(self, pasta_raiz: Folder, nome):
        pastas = list(self.listar_pastas(pasta_raiz))
        pasta_encontrada = next((pasta for pasta in pastas if nome in pasta.name), None)
        return pasta_encontrada

    def obter_arquivo_por_nome(self, pasta: Folder, nome):
        arquivos = list(self.listar_arquivos(pasta))
        arquivo_encontrado = next((arquivo for arquivo in arquivos if nome in arquivo.name), None)
        return arquivo_encontrado
