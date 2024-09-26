# document-storage-project

_Desenvolvido por:_

-   Enzo Bozzani Martins - R.A.: 24.122.020-1
-   Igor Augusto Fiorini Rossi - R.A.: 24.122.023-5

_Seções_:

-   [Pré-requisitos](#pré-requisitos)
-   [Instalação](#instalação)
-   [Execução](#execução)

### Pré-requisitos

Essa aplicação usa [Python](https://www.python.org/) com [Poetry](https://python-poetry.org/) como gerenciador de dependências.

Tenha instalado em sua máquina:

-   [Python](https://www.python.org/). Versão ^3.11
-   [Poetry](https://python-poetry.org/). O projeto foi construído usando Poetry, mas é possível instalar as dependências usando pip (siga para a próxima seção).

### Instalação

Crie um novo virtual env com seu gerenciador de ambientes virtuais favoritos (pyenv-virtualenv, venv, etc)

Clone o repositório:

```
git clone git@github.com:EnzoBozzani/document-storage-project.git
```

Ative o virtualenv em sua pasta da aplicação.

-   Instale as dependências:

    -   Usando pip (cheque se o pip está no seu virtualenv usando `pip --version`):

        ```
        pip install -r requirements.txt
        ```

    -   Usando poetry:
        ```
        poetry install
        ```

Você pode usar MongoDB e PostgreSQL em sua máquina local ou de maneira remota. Apenas configure as variáveis de ambiente com as URLs de conexão, assim como especificado no arquivo .env.example:

```
POSTGRES_URL=""
MONGO_URL=""
```

Para transferir dados de um banco PostgreSQL, é necessário tem um banco PostgreSQL. Siga o exemplo em [https://github.com/EnzoBozzani/projeto-banco-de-dados](https://github.com/EnzoBozzani/projeto-banco-de-dados), o qual contém migrations, seeder e queries (em SQL).

Feito isso, siga para a próxima seção.

### Execução

Para executar a transferência de dados:

```
python3 transfer-data.py
```

Os logs são exibidos no terminal.

Execute algumas queries com:

```
python3 queries.py
```

TASKS:

-   Descrição das collections
-   Queries
