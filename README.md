# document-storage-project

_Developed by:_

-   Enzo Bozzani Martins - R.A.: 24.122.020-1
-   Igor Augusto Fiorini Rossi - R.A.: 24.122.023-5

_Sections_:

-   [Prerequisites](#prerequisites)
-   [Installation](#installation)
-   [Run](#run)

### Prerequisites

This application uses [Python](https://www.python.org/) with [Poetry](https://python-poetry.org/) as the package manager.

Make sure you have the following installed on your machine:

-   [Python](https://www.python.org/). Version ^3.11
-   [Poetry](https://python-poetry.org/). This project was built using Poetry, but it's also possible to download its dependencies using pip.

### Installation

Create a new virtual envinronment with your favorite virtual environment manager (pyenv-virtualenv, venv, etc)

Clone the repository:

```
git clone git@github.com:EnzoBozzani/document-storage-project.git
```

Make sure to activate your virtual environment on the project folder.

You can use MongoDB and PostgreSQL on your local machine or on remote server. Just set env variables with the connection URLs, using the variable names as specified on .env.example:

```
POSTGRES_URL=""
MONGO_URL=""
```

### Run

To run the application:

```
python3 app.py
```

This will start the data transfer. Logs are outputted on terminal

TASKS:

-   [ ] Reuse seeders to populate Postgres
-   [ ] Documentation
