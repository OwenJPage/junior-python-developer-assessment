# Junior Python/AWS Developer Assessment

## Setup

1. Make sure you have Docker and the Docker Compose plugin installed, and that the Docker service is running (on Windows, make sure Docker Desktop is running). If you do not have Docker installed, you can do so [here](https://docs.docker.com/get-started/get-docker/).
2. Once you have verified Docker is installed and running, open a command line (cmd/PowerShell/terminal) and navigate to the project's root directory (where this README is located).
3. Run the command `docker compose up --build` to build and run the containers. This will start the PostgreSQL database, initialise it with tables and records, and start the API.

## Design process (choices and reasoning)

### Docker
The first decision I made before any other was to containerise my application using Docker Compose, this would automate the installation and setup process, and avoid any headaches which could be caused by differences between systems and Python versions. This is especially pertinent when using a database as installation methods and configurations can vary widely across different operating systems.

### Database
I opted to use PostgreSQL as the database for several reasons:
1. It is widely used, performant, and stable.
2. 