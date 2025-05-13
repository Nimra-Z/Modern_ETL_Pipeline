# Modern ETL Pipeline

A robust and scalable ETL (Extract, Transform, Load) pipeline built with Python and Docker, demonstrating best practices in data engineering.

## Features

- Multi-source data extraction (CSV, API, Database)
- Data validation and transformation
- Error handling and logging
- Data quality checks
- Containerized deployment with Docker
- Monitoring and alerting
- Scalable, modular architecture

## Project Structure

```
etl-pipeline/
├── src/
│ ├── extractors/ # Data extraction modules
│ ├── transformers/ # Data transformation logic
│ └── loaders/ # Data loading modules
│ 
├── tests/ # Unit and integration tests
├── config/ # Configuration files
├── logs/ # Log files
├── data/ # Input data files
├── requirements.txt # Project dependencies
├── Dockerfile # Docker build file
├── docker-compose.yml # Docker Compose orchestration
└── README.md # Project documentation
```

## Prerequisites

- Python 3.10+
- Docker & Docker Compose

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/Nimra-Z/Modern_ETL_Pipeline.git
cd etl-pipeline
```

### 2. Set up environment variables

**Never commit your real `.env` file to version control!**

### 3. Build and run with Docker Compose

```bash
docker-compose up --build
```

### 4. Query your data

```bash
docker-compose exec postgres psql -U $TARGET_DB_USER -d target_db
# Then in psql:
# \dt
# SELECT * FROM users;
# \q
```

## Configuration

Edit `config/config.yaml` to set up:
- Data sources (CSV, API, Database)
- Target database connection (uses environment variables)
- Transformation rules
- Logging and error handling
- Monitoring and alerting

## Usage (Local Python, for development)

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python src/main.py
```

## Testing

Run the test suite:
```bash
PYTHONPATH=src pytest tests/
```
> **Tip:** If you use a `src/` layout, always set `PYTHONPATH=src` when running tests.

## Monitoring

The pipeline includes:
- Detailed logging (see `logs/etl_pipeline.log`)
- Performance metrics
- Error tracking
- Data quality reports

## Environment Variables

**Create a `.env` file (not committed) with:**

```
DB_USER=your_db_user
DB_PASSWORD=your_db_password
TARGET_DB_USER=your_target_db_user
TARGET_DB_PASSWORD=your_target_db_password
API_TOKEN=your_api_token
SLACK_WEBHOOK=your_slack_webhook
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
