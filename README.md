# MLOps Assignment 3: NASA APOD ETL Pipeline

This project implements a robust, reproducible Extract, Transform, Load (ETL) pipeline using essential MLOps tools: **Airflow**, **Astronomer**, **DVC**, and **Postgres** within a unified, containerized environment.

## 🏗️ Project Structure

```
Assignment 3/
├── dags/                      # Airflow DAG definitions
│   └── nasa_apod_etl_pipeline.py
├── plugins/                   # Airflow custom plugins
├── data/                      # Data storage (tracked by DVC)
├── logs/                      # Airflow logs
├── config/                    # Configuration files
├── docker-compose.yml         # Docker Compose configuration
├── Dockerfile                 # Custom Airflow image
├── requirements.txt           # Python dependencies
├── .dvcignore                # DVC ignore patterns
├── .gitignore                # Git ignore patterns
└── README.md                 # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+ installed
- Docker Desktop installed and running
- Docker Compose v2.0+
- At least 4GB RAM available for Docker
- Git (for DVC)

### Local Development Setup (Python Virtual Environment)

For local development and testing of DAG code:

**Windows:**
```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1
# OR if PowerShell execution policy is restricted:
.\venv\Scripts\activate.bat

# Install dependencies
pip install -r requirements.txt
```

**Linux/Mac:**
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

**Quick Setup Scripts:**
- Windows: Run `setup_venv.bat` or `setup_venv.ps1`
- Linux/Mac: Run `bash setup_venv.sh`

**Note:** The virtual environment is for local development. The Docker containers use their own Python environments.

### Docker Setup Instructions

1. **Clone/Download the project** (if applicable)

2. **Set Airflow user ID** (Linux/Mac only):
   ```bash
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```
   On Windows, you can skip this step or create a `.env` file with:
   ```
   AIRFLOW_UID=50000
   ```

3. **Initialize Airflow database**:
   ```bash
   docker-compose up airflow-init
   ```

4. **Start all services**:
   ```bash
   docker-compose up -d
   ```

5. **Access Airflow UI**:
   - Open your browser and navigate to: `http://localhost:8080`
   - Default credentials:
     - Username: `airflow`
     - Password: `airflow`

6. **Trigger the DAG**:
   - Navigate to the DAGs page in Airflow UI
   - Find `nasa_apod_etl_pipeline`
   - Toggle it ON (if paused)
   - Click "Trigger DAG" to run manually, or wait for scheduled execution (daily)
   
   **Note**: DVC and Git repositories will be automatically initialized on first run if they don't exist.

## 📋 Pipeline Workflow

The Airflow DAG (`nasa_apod_etl_pipeline`) executes the following sequential steps:

### ✅ Step 1: Data Extraction (E) - **IMPLEMENTED**
- Connects to NASA APOD API endpoint (`https://api.nasa.gov/planetary/apod`)
- Retrieves daily astronomy picture data using `DEMO_KEY`
- Saves raw JSON data to `/opt/airflow/data/nasa_apod_YYYY-MM-DD.json`
- Returns file path via XCom for downstream tasks

### ✅ Step 2: Data Transformation (T) - **IMPLEMENTED**
- Reads raw JSON file from Step 1
- Extracts specific fields: `date`, `title`, `explanation`, `url`, `hdurl`, `media_type`, `service_version`, `copyright`
- Restructures data into a clean Pandas DataFrame
- Saves transformed data to CSV file: `/opt/airflow/data/apod_data.csv`
- Returns CSV file path via XCom

### ✅ Step 3: Data Loading (L) - **IMPLEMENTED**
- Reads transformed CSV file from Step 2
- Creates `nasa_apod_data` table in PostgreSQL (if not exists)
- Performs UPSERT operation (inserts new records or updates existing ones based on date)
- Persists data to PostgreSQL database
- Ensures CSV file is saved locally
- Returns CSV file path via XCom for DVC versioning

### ✅ Step 4: Data Versioning (DVC) - **IMPLEMENTED**
- Initializes DVC repository if not already initialized
- Copies CSV file to project directory (`/opt/airflow/project/data/`)
- Adds CSV file to DVC version control using `dvc add`
- Creates/updates DVC metadata file (`apod_data.csv.dvc`)
- Returns `.dvc` metadata file path via XCom

### ✅ Step 5: Code Versioning (Git) - **IMPLEMENTED**
- Initializes Git repository if not already initialized
- Configures Git user (MLOps Pipeline)
- Stages the DVC metadata file (`.dvc`) to Git
- Commits changes with descriptive message including execution date
- Links pipeline code to the exact version of data it produced
- Returns commit hash

## 🛠️ Technologies Used

- **Apache Airflow 2.8.0**: Workflow orchestration
- **PostgreSQL 13**: Database for metadata and transformed data
- **DVC**: Data version control
- **Docker & Docker Compose**: Containerization
- **Python 3.11**: Programming language

## 📝 API Information

The pipeline uses the NASA Astronomy Picture of the Day (APOD) API:
- **Endpoint**: `https://api.nasa.gov/planetary/apod`
- **API Key**: `DEMO_KEY` (for testing)
- **Documentation**: https://api.nasa.gov/

## 🔧 Development

### Using Virtual Environment for Local Development

After setting up the virtual environment, you can:

1. **Test DAG code locally** (without Airflow):
   ```bash
   # Activate venv first
   python -c "from dags.nasa_apod_etl_pipeline import *; print('DAG loaded successfully')"
   ```

2. **Run Python scripts** that use the same dependencies:
   ```bash
   python your_script.py
   ```

3. **Install additional packages** for development:
   ```bash
   pip install package-name
   pip freeze > requirements.txt  # Update requirements
   ```

4. **Deactivate virtual environment** when done:
   ```bash
   deactivate
   ```

### View Logs
```bash
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

### Stop Services
```bash
docker-compose down
```

### Stop and Remove Volumes
```bash
docker-compose down -v
```

### Rebuild Images
```bash
docker-compose build --no-cache
```

## 📊 Monitoring

- **Airflow UI**: http://localhost:8080
- **Airflow Health Check**: http://localhost:8080/health

## 🎯 Pipeline Features

- **Complete ETL Pipeline**: All 5 steps fully implemented and chained sequentially
- **Data Persistence**: Data stored in both PostgreSQL and CSV format
- **Version Control**: Automatic DVC and Git integration
- **Error Handling**: Comprehensive error handling and logging at each step
- **Idempotency**: UPSERT operations prevent duplicate data
- **XCom Communication**: Tasks communicate via Airflow XCom for data flow

## 🔍 Verifying Pipeline Execution

### Check PostgreSQL Data
```bash
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM nasa_apod_data ORDER BY date DESC LIMIT 5;"
```

### Check DVC Status
```bash
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/project && dvc status"
```

### Check Git Commits
```bash
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/project && git log --oneline"
```

### View Generated Files
- Raw JSON: `data/nasa_apod_YYYY-MM-DD.json`
- Transformed CSV: `data/apod_data.csv`
- DVC Metadata: `data/apod_data.csv.dvc`

## 📚 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [DVC Documentation](https://dvc.org/doc)
- [NASA APOD API](https://api.nasa.gov/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## 👤 Author

MLOps Assignment 3 - Student Project

## 📄 License

This project is for educational purposes only.

