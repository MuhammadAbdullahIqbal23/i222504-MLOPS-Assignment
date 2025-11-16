from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import os
import subprocess
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from git import Repo
import shutil

# Default arguments for the DAG
default_args = {
    'owner': 'mlops_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'nasa_apod_etl_pipeline',
    default_args=default_args,
    description='NASA APOD ETL Pipeline with Airflow, DVC, and Postgres',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=days_ago(1),
    catchup=False,
    tags=['mlops', 'etl', 'nasa', 'apod'],
)


def extract_nasa_apod_data(**context):
    # NASA APOD API endpoint
    api_url = "https://api.nasa.gov/planetary/apod"
    api_key = "f9WLcpt1YKH2BEFcG204HBMROuE0UGXu1XldtoV8"
    
    # Parameters for the API request
    params = {
        'api_key': api_key,
        'hd': 'true'  # Request high-definition image URL if available
    }
    
    try:
        # Make API request
        print(f"Connecting to NASA APOD API: {api_url}")
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse JSON response
        data = response.json()
        print(f"Successfully retrieved data: {json.dumps(data, indent=2)}")
        
        # Create data directory if it doesn't exist
        data_dir = Path('/opt/airflow/data')
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        filename = f"nasa_apod_{execution_date}.json"
        filepath = data_dir / filename
        
        # Save raw data to JSON file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Data successfully saved to: {filepath}")
        
        # Push file path to XCom for downstream tasks
        return str(filepath)
        
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to NASA APOD API: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during extraction: {str(e)}")
        raise


def transform_nasa_apod_data(**context):
    # Pull file path from previous task via XCom
    ti = context['ti']
    json_filepath = ti.xcom_pull(task_ids='extract_nasa_apod_data')
    
    if not json_filepath:
        raise ValueError("No file path received from extract task")
    
    try:
        # Read JSON file
        print(f"Reading JSON file: {json_filepath}")
        with open(json_filepath, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Extract fields of interest
        transformed_data = {
            'date': raw_data.get('date', ''),
            'title': raw_data.get('title', ''),
            'explanation': raw_data.get('explanation', ''),
            'url': raw_data.get('url', ''),
            'hdurl': raw_data.get('hdurl', ''),
            'media_type': raw_data.get('media_type', ''),
            'service_version': raw_data.get('service_version', ''),
            'copyright': raw_data.get('copyright', '')
        }
        
        # Create Pandas DataFrame
        df = pd.DataFrame([transformed_data])
        
        # Ensure date is datetime type
        df['date'] = pd.to_datetime(df['date'])
        
        print(f"Transformed data:\n{df.to_string()}")
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {df.columns.tolist()}")
        
        # Save to CSV
        data_dir = Path('/opt/airflow/data')
        csv_filepath = data_dir / 'apod_data.csv'
        df.to_csv(csv_filepath, index=False, encoding='utf-8')
        print(f"Transformed data saved to CSV: {csv_filepath}")
        
        # Return CSV file path for downstream tasks
        return str(csv_filepath)
        
    except FileNotFoundError as e:
        print(f"JSON file not found: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during transformation: {str(e)}")
        raise


def load_nasa_apod_data(**context):
    # Pull CSV file path from previous task via XCom
    ti = context['ti']
    csv_filepath = ti.xcom_pull(task_ids='transform_nasa_apod_data')

    if not csv_filepath:
        raise ValueError("No CSV file path received from transform task")

    try:
        # Read CSV file
        print(f"Reading CSV file: {csv_filepath}")
        df = pd.read_csv(csv_filepath)

        # PostgreSQL connection parameters
        conn_params = {
            'host': 'postgres',
            'database': 'airflow',
            'user': 'airflow',
            'password': 'airflow',
            'port': 5432
        }

        # Connect to PostgreSQL
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Ensure table exists (create if not exists)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS nasa_apod_data (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            title VARCHAR(500) NOT NULL,
            explanation TEXT,
            url VARCHAR(1000),
            hdurl VARCHAR(1000),
            media_type VARCHAR(50),
            service_version VARCHAR(50),
            copyright VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("PostgreSQL table 'nasa_apod_data' ensured to exist")

        # Prepare data for insertion (upsert - update if exists, insert if not)
        upsert_sql = """
        INSERT INTO nasa_apod_data (date, title, explanation, url, hdurl, media_type, service_version, copyright)
        VALUES %s
        ON CONFLICT (date)
        DO UPDATE SET
            title = EXCLUDED.title,
            explanation = EXCLUDED.explanation,
            url = EXCLUDED.url,
            hdurl = EXCLUDED.hdurl,
            media_type = EXCLUDED.media_type,
            service_version = EXCLUDED.service_version,
            copyright = EXCLUDED.copyright,
            updated_at = CURRENT_TIMESTAMP;
        """

        # Convert DataFrame to list of tuples
        values = [
            (
                row['date'],
                row['title'],
                row['explanation'] if pd.notna(row['explanation']) else None,
                row['url'] if pd.notna(row['url']) else None,
                row['hdurl'] if pd.notna(row['hdurl']) else None,
                row['media_type'] if pd.notna(row['media_type']) else None,
                row['service_version'] if pd.notna(row['service_version']) else None,
                row['copyright'] if pd.notna(row['copyright']) else None
            )
            for _, row in df.iterrows()
        ]

        # Execute upsert
        execute_values(cursor, upsert_sql, values)
        conn.commit()

        print(f"Successfully loaded {len(values)} record(s) into PostgreSQL")

        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM nasa_apod_data")
        count = cursor.fetchone()[0]
        print(f"Total records in database: {count}")

        cursor.close()
        conn.close()

        # Ensure CSV file exists locally
        csv_path = Path(csv_filepath)
        if not csv_path.exists():
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"CSV file ensured at: {csv_path}")

        return str(csv_filepath)

    except psycopg2.Error as e:
        print(f"PostgreSQL error: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during loading: {str(e)}")
        raise


from pathlib import Path
from datetime import datetime
import shutil
import subprocess

def version_data_with_dvc(git_repo_url: str, **context):
    """
    Version NASA APOD CSV data using DVC and push metadata to GitHub.
    """
    ti = context['ti']
    csv_filepath = ti.xcom_pull(task_ids='load_nasa_apod_data')
    
    # Get GitHub token from environment variable
    github_token = os.getenv('GITHUB_TOKEN')

    if not csv_filepath:
        raise ValueError("No CSV file path received from load task")

    try:
        csv_path = Path(csv_filepath)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_filepath}")

        # Project root
        project_root = Path('/opt/airflow/project')
        project_root.mkdir(parents=True, exist_ok=True)

        # Initialize DVC if not already initialized
        dvc_dir = project_root / '.dvc'
        if not dvc_dir.exists():
            print("Initializing DVC repository...")
            result = subprocess.run(
                ['dvc', 'init', '--no-scm'],
                cwd=str(project_root),
                capture_output=True,
                text=True
            )
            if result.returncode != 0 and 'already initialized' not in result.stderr.lower():
                print(f"DVC init error: {result.stderr}")

        # Prepare data directory
        project_data_dir = project_root / 'data'
        project_data_dir.mkdir(parents=True, exist_ok=True)

        # Create a timestamped filename to avoid SameFileError
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        new_csv_name = f"{csv_path.stem}_{timestamp}{csv_path.suffix}"
        project_csv_path = project_data_dir / new_csv_name

        # Copy CSV
        shutil.copy2(csv_path, project_csv_path)
        print(f"Copied CSV to project directory as: {project_csv_path}")

        # Add CSV to DVC
        csv_relative_path = project_csv_path.relative_to(project_root)
        print(f"Adding {csv_relative_path} to DVC...")
        result = subprocess.run(
            ['dvc', 'add', str(csv_relative_path)],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=True
        )
        print(f"DVC add output: {result.stdout}")

        # DVC metadata file
        dvc_metadata_file = project_root / f"{csv_relative_path}.dvc"
        if not dvc_metadata_file.exists():
            raise FileNotFoundError("DVC metadata file not found after adding file to DVC")

        # Initialize Git if needed
        git_dir = project_root / '.git'
        if not git_dir.exists():
            subprocess.run(['git', 'init'], cwd=str(project_root), check=True)

        # Configure Git to trust this directory (fixes "dubious ownership" error)
        # This is needed when the repository is owned by a different user
        project_root_str = str(project_root)
        
        # Check if directory is already in safe.directory list
        safe_dir_result = subprocess.run(
            ['git', 'config', '--global', '--get-all', 'safe.directory'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        safe_directories = [d.strip() for d in safe_dir_result.stdout.strip().split('\n') if d.strip()] if safe_dir_result.returncode == 0 else []
        
        if project_root_str not in safe_directories:
            subprocess.run(
                ['git', 'config', '--global', '--add', 'safe.directory', project_root_str],
                cwd=str(project_root),
                check=True
            )
            print(f"Added {project_root_str} to Git safe.directory")
        else:
            print(f"Directory {project_root_str} already in Git safe.directory")

        # Configure Git user name and email (required for commits)
        subprocess.run(
            ['git', 'config', 'user.name', 'MuhammadAbdullahIqbal23'],
            cwd=str(project_root),
            check=True
        )
        subprocess.run(
            ['git', 'config', 'user.email', 'abdullahiqbal1133@gmail.com'],
            cwd=str(project_root),
            check=True
        )
        print("Configured Git user name and email")

        # Build authenticated URL with token for pushing
        # Format: https://token@github.com/username/repo.git
        if github_token and git_repo_url.startswith('https://github.com/'):
            # Insert token into URL for authentication
            authenticated_url = git_repo_url.replace('https://github.com/', f'https://{github_token}@github.com/')
        else:
            authenticated_url = git_repo_url
        
        # Check if 'origin' remote already exists and handle it properly
        # First, try to get the URL of origin remote
        get_url_result = subprocess.run(
            ['git', 'remote', 'get-url', 'origin'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        
        if get_url_result.returncode == 0:
            # Remote exists, update URL with token for authentication
            current_url = get_url_result.stdout.strip()
            # Always update to authenticated URL to ensure token is used
            subprocess.run(
                ['git', 'remote', 'set-url', 'origin', authenticated_url],
                cwd=str(project_root),
                check=True
            )
            print(f"Updated Git remote 'origin' URL with authentication")
        else:
            # Remote doesn't exist, add it with authenticated URL
            # But first check if there are any remotes at all
            remotes_check = subprocess.run(
                ['git', 'remote'],
                cwd=str(project_root),
                capture_output=True,
                text=True
            )
            
            # Try to add the remote with authenticated URL
            add_result = subprocess.run(
                ['git', 'remote', 'add', 'origin', authenticated_url],
                cwd=str(project_root),
                capture_output=True,
                text=True
            )
            
            if add_result.returncode == 0:
                print(f"Added Git remote 'origin' with authentication")
            else:
                # Remote might have been added between checks, verify
                verify_result = subprocess.run(
                    ['git', 'remote', 'get-url', 'origin'],
                    cwd=str(project_root),
                    capture_output=True,
                    text=True
                )
                if verify_result.returncode == 0:
                    # Update to authenticated URL
                    subprocess.run(
                        ['git', 'remote', 'set-url', 'origin', authenticated_url],
                        cwd=str(project_root),
                        check=True
                    )
                    print(f"Updated existing Git remote 'origin' with authentication")
                else:
                    # If it still doesn't exist, raise the error
                    print(f"Error adding remote: {add_result.stderr}")
                    raise subprocess.CalledProcessError(
                        add_result.returncode,
                        add_result.args,
                        add_result.stdout,
                        add_result.stderr
                    )

        # Check if repository has any commits
        log_check = subprocess.run(
            ['git', 'log', '--oneline'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        has_commits = bool(log_check.stdout.strip())
        
        if not has_commits:
            # Repository has no commits, make an initial empty commit first
            # This creates the default branch (usually 'main' in newer Git versions)
            subprocess.run(
                ['git', 'commit', '--allow-empty', '-m', 'Initial commit'],
                cwd=str(project_root),
                check=True
            )
            print("Created initial empty commit")
        
        # Ensure we're on the main branch (create if needed)
        branch_check = subprocess.run(
            ['git', 'branch', '--show-current'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        current_branch = branch_check.stdout.strip()
        
        # Check if main branch exists
        branches_check = subprocess.run(
            ['git', 'branch'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        branches = [b.strip().replace('*', '').strip() for b in branches_check.stdout.splitlines() if b.strip()]
        
        if 'main' not in branches:
            # If default branch is not 'main', rename it or create main
            if current_branch and current_branch != 'main':
                # Rename current branch to main
                subprocess.run(
                    ['git', 'branch', '-m', current_branch, 'main'],
                    cwd=str(project_root),
                    check=True
                )
                print(f"Renamed branch '{current_branch}' to 'main'")
            elif current_branch == 'main':
                # Already on main but it's not showing in branch list (shouldn't happen)
                print("Already on 'main' branch")
            else:
                # No branch exists yet or we're in detached HEAD, create main
                # First check if we have commits
                if has_commits:
                    subprocess.run(
                        ['git', 'checkout', '-b', 'main'],
                        cwd=str(project_root),
                        check=True
                    )
                    print("Created and switched to 'main' branch")
                else:
                    # No commits yet, the initial commit should have created a branch
                    # Check what branch we're on now
                    branch_check_after = subprocess.run(
                        ['git', 'branch', '--show-current'],
                        cwd=str(project_root),
                        capture_output=True,
                        text=True
                    )
                    branch_after = branch_check_after.stdout.strip()
                    if branch_after and branch_after != 'main':
                        subprocess.run(
                            ['git', 'branch', '-m', branch_after, 'main'],
                            cwd=str(project_root),
                            check=True
                        )
                        print(f"Renamed branch '{branch_after}' to 'main'")
                    else:
                        print("On 'main' branch")
        elif current_branch != 'main':
            # Switch to main branch if not already on it
            subprocess.run(
                ['git', 'checkout', 'main'],
                cwd=str(project_root),
                check=True
            )
            print(f"Switched to 'main' branch from '{current_branch}'")
        else:
            print("Already on 'main' branch")

        # Add DVC metadata and commit
        subprocess.run(['git', 'add', str(dvc_metadata_file)], cwd=str(project_root), check=True)
        
        # Check if there are changes to commit
        status = subprocess.run(
            ['git', 'status', '--porcelain'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        if status.stdout.strip():
            commit_result = subprocess.run(
                ['git', 'commit', '-m', f"Add DVC tracked file {new_csv_name}"],
                cwd=str(project_root),
                capture_output=True,
                text=True
            )
            if commit_result.returncode == 0:
                print(f"Committed DVC metadata: {new_csv_name}")
            else:
                print(f"Git commit error: {commit_result.stderr}")
                print(f"Git commit stdout: {commit_result.stdout}")
                raise subprocess.CalledProcessError(
                    commit_result.returncode,
                    commit_result.args,
                    commit_result.stdout,
                    commit_result.stderr
                )
        else:
            print("No changes to commit (file may already be tracked)")
        
        # Verify remote is configured before pushing
        remote_check = subprocess.run(
            ['git', 'remote', '-v'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        print(f"Git remotes configured: {remote_check.stdout}")
        
        # Check current branch
        branch_check = subprocess.run(
            ['git', 'branch', '--show-current'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        current_branch = branch_check.stdout.strip()
        print(f"Current branch: {current_branch}")
        
        # Pull remote changes first to avoid push rejection
        print(f"Pulling latest changes from origin/{current_branch}...")
        pull_result = subprocess.run(
            ['git', 'pull', '--rebase', 'origin', current_branch],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        if pull_result.returncode == 0:
            print(f"Successfully pulled and rebased changes from remote")
        else:
            print(f"Pull warning/error (may be first push): {pull_result.stderr}")
        
        # Push to remote (with error handling)
        print(f"Attempting to push to origin/{current_branch}...")
        push_result = subprocess.run(
            ['git', 'push', '-u', 'origin', current_branch],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        
        print(f"Push return code: {push_result.returncode}")
        print(f"Push stdout: {push_result.stdout}")
        print(f"Push stderr: {push_result.stderr}")
        
        if push_result.returncode == 0:
            print(f"✅ SUCCESS: DVC metadata committed and pushed to GitHub: {dvc_metadata_file}")
        else:
            # Check if it's an authentication error or if the branch doesn't exist on remote
            error_msg = push_result.stderr.lower()
            if 'authentication' in error_msg or 'permission denied' in error_msg or 'not found' in error_msg:
                print(f"❌ ERROR: Could not push to remote (authentication/permission issue)")
                print(f"Full error: {push_result.stderr}")
                print("DVC metadata committed locally but NOT pushed to GitHub")
                print("To push manually, run: git push -u origin main")
            elif 'no upstream branch' in error_msg or 'does not exist' in error_msg:
                # Try pushing without -u flag first, or create the branch
                print("Remote branch doesn't exist, attempting to create it...")
                push_result2 = subprocess.run(
                    ['git', 'push', 'origin', current_branch],
                    cwd=str(project_root),
                    capture_output=True,
                    text=True
                )
                print(f"Second push attempt - return code: {push_result2.returncode}")
                print(f"Second push stdout: {push_result2.stdout}")
                print(f"Second push stderr: {push_result2.stderr}")
                if push_result2.returncode == 0:
                    print(f"✅ SUCCESS: DVC metadata committed and pushed to GitHub: {dvc_metadata_file}")
                else:
                    print(f"❌ ERROR: Could not push to remote: {push_result2.stderr}")
                    print("DVC metadata committed locally but NOT pushed to GitHub")
            else:
                print(f"❌ ERROR: Git push failed")
                print(f"Full error: {push_result.stderr}")
                print("DVC metadata committed locally but NOT pushed to GitHub")
                # Don't raise an error - the commit was successful, push failure is not critical

        return str(dvc_metadata_file)

    except subprocess.CalledProcessError as e:
        print(f"Subprocess error: {e.stderr}")
        raise
    except Exception as e:
        print(f"Unexpected error during DVC versioning: {str(e)}")
        raise




def commit_dvc_metadata_to_git(**context):
    """
    Commits DVC metadata to a Git repository and pushes to a remote (e.g., GitHub).
    """
    ti = context['ti']
    dvc_metadata_filepath = ti.xcom_pull(task_ids='version_data_with_dvc')

    if not dvc_metadata_filepath:
        print("No DVC metadata file path received. Skipping Git commit.")
        return None

    dvc_metadata_path = Path(dvc_metadata_filepath)
    project_root = Path('/opt/airflow/project')

    # Clean up any stale Git lock files
    config_lock = project_root / '.git' / 'config.lock'
    if config_lock.exists():
        try:
            config_lock.unlink()
            print("Removed stale Git config lock file")
        except Exception as e:
            print(f"Warning: Could not remove lock file: {e}")

    # Configure Git safe.directory (fixes "dubious ownership" error)
    project_root_str = str(project_root)
    safe_dir_result = subprocess.run(
        ['git', 'config', '--global', '--get-all', 'safe.directory'],
        cwd=str(project_root),
        capture_output=True,
        text=True
    )
    safe_directories = [d.strip() for d in safe_dir_result.stdout.strip().split('\n') if d.strip()] if safe_dir_result.returncode == 0 else []
    
    if project_root_str not in safe_directories:
        subprocess.run(
            ['git', 'config', '--global', '--add', 'safe.directory', project_root_str],
            cwd=str(project_root),
            check=True
        )
        print(f"Added {project_root_str} to Git safe.directory")

    # Initialize Git repo if not exists
    if not (project_root / '.git').exists():
        print("Initializing Git repository...")
        subprocess.run(['git', 'init'], cwd=str(project_root), check=True)
        repo = Repo(str(project_root))
    else:
        repo = Repo(str(project_root))

    # Configure Git user using subprocess (more reliable than config_writer)
    subprocess.run(
        ['git', 'config', 'user.name', 'MuhammadAbdullahIqbal23'],
        cwd=str(project_root),
        check=True
    )
    subprocess.run(
        ['git', 'config', 'user.email', 'abdullahiqbal1133@gmail.com'],
        cwd=str(project_root),
        check=True
    )
    print("Configured Git user name and email")

    # Stage DVC file and .gitignore/.dvcignore if they exist
    files_to_add = [str(dvc_metadata_path.relative_to(project_root))]
    for f in ['.dvcignore', '.gitignore']:
        path = project_root / f
        if path.exists():
            files_to_add.append(str(path.relative_to(project_root)))
    repo.index.add(files_to_add)

    # Get GitHub token for authentication
    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is required")
    print("Using GitHub token from environment variable")
    
    # Ensure remote is configured and update URL with token for authentication
    git_repo_url = 'https://github.com/MuhammadAbdullahIqbal23/i222504-MLOPS-Assignment.git'
    
    # Build authenticated URL with token (always use token)
    authenticated_url = git_repo_url.replace('https://github.com/', f'https://{github_token}@github.com/')
    print(f"Built authenticated URL (first 50 chars): {authenticated_url[:50]}...")
    print(f"Token present in URL: {'@github.com' in authenticated_url}")
    
    # Check if remote exists and update it - always use subprocess to read/write
    remote_check = subprocess.run(
        ['git', 'remote', 'get-url', 'origin'],
        cwd=str(project_root),
        capture_output=True,
        text=True
    )
    
    if remote_check.returncode == 0:
        # Remote exists, check if it needs updating
        current_url = remote_check.stdout.strip()
        print(f"Current remote URL: {current_url}")
        
        # Check if URL already has token (contains @github.com)
        if '@github.com' not in current_url:
            # Update to authenticated URL
            update_result = subprocess.run(
                ['git', 'remote', 'set-url', 'origin', authenticated_url],
                cwd=str(project_root),
                capture_output=True,
                text=True,
                check=True
            )
            print(f"Update command return code: {update_result.returncode}")
            if update_result.stderr:
                print(f"Update command stderr: {update_result.stderr}")
            
            # Verify the update worked
            verify_check = subprocess.run(
                ['git', 'remote', 'get-url', 'origin'],
                cwd=str(project_root),
                capture_output=True,
                text=True
            )
            updated_url = verify_check.stdout.strip()
            print(f"Updated remote URL with authentication token")
            print(f"New remote URL: {updated_url[:50]}...")  # Show first 50 chars to avoid exposing full token
            if '@github.com' not in updated_url:
                print(f"⚠️  WARNING: Remote URL update may have failed - token not detected")
                print(f"Expected URL format: https://token@github.com/...")
                print(f"Actual URL: {updated_url}")
        else:
            print(f"Remote URL already has authentication")
    else:
        # Remote doesn't exist, add it with authenticated URL
        subprocess.run(
            ['git', 'remote', 'add', 'origin', authenticated_url],
            cwd=str(project_root),
            check=True
        )
        print(f"Added remote 'origin' with authentication token")
        
        # Verify it was added correctly
        verify_check = subprocess.run(
            ['git', 'remote', 'get-url', 'origin'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        if verify_check.returncode == 0:
            added_url = verify_check.stdout.strip()
            print(f"Verified remote URL: {added_url[:50]}...")

    # Commit changes
    if repo.is_dirty() or repo.untracked_files:
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        commit_message = f"Add DVC metadata for NASA APOD data - {execution_date}"
        commit = repo.index.commit(commit_message)
        print(f"Committed to Git: {commit.hexsha} - {commit_message}")

        # Verify remote URL before pushing - force refresh
        remote_url_check = subprocess.run(
            ['git', 'remote', 'get-url', 'origin'],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        final_remote_url = remote_url_check.stdout.strip()
        print(f"Remote URL before push: {final_remote_url}")
        
        # If URL still doesn't have token, update it one more time right before push
        if '@github.com' not in final_remote_url and github_token:
            print("⚠️  Remote URL missing token, updating again before push...")
            subprocess.run(
                ['git', 'remote', 'set-url', 'origin', authenticated_url],
                cwd=str(project_root),
                check=True
            )
            # Verify again
            final_check = subprocess.run(
                ['git', 'remote', 'get-url', 'origin'],
                cwd=str(project_root),
                capture_output=True,
                text=True
            )
            final_remote_url = final_check.stdout.strip()
            print(f"Final remote URL after update: {final_remote_url[:50]}...")
            if '@github.com' not in final_remote_url:
                print(f"❌ CRITICAL: Remote URL still doesn't have token after update!")
                print(f"This may indicate a permissions issue or git config problem.")
        
        # Push to remote - always use authenticated URL directly
        if 'origin' in repo.remotes:
            print("Pushing commit to remote...")
            try:
                # Get current branch
                branch_check = subprocess.run(
                    ['git', 'branch', '--show-current'],
                    cwd=str(project_root),
                    capture_output=True,
                    text=True
                )
                current_branch = branch_check.stdout.strip() or 'main'
                
                # Always push directly to authenticated URL (bypasses remote config issues)
                push_cmd = ['git', 'push', authenticated_url, current_branch]
                print(f"Pushing directly to authenticated URL: {authenticated_url[:50]}...")
                print(f"Branch: {current_branch}")
                
                push_result = subprocess.run(
                    push_cmd,
                    cwd=str(project_root),
                    capture_output=True,
                    text=True
                )
                print(f"Push return code: {push_result.returncode}")
                print(f"Push stdout: {push_result.stdout}")
                print(f"Push stderr: {push_result.stderr}")
                
                if push_result.returncode == 0:
                    print("✅ SUCCESS: Push successful - commits are now on GitHub!")
                else:
                    print(f"❌ ERROR: Git push failed: {push_result.stderr}")
                    print("Commit was successful locally but NOT pushed to GitHub")
                    print(f"Attempted to push to: {authenticated_url[:50]}...")
                    print("To push manually, run: git push origin main")
            except Exception as e:
                print(f"❌ ERROR: Git push exception: {str(e)}")
                print("Commit was successful locally. You may need to push manually.")
        else:
            print("❌ WARNING: No remote named 'origin' configured. Skipping push.")
            print("Commit was successful locally but NOT pushed to GitHub")
        
        return commit.hexsha
    else:
        print("No changes to commit. Skipping Git commit.")
        return repo.head.commit.hexsha if repo.head.is_valid() else None




# Step 1: Extract Task
extract_task = PythonOperator(
    task_id='extract_nasa_apod_data',
    python_callable=extract_nasa_apod_data,
    dag=dag,
)

# Step 2: Transform Task
transform_task = PythonOperator(
    task_id='transform_nasa_apod_data',
    python_callable=transform_nasa_apod_data,
    dag=dag,
)

# Step 3: Load Task
load_task = PythonOperator(
    task_id='load_nasa_apod_data',
    python_callable=load_nasa_apod_data,
    dag=dag,
)

# Step 4: DVC Versioning Task
version_task = PythonOperator(
    task_id='version_data_with_dvc',
    python_callable=version_data_with_dvc,
    op_kwargs={'git_repo_url': 'https://github.com/MuhammadAbdullahIqbal23/i222504-MLOPS-Assignment.git'},
    dag=dag,
)


# Step 5: Git Commit Task
git_commit_task = PythonOperator(
    task_id='commit_dvc_metadata_to_git',
    python_callable=commit_dvc_metadata_to_git,
    dag=dag,
)

# Set task dependencies - sequential pipeline
extract_task >> transform_task >> load_task >> version_task >> git_commit_task

