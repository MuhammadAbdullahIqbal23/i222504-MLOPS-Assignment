# PowerShell script for setting up Python virtual environment on Windows

Write-Host "Setting up Python virtual environment..." -ForegroundColor Green

# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Upgrade pip
python -m pip install --upgrade pip

# Install requirements
pip install -r requirements.txt

Write-Host "Virtual environment setup complete!" -ForegroundColor Green
Write-Host "To activate the virtual environment, run: .\venv\Scripts\Activate.ps1" -ForegroundColor Yellow
Write-Host "To deactivate, run: deactivate" -ForegroundColor Yellow

