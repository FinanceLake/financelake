# Fix Python Path for PySpark
# This script finds Python and configures environment variables

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Fixing Python Path for PySpark" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Find Python executable
Write-Host "[1/3] Locating Python installation..." -ForegroundColor Green

$pythonPaths = @()

# Try different methods to find Python
# Method 1: Check if py launcher works
try {
    $pyOutput = py --version 2>&1
    if ($pyOutput -match "Python") {
        $pythonPaths += (py -c "import sys; print(sys.executable)" 2>$null)
        Write-Host "      Found via py launcher: $($pythonPaths[-1])" -ForegroundColor Gray
    }
} catch {}

# Method 2: Check common installation paths
$commonPaths = @(
    "C:\Python313\python.exe",
    "C:\Python312\python.exe",
    "C:\Python311\python.exe",
    "C:\Python310\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe"
)

foreach ($path in $commonPaths) {
    if (Test-Path $path) {
        $pythonPaths += $path
        Write-Host "      Found at: $path" -ForegroundColor Gray
    }
}

# Method 3: Search in PATH (excluding Windows Store)
$pathDirs = $env:PATH -split ';'
foreach ($dir in $pathDirs) {
    if ($dir -notmatch "WindowsApps") {
        $possiblePython = Join-Path $dir "python.exe"
        if (Test-Path $possiblePython) {
            $pythonPaths += $possiblePython
            Write-Host "      Found in PATH: $possiblePython" -ForegroundColor Gray
        }
    }
}

# Remove duplicates
$pythonPaths = $pythonPaths | Select-Object -Unique

if ($pythonPaths.Count -eq 0) {
    Write-Host ""
    Write-Host "[ERROR] No Python installation found!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install Python from: https://www.python.org/downloads/" -ForegroundColor Yellow
    Write-Host "Or use: py --version to check if Python is installed" -ForegroundColor Yellow
    exit 1
}

# Select the first valid Python
$selectedPython = $pythonPaths[0]

Write-Host ""
Write-Host "Selected Python: $selectedPython" -ForegroundColor Green

# Verify Python works
try {
    $version = & $selectedPython --version 2>&1
    Write-Host "Version: $version" -ForegroundColor Gray
} catch {
    Write-Host "[ERROR] Selected Python doesn't work!" -ForegroundColor Red
    exit 1
}

# Step 2: Set environment variables
Write-Host ""
Write-Host "[2/3] Setting environment variables for current session..." -ForegroundColor Green

$env:PYSPARK_PYTHON = $selectedPython
$env:PYSPARK_DRIVER_PYTHON = $selectedPython

Write-Host "      PYSPARK_PYTHON = $selectedPython" -ForegroundColor Gray
Write-Host "      PYSPARK_DRIVER_PYTHON = $selectedPython" -ForegroundColor Gray

# Step 3: Test configuration
Write-Host ""
Write-Host "[3/3] Testing configuration..." -ForegroundColor Green

Write-Host "      Running quick test..." -ForegroundColor Gray

# Create a simple test
$testScript = @"
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("SUCCESS: Spark can find Python!")
spark.stop()
"@

$testFile = "test_python_config.py"
$testScript | Out-File -FilePath $testFile -Encoding UTF8

try {
    $result = & $selectedPython $testFile 2>&1 | Select-String "SUCCESS"
    if ($result) {
        Write-Host "      [OK] Configuration is working!" -ForegroundColor Green
    } else {
        Write-Host "      [WARNING] Test didn't complete, but config is set" -ForegroundColor Yellow
    }
} catch {
    Write-Host "      [WARNING] Test failed, but environment variables are set" -ForegroundColor Yellow
}

# Cleanup
if (Test-Path $testFile) {
    Remove-Item $testFile -Force
}

# Summary
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Configuration Complete!" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "[CURRENT SESSION]" -ForegroundColor Green
Write-Host "Environment variables are set for this PowerShell session only." -ForegroundColor Gray
Write-Host ""
Write-Host "[TO MAKE PERMANENT]" -ForegroundColor Yellow
Write-Host "Run these commands to set for your user account:" -ForegroundColor Gray
Write-Host ""
Write-Host '[System.Environment]::SetEnvironmentVariable("PYSPARK_PYTHON", "' + $selectedPython + '", "User")' -ForegroundColor Cyan
Write-Host '[System.Environment]::SetEnvironmentVariable("PYSPARK_DRIVER_PYTHON", "' + $selectedPython + '", "User")' -ForegroundColor Cyan
Write-Host ""
Write-Host "Or set manually in Windows:" -ForegroundColor Gray
Write-Host "  1. Search 'environment variables' in Windows" -ForegroundColor Gray
Write-Host "  2. Click 'Edit environment variables for your account'" -ForegroundColor Gray
Write-Host "  3. Add New variable:" -ForegroundColor Gray
Write-Host "     Name: PYSPARK_PYTHON" -ForegroundColor Gray
Write-Host "     Value: $selectedPython" -ForegroundColor Gray
Write-Host "  4. Add another:" -ForegroundColor Gray
Write-Host "     Name: PYSPARK_DRIVER_PYTHON" -ForegroundColor Gray
Write-Host "     Value: $selectedPython" -ForegroundColor Gray
Write-Host ""
Write-Host "[TEST YOUR SETUP]" -ForegroundColor Green
Write-Host "  python test_simple.py" -ForegroundColor Cyan
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Output commands for easy copy-paste
Write-Host "[QUICK PERMANENT FIX]" -ForegroundColor Magenta
Write-Host "Copy and run these two commands:" -ForegroundColor Yellow
Write-Host ""
Write-Host '[System.Environment]::SetEnvironmentVariable("PYSPARK_PYTHON", "' + $selectedPython + '", "User")' -ForegroundColor White -BackgroundColor DarkGreen
Write-Host '[System.Environment]::SetEnvironmentVariable("PYSPARK_DRIVER_PYTHON", "' + $selectedPython + '", "User")' -ForegroundColor White -BackgroundColor DarkGreen
Write-Host ""

