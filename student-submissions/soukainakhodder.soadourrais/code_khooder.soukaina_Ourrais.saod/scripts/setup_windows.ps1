# Windows Setup Script for Spark/Hadoop
# This script downloads and configures Hadoop winutils for Spark on Windows

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Spark/Hadoop Windows Setup for Real-Time Stock Insight" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "[WARNING] Not running as Administrator. Some steps may require elevation." -ForegroundColor Yellow
    Write-Host ""
}

# Step 1: Create Hadoop directory
$hadoopHome = "C:\hadoop"
$hadoopBin = "$hadoopHome\bin"

Write-Host "[1/5] Creating Hadoop directory structure..." -ForegroundColor Green
if (-not (Test-Path $hadoopHome)) {
    New-Item -ItemType Directory -Path $hadoopHome -Force | Out-Null
    Write-Host "      Created: $hadoopHome" -ForegroundColor Gray
}
if (-not (Test-Path $hadoopBin)) {
    New-Item -ItemType Directory -Path $hadoopBin -Force | Out-Null
    Write-Host "      Created: $hadoopBin" -ForegroundColor Gray
}

# Step 2: Download winutils.exe
Write-Host "[2/5] Downloading winutils.exe..." -ForegroundColor Green
$winutilsPath = "$hadoopBin\winutils.exe"

if (Test-Path $winutilsPath) {
    Write-Host "      winutils.exe already exists. Skipping download." -ForegroundColor Gray
} else {
    try {
        $winutilsUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
        Write-Host "      Downloading from GitHub..." -ForegroundColor Gray
        Invoke-WebRequest -Uri $winutilsUrl -OutFile $winutilsPath -UseBasicParsing
        Write-Host "      Downloaded successfully!" -ForegroundColor Gray
    } catch {
        Write-Host "      [ERROR] Failed to download winutils.exe" -ForegroundColor Red
        Write-Host "      Please download manually from:" -ForegroundColor Yellow
        Write-Host "      https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.6/bin/winutils.exe" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "      Place it in: $hadoopBin" -ForegroundColor Yellow
    }
}

# Step 3: Download hadoop.dll
Write-Host "[3/5] Downloading hadoop.dll..." -ForegroundColor Green
$hadoopDllPath = "$hadoopBin\hadoop.dll"

if (Test-Path $hadoopDllPath) {
    Write-Host "      hadoop.dll already exists. Skipping download." -ForegroundColor Gray
} else {
    try {
        $hadoopDllUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"
        Write-Host "      Downloading from GitHub..." -ForegroundColor Gray
        Invoke-WebRequest -Uri $hadoopDllUrl -OutFile $hadoopDllPath -UseBasicParsing
        Write-Host "      Downloaded successfully!" -ForegroundColor Gray
    } catch {
        Write-Host "      [ERROR] Failed to download hadoop.dll" -ForegroundColor Red
        Write-Host "      Please download manually from:" -ForegroundColor Yellow
        Write-Host "      https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.6/bin/hadoop.dll" -ForegroundColor Yellow
    }
}

# Step 4: Set environment variables for current session
Write-Host "[4/5] Setting environment variables for current session..." -ForegroundColor Green
$env:HADOOP_HOME = $hadoopHome
$env:PATH = "$hadoopBin;$env:PATH"
Write-Host "      HADOOP_HOME = $hadoopHome" -ForegroundColor Gray
Write-Host "      Added to PATH: $hadoopBin" -ForegroundColor Gray

# Step 5: Create temp directories and set permissions
Write-Host "[5/5] Creating temp directories and setting permissions..." -ForegroundColor Green
$tmpHive = "C:\tmp\hive"
if (-not (Test-Path $tmpHive)) {
    New-Item -ItemType Directory -Path $tmpHive -Force | Out-Null
    Write-Host "      Created: $tmpHive" -ForegroundColor Gray
}

if (Test-Path $winutilsPath) {
    try {
        & $winutilsPath chmod 777 $tmpHive
        Write-Host "      Set permissions on $tmpHive" -ForegroundColor Gray
    } catch {
        Write-Host "      [WARNING] Could not set permissions. Run as Administrator if issues persist." -ForegroundColor Yellow
    }
}

# Summary
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Setup Complete!" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "[NEXT STEPS]" -ForegroundColor Green
Write-Host ""
Write-Host "1. For PERMANENT setup (recommended):" -ForegroundColor Yellow
Write-Host "   - Press Win+X -> System -> Advanced system settings" -ForegroundColor Gray
Write-Host "   - Environment Variables -> New System Variable" -ForegroundColor Gray
Write-Host "   - Name: HADOOP_HOME, Value: C:\hadoop" -ForegroundColor Gray
Write-Host "   - Edit PATH and add: C:\hadoop\bin" -ForegroundColor Gray
Write-Host "   - Restart your terminal" -ForegroundColor Gray
Write-Host ""
Write-Host "2. For CURRENT session only:" -ForegroundColor Yellow
Write-Host "   Environment variables are already set for this PowerShell session." -ForegroundColor Gray
Write-Host ""
Write-Host "3. Test the setup:" -ForegroundColor Yellow
Write-Host "   python test_simple.py" -ForegroundColor Gray
Write-Host ""
Write-Host "4. Run the demo:" -ForegroundColor Yellow
Write-Host "   python main.py --duration 60" -ForegroundColor Gray
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

