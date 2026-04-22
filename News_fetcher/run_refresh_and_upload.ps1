# Futunn Cache Refresh + Upload to VPS
# 本機抓取新聞快取，完成後上傳到 VPS

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe   = "C:\Users\aaron\AppData\Local\Microsoft\WindowsApps\python.exe"
$PythonScript = "$ScriptDir\refresh_futunn_cache.py"
$CacheFile   = "$ScriptDir\data\futunn_cache.json"
$SshKey      = "C:\Users\aaron\Documents\VPS\contabo\id_ed25519"
$VpsUser     = "root"
$VpsHost     = "161.97.167.144"
$VpsDest     = "/root/apps/4m_strategy/News_fetcher/data/futunn_cache.json"
$LogFile     = "$ScriptDir\logs\refresh.log"

# 建立 log 目錄
New-Item -ItemType Directory -Force -Path "$ScriptDir\logs" | Out-Null

function Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts  $msg" | Tee-Object -FilePath $LogFile -Append
}

Log "=== 開始 Futunn Cache Refresh ==="

# Step 1: 執行 Python 抓取腳本
Log "執行 refresh_futunn_cache.py ..."
& $PythonExe $PythonScript 2>&1 | ForEach-Object { Log $_ }

if ($LASTEXITCODE -ne 0) {
    Log "ERROR: Python 腳本執行失敗（exit code $LASTEXITCODE），跳過上傳。"
    exit 1
}

if (-not (Test-Path $CacheFile)) {
    Log "ERROR: 找不到 $CacheFile，跳過上傳。"
    exit 1
}

# Step 2: SCP 上傳到 VPS
Log "上傳 futunn_cache.json 到 VPS ..."
& scp -i $SshKey -o StrictHostKeyChecking=no $CacheFile "${VpsUser}@${VpsHost}:${VpsDest}" 2>&1 | ForEach-Object { Log $_ }

if ($LASTEXITCODE -eq 0) {
    Log "上傳成功。"
} else {
    Log "ERROR: SCP 上傳失敗（exit code $LASTEXITCODE）。"
    exit 1
}

Log "=== 完成 ==="
