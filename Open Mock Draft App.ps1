param(
    [int]$Port = 8501,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$appUrl = "http://localhost:$Port"

function Test-AppReady {
    param(
        [int]$CheckPort
    )

    try {
        return [bool](Test-NetConnection -ComputerName "localhost" -Port $CheckPort -InformationLevel Quiet -WarningAction SilentlyContinue)
    } catch {
        return $false
    }
}

function Test-IsOurApp {
    param(
        [int]$CheckPort,
        [string]$ExpectedRoot
    )

    try {
        $conn = Get-NetTCPConnection -LocalPort $CheckPort -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $conn) { return $false }

        $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
        if (-not $proc) { return $false }

        $wmi = Get-CimInstance Win32_Process -Filter "ProcessId = $($proc.Id)" -ErrorAction SilentlyContinue
        if (-not $wmi -or -not $wmi.CommandLine) { return $false }

        $normalizedCmd = $wmi.CommandLine.Replace('\', '/').ToLower()
        $normalizedRoot = $ExpectedRoot.Replace('\', '/').ToLower()
        return $normalizedCmd.Contains($normalizedRoot)
    } catch {
        return $false
    }
}

function Get-PythonCommand {
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        return $python.Source
    }

    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        return $py.Source
    }

    throw "Python was not found on PATH."
}

$pythonCommand = Get-PythonCommand

try {
    & $pythonCommand -c "import streamlit" 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Streamlit is not installed for $pythonCommand. Run: python -m pip install --user -r requirements.txt"
    }

    $portInUse = Test-AppReady -CheckPort $Port
    $needsStart = $true

    if ($portInUse) {
        if (Test-IsOurApp -CheckPort $Port -ExpectedRoot $projectRoot) {
            Write-Host "Mock Draft app is already running on port $Port."
            $needsStart = $false
        } else {
            Write-Host "Port $Port is in use by a different app. Stopping it..."
            try {
                $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
                if ($conn) {
                    Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
                    Start-Sleep -Seconds 2
                }
            } catch {
                throw "Port $Port is in use by another process and could not be stopped. Try a different port: .\Open Mock Draft App.ps1 -Port 8502"
            }
        }
    }

    if ($needsStart) {
        Write-Host "Starting Streamlit on port $Port..."
        Push-Location $projectRoot
        try {
            & cmd.exe /c start "" /min $pythonCommand -m streamlit run app.py --server.headless true --server.port $Port | Out-Null
        } finally {
            Pop-Location
        }

        $ready = $false
        foreach ($attempt in 1..60) {
            Start-Sleep -Milliseconds 500
            if (Test-AppReady -CheckPort $Port) {
                $ready = $true
                break
            }
        }

        if (-not $ready) {
            throw "Streamlit did not become ready at $appUrl within 30 seconds."
        }
    }

    if (-not $NoBrowser) {
        Start-Process "cmd.exe" -ArgumentList "/c start $appUrl" -WindowStyle Hidden
    }
} catch {
    Write-Error $_
    exit 1
}
