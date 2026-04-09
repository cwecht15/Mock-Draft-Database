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

    if (-not (Test-AppReady -CheckPort $Port)) {
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
        Start-Process $appUrl | Out-Null
    }
} catch {
    Write-Error $_
    exit 1
}
