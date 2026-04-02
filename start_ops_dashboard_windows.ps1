$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $RepoRoot

$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    throw "Missing virtual environment at .venv\Scripts\python.exe"
}

$EnvPath = Join-Path $RepoRoot ".env.local"
if (Test-Path $EnvPath) {
    foreach ($RawLine in Get-Content $EnvPath) {
        $Line = $RawLine.Trim()
        if (-not $Line -or $Line.StartsWith("#")) {
            continue
        }
        if ($Line.StartsWith("export ")) {
            $Line = $Line.Substring(7).TrimStart()
        }
        $Parts = $Line.Split("=", 2)
        if ($Parts.Length -ne 2) {
            continue
        }
        $Name = $Parts[0].Trim()
        if ($Name -notin @("GOOGLE_API_KEY", "GOOGLE_CSE_CX")) {
            continue
        }
        $Value = $Parts[1].Trim()
        if ($Value.Length -ge 2) {
            if (($Value.StartsWith('"') -and $Value.EndsWith('"')) -or ($Value.StartsWith("'") -and $Value.EndsWith("'"))) {
                $Value = $Value.Substring(1, $Value.Length - 2)
            }
        }
        Set-Item -Path "Env:$Name" -Value $Value
    }
}

& $PythonExe .\run_ops_dashboard.py
