param([switch]$Stop)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
function Test-DockerReady {
    # Windows PowerShell turns redirected native stderr into error records.
    $ErrorActionPreference = 'Continue'
    & $dockerPath info --format '{{.OSType}}' 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}
try {
    $docker = Get-Command docker -ErrorAction SilentlyContinue
    $desktop = Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'
    $dockerExe = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'
    if (-not $docker -and (Test-Path -LiteralPath $dockerExe)) { $docker = Get-Command $dockerExe }
    if (-not $docker) {
        if ($Stop) { throw 'Docker is not installed.' }
        if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
            throw 'Install App Installer (winget), then rerun this script.'
        }
        & winget install --exact --id Docker.DockerDesktop --accept-source-agreements --accept-package-agreements
        if ($LASTEXITCODE -ne 0) { throw 'Docker installation failed or requires a reboot. Complete setup and rerun.' }
        if (-not (Test-Path -LiteralPath $dockerExe)) { throw 'Finish Docker Desktop setup, reboot if requested, then rerun.' }
        $docker = Get-Command $dockerExe
    }
    $dockerPath = $docker.Source
    $ready = Test-DockerReady
    if (-not $ready -and -not $Stop) {
        if (Test-Path -LiteralPath $desktop) { Start-Process -FilePath $desktop -WindowStyle Hidden }
        Write-Host 'Waiting up to 3 minutes for Docker. Complete any first-run/WSL prompts in Docker Desktop.'
        $deadline = (Get-Date).AddMinutes(3)
        do {
            Start-Sleep -Seconds 3
            $ready = Test-DockerReady
        } until ($ready -or (Get-Date) -ge $deadline)
        if (-not $ready) { throw 'Docker is unavailable. Enable WSL 2/virtualization, finish Docker setup, reboot if requested, and rerun.' }
    }
    $osType = & $dockerPath info --format '{{.OSType}}'
    if ($LASTEXITCODE -ne 0 -or $osType -ne 'linux') { throw 'Docker must be running in Linux containers mode.' }
    & $dockerPath compose version
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose v2 is required. Update Docker Desktop.' }
    $envFile = Join-Path $PSScriptRoot '.monitoring.env'
    if (-not (Test-Path -LiteralPath $envFile)) {
        $bytes = New-Object byte[] 24
        $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
        $password = [Convert]::ToBase64String($bytes)
        Set-Content -LiteralPath $envFile -Value "GRAFANA_ADMIN_PASSWORD=$password" -Encoding ASCII
    }
    $composeArgs = @('compose', '--env-file', $envFile, '-f', 'docker-compose.monitoring.yml')
    if ($Stop) {
        & $dockerPath @composeArgs down
    } else {
        & $dockerPath @composeArgs up -d --wait --wait-timeout 120
    }
    if ($LASTEXITCODE -ne 0) { throw 'Monitoring stack command failed. Check Docker output above.' }
    if (-not $Stop) {
        Write-Host 'Grafana: http://localhost:3003 (admin; password in .monitoring.env)'
        Write-Host 'Prometheus: http://localhost:9091'
        Write-Host 'Run the bot separately with run.bat after installing requirements.txt.'
    }
} catch {
    Write-Error $_ -ErrorAction Continue
    exit 1
}
