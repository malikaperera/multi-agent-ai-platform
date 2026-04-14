param(
    [int]$DurationMinutes = 180,
    [int]$IntervalSeconds = 60
)

$ErrorActionPreference = "Continue"

$root = Split-Path -Parent $PSScriptRoot
$logsDir = Join-Path $root "logs\\monitoring"
New-Item -ItemType Directory -Force -Path $logsDir | Out-Null

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$jsonlPath = Join-Path $logsDir "runtime_${stamp}.jsonl"
$textPath = Join-Path $logsDir "runtime_${stamp}.log"

function Safe-InvokeJson {
    param([string]$Uri, [int]$TimeoutSec = 20)
    try {
        return Invoke-RestMethod -Uri $Uri -TimeoutSec $TimeoutSec
    } catch {
        return @{ error = $_.Exception.Message; uri = $Uri }
    }
}

function Safe-DockerLogs {
    param([string]$Service, [string]$Since = "70s")
    try {
        return docker compose logs --since $Since $Service 2>&1
    } catch {
        return @("ERROR reading logs for ${Service}: $($_.Exception.Message)")
    }
}

$end = (Get-Date).AddMinutes($DurationMinutes)

while ((Get-Date) -lt $end) {
    $now = Get-Date
    $health = Safe-InvokeJson "http://localhost:8000/health" 10
    $metrics = Safe-InvokeJson "http://localhost:8000/system/metrics" 20
    $recommendations = Safe-InvokeJson "http://localhost:8000/operator/recommendations" 20
    $forge = Safe-InvokeJson "http://localhost:8000/agents/forge" 20
    $merlin = Safe-InvokeJson "http://localhost:8000/agents/merlin" 20
    $venture = Safe-InvokeJson "http://localhost:8000/agents/venture" 20
    $sentinel = Safe-InvokeJson "http://localhost:8000/agents/sentinel" 20
    $zuko = Safe-InvokeJson "http://localhost:8000/agents/zuko" 20
    $tasks = Safe-InvokeJson "http://localhost:8000/tasks?limit=250" 30

    $taskList = @()
    if ($tasks -is [System.Array]) {
        $taskList = $tasks
    } elseif ($tasks.value) {
        $taskList = @($tasks.value)
    }

    $inProgress = @($taskList | Where-Object { $_.status -eq "in_progress" })
    $pending = @($taskList | Where-Object { $_.status -eq "pending" })
    $forgeOpen = @($taskList | Where-Object { $_.to_agent -eq "forge" -and $_.status -in @("pending","plan_ready","approved","plan_approved","in_progress","awaiting_validation") })
    $stuck = @($recommendations.items | Where-Object { $_.id -like "stuck_task_*" })

    $snapshot = [ordered]@{
        timestamp = $now.ToString("o")
        health = $health
        in_progress_count = $inProgress.Count
        pending_count = $pending.Count
        forge_open_count = $forgeOpen.Count
        stuck_count = $stuck.Count
        merlin = @{
            status = $merlin.status
            stage = $merlin.stage
            current_model = $merlin.current_model
            current_task_id = $merlin.current_task_id
        }
        venture = @{
            status = $venture.status
            stage = $venture.stage
            current_model = $venture.current_model
            current_task_id = $venture.current_task_id
        }
        forge = @{
            status = $forge.status
            stage = $forge.stage
            current_model = $forge.current_model
            current_task_id = $forge.current_task_id
        }
        sentinel = @{
            status = $sentinel.status
            stage = $sentinel.stage
            current_model = $sentinel.current_model
            current_task_id = $sentinel.current_task_id
        }
        zuko = @{
            status = $zuko.status
            stage = $zuko.stage
            current_model = $zuko.current_model
            current_task_id = $zuko.current_task_id
        }
        metrics = $metrics
        stuck = $stuck
    }

    $snapshotJson = $snapshot | ConvertTo-Json -Depth 10 -Compress
    Add-Content -Path $jsonlPath -Value $snapshotJson

    $lines = @()
    $lines += "[$($now.ToString("o"))] health=$($health.status) in_progress=$($inProgress.Count) pending=$($pending.Count) forge_open=$($forgeOpen.Count) stuck=$($stuck.Count)"
    $lines += "  merlin=$($merlin.status)/$($merlin.current_model) venture=$($venture.status)/$($venture.current_model) forge=$($forge.status)/$($forge.current_model) sentinel=$($sentinel.status)/$($sentinel.current_model) zuko=$($zuko.status)/$($zuko.current_model)"
    if ($metrics.gpu.ollama_residency) {
        $lines += "  gpu_residency=$($metrics.gpu.ollama_residency.residency_percent)% models=$((@($metrics.ollama.loaded_models) | ForEach-Object { $_.name }) -join ', ')"
    }
    if ($stuck.Count -gt 0) {
        foreach ($item in $stuck | Select-Object -First 5) {
            $lines += "  stuck: $($item.title) :: $($item.summary)"
        }
    }
    $lines += "  roderick logs:"
    $lines += @(Safe-DockerLogs "roderick" "70s" | Select-Object -Last 20 | ForEach-Object { "    $_" })
    $lines += "  api logs:"
    $lines += @(Safe-DockerLogs "api" "70s" | Select-Object -Last 10 | ForEach-Object { "    $_" })
    $lines += "  zuko logs:"
    $lines += @(Safe-DockerLogs "zuko" "70s" | Select-Object -Last 10 | ForEach-Object { "    $_" })
    Add-Content -Path $textPath -Value ($lines -join [Environment]::NewLine)
    Add-Content -Path $textPath -Value ""

    Start-Sleep -Seconds $IntervalSeconds
}

Write-Output "Monitoring finished. JSONL: $jsonlPath`nText log: $textPath"
