#Requires -Version 7.0
[CmdletBinding()]
param(
    [switch]$Demo,
    [switch]$Check,
    [switch]$Mock,
    [switch]$Install
)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'
$ErrorActionPreference = 'Stop'
try {
    Set-Location -LiteralPath $PSScriptRoot
    if (@($Demo, $Check, $Mock, $Install).Where({ $_.IsPresent }).Count -gt 1) {
        throw '一次只能选择一个模式。'
    }

    $venv = [System.IO.Path]::Combine($PSScriptRoot, '.venv')
    $python = [System.IO.Path]::Combine($venv, 'Scripts', 'python.exe')
    $requirements = [System.IO.Path]::Combine($PSScriptRoot, 'requirements.txt')
    $program = [System.IO.Path]::Combine($PSScriptRoot, 'qq_bot.py')
    $config = [System.IO.Path]::Combine($PSScriptRoot, 'config.yaml')
    $example = [System.IO.Path]::Combine($PSScriptRoot, 'config.example.yaml')

    if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
        $base = $null
        $prefix = @()
        foreach ($candidate in @('python', 'py')) {
            $command = Get-Command $candidate -ErrorAction SilentlyContinue
            if (-not $command) { continue }
            $candidatePrefix = if ($candidate -eq 'py') { @('-3') } else { @() }
            & $command.Source @candidatePrefix -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>$null
            if ($LASTEXITCODE -eq 0) { $base = $command.Source; $prefix = $candidatePrefix; break }
        }
        if (-not $base) { throw '未找到 Python 3.10 或更高版本，请先安装 Python 并启用 PATH。' }
        Write-Host '正在创建本地 Python 虚拟环境...'
        & $base @prefix -m venv $venv
        if ($LASTEXITCODE -ne 0) { throw '创建虚拟环境失败。' }
    }

    & $python -c 'import aiohttp, httpx, yaml, fitz, docx' 2>$null
    if ($LASTEXITCODE -ne 0 -or $Install) {
        Write-Host '正在安装 Python 依赖...'
        & $python -m pip install -r $requirements
        if ($LASTEXITCODE -ne 0) { throw '安装依赖失败，请检查网络和 Python 环境。' }
    }
    if ($Install) { exit 0 }
    if ($Demo) {
        & $python $program --demo
        exit $LASTEXITCODE
    }
    if (-not (Test-Path -LiteralPath $config -PathType Leaf)) {
        Copy-Item -LiteralPath $example -Destination $config
        Write-Host '已创建 config.yaml。请填写 API Key、模型及允许聊天的 QQ 号，然后重新运行。'
        Write-Host "配置文件：$config"
        exit 0
    }
    $programArgs = @($program, '--config', $config)
    if ($Check) { $programArgs += '--check' }
    if ($Mock) { $programArgs += '--mock-llm' }
    & $python @programArgs
    exit $LASTEXITCODE
} catch {
    Write-Error $_
    exit 1
}
