# Сценарий для записи экрана: показывает исходное состояние, гоняет бенчмарк,
# печатает итоговый файл результатов. Запускается в отдельном окне, которое
# ffmpeg снимает по заголовку окна.

$Host.UI.RawUI.WindowTitle = "RevScope Benchmark Run"
try {
  $Host.UI.RawUI.BufferSize = New-Object Management.Automation.Host.Size(150, 3000)
  $Host.UI.RawUI.WindowSize  = New-Object Management.Automation.Host.Size(150, 42)
} catch {}

Set-Location "C:\Users\Ilay\test\revscope"
Clear-Host

function Step($text) {
  Write-Host ""
  Write-Host "PS> $text" -ForegroundColor Cyan
}

# Ждём, пока рекордер реально начнёт писать (он создаёт файл-флаг), иначе
# первые секунды прогона не попадут в кадр
$flag = "C:\Users\Ilay\test\revscope\bench\out\.recording"
$waited = 0
while (-not (Test-Path $flag) -and $waited -lt 90) {
  Start-Sleep -Milliseconds 500
  $waited += 0.5
}
Start-Sleep -Seconds 2

Write-Host "revscope benchmark - single take, no edits" -ForegroundColor White
Write-Host ("=" * 60) -ForegroundColor DarkGray
Write-Host ("started: " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss K")) -ForegroundColor DarkGray

# 1. Код на месте и не правился
Step "git log -1 --oneline; git status --short"
git log -1 --oneline
git status --short
Start-Sleep -Seconds 4

# 2. База — обычный контейнер Postgres, ничего припрятанного
Step "docker ps --filter name=revscope"
docker ps --filter name=revscope --format "{{.Names}}  {{.Image}}  {{.Status}}  {{.Ports}}"
Start-Sleep -Seconds 4

# 3. Датасет: детерминированный генератор
Step "head -25 gen.py  (seeded generator)"
Get-Content gen.py -TotalCount 25
Start-Sleep -Seconds 6

# 4. Сам прогон
Step "python -u -m bench.run_all"
python -u -m bench.run_all

# 5. Файл результатов, который пишет сам бенчмарк
Step "type bench\out\results.md"
Get-Content "bench\out\results.md" -TotalCount 40

Write-Host ""
Write-Host ("finished: " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss K")) -ForegroundColor DarkGray
Start-Sleep -Seconds 8
