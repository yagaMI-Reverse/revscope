# Снимает ОДНО окно по заголовку через PrintWindow (PW_RENDERFULLCONTENT).
# Работает, даже если окно перекрыто другими или уехало на второй рабочий стол,
# поэтому машина остаётся свободной, пока идёт запись.
#
#   powershell -File grab_window.ps1 -Title "..." -OutDir frames -Seconds 470 -Fps 8

param(
  [string]$Title   = "RevScope Benchmark Run",
  [string]$OutDir  = "C:\Users\Ilay\test\revscope\bench\out\frames",
  [int]   $Seconds = 470,
  [int]   $Fps     = 8
)

Add-Type -AssemblyName System.Drawing
Add-Type @"
using System;
using System.Runtime.InteropServices;
[StructLayout(LayoutKind.Sequential)]
public struct RECT { public int L; public int T; public int R; public int B; }
public class Win {
  [DllImport("user32.dll")] public static extern bool PrintWindow(IntPtr h, IntPtr hdc, uint flags);
  [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr h, out RECT r);
}
"@

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
Get-ChildItem $OutDir -Filter *.png -ErrorAction SilentlyContinue | Remove-Item -Force

# ждём появления окна
$proc = $null; $t = 0
while (-not $proc -and $t -lt 60) {
  $proc = Get-Process powershell -ErrorAction SilentlyContinue |
          Where-Object { $_.MainWindowTitle -eq $Title } | Select-Object -First 1
  if (-not $proc) { Start-Sleep -Milliseconds 500; $t += 0.5 }
}
if (-not $proc) { Write-Output "ОКНО НЕ НАЙДЕНО"; exit 1 }

$h = $proc.MainWindowHandle
$rect = New-Object RECT
[void][Win]::GetWindowRect($h, [ref]$rect)
$w = $rect.R - $rect.L; $ht = $rect.B - $rect.T
if ($w -le 0 -or $ht -le 0) { Write-Output "НЕВЕРНЫЙ РАЗМЕР ОКНА"; exit 1 }
Write-Output "окно $w x $ht, снимаю $Seconds с при $Fps fps"

$delay = [int](1000 / $Fps)
$deadline = (Get-Date).AddSeconds($Seconds)
$i = 0
$bmp = New-Object System.Drawing.Bitmap($w, $ht)
$gfx = [System.Drawing.Graphics]::FromImage($bmp)

while ((Get-Date) -lt $deadline) {
  $started = Get-Date
  $hdc = $gfx.GetHdc()
  # флаг 2 = PW_RENDERFULLCONTENT: берёт содержимое даже у перекрытого окна
  [void][Win]::PrintWindow($h, $hdc, 2)
  $gfx.ReleaseHdc($hdc)
  $bmp.Save((Join-Path $OutDir ("f{0:D5}.png" -f $i)), [System.Drawing.Imaging.ImageFormat]::Png)
  $i++
  # процесс закончился — досняли ещё пару секунд и выходим
  if ($proc.HasExited) { break }
  $spent = ((Get-Date) - $started).TotalMilliseconds
  $sleep = $delay - $spent
  if ($sleep -gt 0) { Start-Sleep -Milliseconds $sleep }
}
$gfx.Dispose(); $bmp.Dispose()
Write-Output "кадров: $i"
