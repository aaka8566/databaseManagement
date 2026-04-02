$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$Dist = Join-Path $Root "dist"
$AppName = "Sharingan"
$ExeName = "sharingan.exe"

New-Item -ItemType Directory -Force -Path $Dist | Out-Null

if (Get-Command cargo-bundle -ErrorAction SilentlyContinue) {
    cargo bundle --release --format msi
    Write-Host "MSI created under target\\release\\bundle\\msi\\"
    exit 0
}

cargo build --release

$BundleDir = Join-Path $Dist "windows"
$ZipPath = Join-Path $Dist "$AppName-windows.zip"

if (Test-Path $BundleDir) {
    Remove-Item -Recurse -Force $BundleDir
}
if (Test-Path $ZipPath) {
    Remove-Item -Force $ZipPath
}

New-Item -ItemType Directory -Force -Path $BundleDir | Out-Null
Copy-Item (Join-Path $Root "target\\release\\$ExeName") (Join-Path $BundleDir "$AppName.exe")
Compress-Archive -Path (Join-Path $BundleDir "*") -DestinationPath $ZipPath

Write-Host "ZIP created at $ZipPath"
Write-Host "For a proper MSI installer, install cargo-bundle or cargo-wix on Windows and build there."
