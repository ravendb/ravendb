
function Help () {
    Write-Host -NoNewline -ForegroundColor Cyan "-WinX64 "
    Write-Host " - build only Windows x64 artifacts"

    Write-Host -NoNewline -ForegroundColor Cyan "-WinX86 "
    Write-Host " - build only Windows x86 artifacts"

    Write-Host -NoNewline -ForegroundColor Cyan "-Ubuntu14"
    Write-Host " - build only Ubuntu 14.04 artifacts"
    
    Write-Host -NoNewline -ForegroundColor Cyan "-Ubuntu16"
    Write-Host " - build only Ubuntu 16.04 artifacts"
    
    Write-Host -NoNewline -ForegroundColor Cyan "-Rpi"
    Write-Host " - build only Raspberry Pie artifacts"

    Write-Host -NoNewline -ForegroundColor Cyan "-DontRebuildStudio"
    Write-Host " - skip building studio if it was build before"

    Write-Host -NoNewline -ForegroundColor Cyan "-Target [TargetIds]"
    Write-Host -NoNewline " - accepts comma-separated list of build target names; builds only for selected platforms "
    Write-Host "(possible build targets: $($TARGET_SPECS.TargetId -Join ", "))"

    exit 0
}