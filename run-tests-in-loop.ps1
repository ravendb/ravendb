cd build
for($i = 0; $i -le 250; $i++) 
{
    $sp = [System.Diagnostics.Stopwatch]::StartNew()
    Write-Host "Starting test #$i" -foregroundcolor Cyan
    ..\Tools\XUnit\xunit.console.clr4.exe "Raven.Munin.Tests.dll" 
    if ($lastExitCode -ne 0)
      { throw "err" }
    ..\Tools\XUnit\xunit.console.clr4.exe "Raven.Tests.dll" 
    if ($lastExitCode -ne 0)
      { throw "err" }
    ..\Tools\XUnit\xunit.console.clr4.exe "Raven.Scenarios.dll" 
    if ($lastExitCode -ne 0)
      { throw "err" }
    ..\Tools\XUnit\xunit.console.clr4.exe "Raven.Client.Tests.dll" 
    if ($lastExitCode -ne 0)
      { throw "err" }
    ..\Tools\XUnit\xunit.console.clr4.exe "Raven.Client.VisualBasic.Tests.dll" 
    if ($lastExitCode -ne 0)
      { throw "err" }
    ..\Tools\XUnit\xunit.console.clr4.exe "Raven.Bundles.Tests.dll" 
    if ($lastExitCode -ne 0)
      { throw "err" }
     Write-Host "Completed in " $sp.Elapsed -foregroundcolor Cyan
   
}