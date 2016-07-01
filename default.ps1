Include ".\build_utils.ps1"

if($env:BUILD_NUMBER -ne $null) {
    $env:buildlabel = $env:BUILD_NUMBER
}
else {
    $env:buildlabel = "13"
}

properties {
    $base_dir  = resolve-path .
    $lib_dir = "$base_dir\SharedLibs"
    $packages_dir = "$base_dir\packages"
    $build_dir = "$base_dir\artifacts"
    $sln_file_name = "zzz_RavenDB_Release.sln"
    $sln_file = "$base_dir\$sln_file_name"
    $version = "3.5"
    $tools_dir = "$base_dir\Tools"
    $release_dir = "$base_dir\Release"
    $liveTest_dir = "C:\Sites\RavenDB 3\Web"
    $uploader = "..\Uploader\S3Uploader.exe"
    $global:configuration = "Release"
    $msbuild = "C:\Program Files (x86)\MSBuild\14.0\Bin\MSBuild.exe"
    $nowarn = "1591 1573 1591 HeapAnalyzerBoxingRule HeapAnalyzerClosureCaptureRule HeapAnalyzerImplicitParamsRule HeapAnalyzerStringConcatRule HeapAnalyzerValueTypeNonOverridenCallRule HeapAnalyzerClosureSourceRule HeapAnalyzerLambdaInGenericMethodRule HeapAnalyzerEnumeratorAllocationRule HeapAnalyzerMethodGroupAllocationRule HeapAnalyzerLambdaInGenericMethodRule"

    $dnxVersion = "1.0.0-rc1-update1"
    $dnxArchitecture = "x64"
    $dnxToolsDir = "$base_dir\Tools\DNX"
    $dnvm = "$dnxToolsDir\dnvm.cmd"
    $dnxRuntimeDir = "$env:USERPROFILE\.dnx\runtimes\dnx-coreclr-win-$dnxArchitecture.$dnxVersion\bin";
    $dnu = "$dnxRuntimeDir\dnu.cmd"
    $dnx = "$dnxRuntimeDir\dnx.exe"
    
    $nuget = "$base_dir\.nuget\NuGet.exe"
    
    $global:is_pull_request = $FALSE
}

task default -depends Test, DoReleasePart1

task Verify40 {
    if( (ls "$env:windir\Microsoft.NET\Framework\v4.0*") -eq $null ) {
        throw "Building Raven requires .NET 4.0, which doesn't appear to be installed on this machine"
    }
}

task Clean {
    Remove-Item -force -recurse $build_dir -ErrorAction SilentlyContinue
    Remove-Item -force -recurse $release_dir -ErrorAction SilentlyContinue
}

task NuGet {
    &"$nuget" restore "$sln_file"
}

task Init -depends Verify40, Clean, NuGet {

    $commit = Get-Git-Commit
    if( $env:buildlabel -ne 13){
    (Get-Content "$base_dir\CommonAssemblyInfo.cs") | 
            Foreach-Object { $_ -replace "\.13\.", ".$($env:buildlabel)." } |
            Foreach-Object { $_ -replace "{build-label}", "$($env:buildlabel)" } |
        Foreach-Object { $_ -replace "{commit}", $commit } |
        Foreach-Object { $_ -replace "{stable}", $global:uploadMode } |
        Set-Content "$base_dir\CommonAssemblyInfo.cs" -Encoding UTF8
    }
    New-Item $release_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
    New-Item $build_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
}

task Compile -depends Init, CompileHtml5 {
    
    $commit = Get-Git-Commit-Full

    Write-Host "Compiling with '$global:configuration' configuration" -ForegroundColor Yellow
    
    &"$msbuild" "$sln_file" /p:Configuration=$global:configuration /p:nowarn="$nowarn" /p:VisualStudioVersion=14.0 /maxcpucount /verbosity:minimal
    
    Write-Host "msbuild exit code: $LastExitCode"

    if( $LastExitCode -ne 0){
        throw "Failed to build"
    }
    
    if ($commit -ne "0000000000000000000000000000000000000000") {
        exec { &"$tools_dir\GitLink.Custom.exe" "$base_dir" /u https://github.com/ayende/ravendb /c $global:configuration /b master /s "$commit" /f "$sln_file_name" }
    }
    
    exec { &"$tools_dir\Assembly.Validator.exe" "$lib_dir" "$lib_dir\Sources" }
}

task CompileHtml5 {
    
    "{ ""BuildVersion"": $env:buildlabel }" | Out-File "Raven.Studio.Html5\version.json" -Encoding UTF8
    
    Write-Host "Compiling HTML5" -ForegroundColor Yellow

    &"$msbuild" "Raven.Studio.Html5\Raven.Studio.Html5.csproj" /p:Configuration=$global:configuration /p:nowarn="$nowarn" /p:VisualStudioVersion=14.0 /maxcpucount /verbosity:minimal
    
    Remove-Item $build_dir\Html5 -Force -Recurse -ErrorAction SilentlyContinue | Out-Null
    Remove-Item $build_dir\Raven.Studio.Html5.zip -Force -ErrorAction SilentlyContinue | Out-Null
    
    Set-Location $base_dir\Raven.Studio.Html5
    exec { & $tools_dir\Pvc\pvc.exe optimized-build }
    
    Copy-Item $base_dir\Raven.Studio.Html5\optimized-build $build_dir\Html5 -Recurse
    Copy-Item $base_dir\Raven.Studio.Html5\favicon.ico $build_dir\Html5
    Copy-Item $base_dir\Raven.Studio.Html5\fonts $build_dir\Html5 -Recurse -Force
    Copy-Item $base_dir\Raven.Studio.Html5\Content $build_dir\Html5 -Recurse -Force
    
    Set-Location $build_dir\Html5
    exec { & $tools_dir\zip.exe -9 -A -r $build_dir\Raven.Studio.Html5.zip *.* }
    Set-Location $base_dir
}

task CompileDnx -depends Compile {

    &"$dnvm" install $dnxVersion -r coreclr -arch $dnxArchitecture

    &"$dnvm" use -r coreclr -arch $dnxArchitecture $dnxVersion

    &"$dnu" restore --quiet .
    
    &"$dnu" build --quiet --configuration "$global:configuration" --out "$build_dir\DNX\Raven.Client\" Raven.Sparrow\Sparrow Raven.Abstractions Raven.Client.Lightweight
    
    &"$dnu" build --quiet --configuration "$global:configuration" --out "$build_dir\DNX\Raven.Client.Authorization" Bundles\Raven.Client.Authorization
    
    &"$dnu" build --quiet --configuration "$global:configuration" --out "$build_dir\DNX\Raven.Client.UniqueConstraints" Bundles\Raven.Client.UniqueConstraints
}

task TestDnx -depends CompileDnx {
    Clear-Host

    Push-Location "$base_dir\Raven.Tests.Core"
    
    Start-Process -FilePath "$dnx" -ArgumentList "--configuration $global:configuration test" -NoNewWindow -Wait

    Pop-Location
}

task FullStorageTest {
    $global:full_storage_test = $true
}

task Test -depends TestDnx {

    $test_prjs = New-Object System.Collections.ArrayList

    [void]$test_prjs.Add("$base_dir\Raven.Sparrow\Sparrow.Tests\bin\$global:configuration\Sparrow.Tests.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests.Core\bin\$global:configuration\Raven.Tests.Core.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests.Web\bin\Raven.Tests.Web.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests\bin\$global:configuration\Raven.Tests.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests.Bundles\bin\$global:configuration\Raven.Tests.Bundles.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests.Issues\bin\$global:configuration\Raven.Tests.Issues.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests.FileSystem\bin\$global:configuration\Raven.Tests.FileSystem.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests.MailingList\bin\$global:configuration\Raven.Tests.MailingList.dll");
    
    if ($global:is_pull_request -eq $FALSE) {
        [void]$test_prjs.Add("$base_dir\Raven.SlowTests\bin\$global:configuration\Raven.SlowTests.dll");
    }
    
    [void]$test_prjs.Add("$base_dir\Raven.Voron\Voron.Tests\bin\$global:configuration\Voron.Tests.dll");
    [void]$test_prjs.Add("$base_dir\Raven.DtcTests\bin\$global:configuration\Raven.DtcTests.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests.Counters\bin\$global:configuration\Raven.Tests.Counters.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests.TimeSeries\bin\$global:configuration\Raven.Tests.TimeSeries.dll");
    [void]$test_prjs.Add("$base_dir\Rachis\Rachis.Tests\bin\$global:configuration\Rachis.Tests.dll");
    [void]$test_prjs.Add("$base_dir\Raven.Tests.Raft\bin\$global:configuration\Raven.Tests.Raft.dll");

    Write-Host $test_prjs
    
    $xUnit = "$lib_dir\xunit\xunit.console.clr4.exe"
    Write-Host "xUnit location: $xUnit"
    
    $hasErrors = $false
    $test_prjs | ForEach-Object { 
        if($global:full_storage_test) {
            $env:raventest_storage_engine = 'esent';
            Write-Host "Testing $_ (esent)"
        }
        else {
            $env:raventest_storage_engine = $null;
            Write-Host "Testing $_ (default)"
        }
        &"$xUnit" "$_"
        if ($lastexitcode -ne 0) {
            $hasErrors = $true
            Write-Host "-------- ---------- -------- ---"
            Write-Host "Failure for $_ - $lastexitcode"
            Write-Host "-------- ---------- -------- ---"
        }
    }
    if ($hasErrors) {
        throw ("Test failure!")
    }
}

task StressTest -depends Compile {
    
    $xUnit = Get-PackagePath xunit.runners
    $xUnit = "$xUnit\tools\xunit.console.clr4.exe"
    
    @("Raven.StressTests.dll") | ForEach-Object { 
        Write-Host "Testing $_"
        
        if($global:full_storage_test) {
            $env:raventest_storage_engine = 'esent';
            Write-Host "Testing $_ (esent)"
            &"$xUnit" "$_"
        }
        else {
            $env:raventest_storage_engine = $null;
            Write-Host "Testing $_ (default)"
            &"$xUnit" "$_"
        }
    }
}

task MeasurePerformance -depends Compile {
    $RavenDbStableLocation = "F:\RavenDB"
    $DataLocation = "F:\Data"
    $LogsLocation = "F:\PerformanceLogs"
    $stableBuildToTests = @(616, 573, 531, 499, 482, 457, 371)
    $stableBuildToTests | ForEach-Object { 
        $RavenServer = $RavenDbStableLocation + "\RavenDB-Build-$_\Server"
        Write-Host "Measure performance against RavenDB Build #$_, Path: $RavenServer"
        exec { &"$base_dir\Raven.Performance\bin\$global:configuration\Raven.Performance.exe" "--database-location=$RavenDbStableLocation --build-number=$_ --data-location=$DataLocation --logs-location=$LogsLocation" }
    }
}

task Unstable {
    $global:uploadCategory = "RavenDB-Unstable"
    $global:uploadMode = "Unstable"
    $global:configuration = "Release"
}

task Stable {
    $global:uploadCategory = "RavenDB"
    $global:uploadMode = "Stable"
    $global:configuration = "Release"
}

task RunTests -depends Test

task PullRequest {
    $global:is_pull_request = $TRUE
}

task RunAllTests -depends FullStorageTest,RunTests,StressTest

task CreateOutpuDirectories -depends CleanOutputDirectory {
    New-Item $build_dir\Output -Type directory -ErrorAction SilentlyContinue | Out-Null
    New-Item $build_dir\Output\Server -Type directory | Out-Null
    New-Item $build_dir\Output\Web -Type directory | Out-Null
    New-Item $build_dir\Output\Web\bin -Type directory | Out-Null
    New-Item $build_dir\Output\Client -Type directory | Out-Null
    New-Item $build_dir\Output\Bundles -Type directory | Out-Null
    New-Item $build_dir\Output\Smuggler -Type directory | Out-Null
    New-Item $build_dir\Output\Backup -Type directory | Out-Null
    New-Item $build_dir\Output\Migration -Type directory | Out-Null
    New-Item $build_dir\Output\Diag\Traffic -Type directory | Out-Null
    New-Item $build_dir\Output\Diag\ApiToken -Type directory | Out-Null
    New-Item $build_dir\Output\Diag\StorageExporter -Type directory | Out-Null
    New-Item $build_dir\Output\Monitor -Type directory | Out-Null
}

task CleanOutputDirectory { 
    Remove-Item $build_dir\Output -Recurse -Force -ErrorAction SilentlyContinue
}

task CopyMonitor {
    Copy-Item $base_dir\Raven.Monitor\bin\$global:configuration\amd64 $build_dir\Output\Monitor\amd64 -recurse
    Copy-Item $base_dir\Raven.Monitor\bin\$global:configuration\x86 $build_dir\Output\Monitor\x86 -recurse
    Copy-Item $base_dir\Raven.Monitor\bin\$global:configuration\Raven.Monitor.exe $build_dir\Output\Monitor
    Copy-Item $base_dir\Raven.Monitor\bin\$global:configuration\Microsoft.Diagnostics.Tracing.TraceEvent.dll  $build_dir\Output\Monitor
}

task CopySmuggler {
    Copy-Item $base_dir\Raven.Smuggler\bin\$global:configuration\Raven.Abstractions.??? $build_dir\Output\Smuggler
    Copy-Item $base_dir\Raven.Smuggler\bin\$global:configuration\Raven.Database.??? $build_dir\Output\Smuggler
    Copy-Item $base_dir\Raven.Smuggler\bin\$global:configuration\Raven.Smuggler.??? $build_dir\Output\Smuggler
}

task CopyBackup {
    Copy-Item $base_dir\Raven.Backup\bin\$global:configuration\Raven.Abstractions.??? $build_dir\Output\Backup
    Copy-Item $base_dir\Raven.Backup\bin\$global:configuration\Raven.Backup.??? $build_dir\Output\Backup
}

task CopyMigration {
    Copy-Item $base_dir\Raven.Migration\bin\$global:configuration\Raven.Abstractions.??? $build_dir\Output\Migration
    Copy-Item $base_dir\Raven.Migration\bin\$global:configuration\Raven.Migration.??? $build_dir\Output\Migration
}

task CopyRavenTraffic {
    Copy-Item $base_dir\Tools\Raven.Traffic\bin\$global:configuration\Raven.Abstractions.??? $build_dir\Output\Diag\Traffic
    Copy-Item $base_dir\Tools\Raven.Traffic\bin\$global:configuration\Raven.Traffic.??? $build_dir\Output\Diag\Traffic
}

task CopyRavenApiToken {
    Copy-Item $base_dir\Tools\Raven.ApiToken\bin\$global:configuration\Raven.Abstractions.??? $build_dir\Output\Diag\ApiToken
    Copy-Item $base_dir\Tools\Raven.ApiToken\bin\$global:configuration\Raven.ApiToken.??? $build_dir\Output\Diag\ApiToken
    Copy-Item $base_dir\Tools\Raven.ApiToken\token_test.ps1 $build_dir\Output\Diag\ApiToken
}

task CopyStorageExporter {
    Copy-Item $base_dir\Raven.StorageExporter\bin\$global:configuration\Raven.Abstractions.??? $build_dir\Output\Diag\StorageExporter
    Copy-Item $base_dir\Raven.StorageExporter\bin\$global:configuration\Raven.Database.??? $build_dir\Output\Diag\StorageExporter
    Copy-Item $base_dir\Raven.StorageExporter\bin\$global:configuration\Raven.StorageExporter.??? $build_dir\Output\Diag\StorageExporter
}

task CopyClient {
    $client_dlls = @( "$base_dir\Raven.Client.Lightweight\bin\$global:configuration\Raven.Abstractions.???", "$base_dir\Raven.Client.Lightweight\bin\$global:configuration\Raven.Client.Lightweight.???")
    $client_dlls | ForEach-Object { Copy-Item "$_" $build_dir\Output\Client }
}

task CopyWeb {
    @( "$base_dir\Raven.Database\bin\$global:configuration\Raven.Database.???",
        "$base_dir\Raven.Abstractions\bin\$global:configuration\Raven.Abstractions.???", 
        "$base_dir\Raven.Web\bin\Microsoft.Owin.???",
        "$base_dir\Raven.Web\bin\Owin.???",
        "$base_dir\Raven.Web\bin\Microsoft.Owin.Host.SystemWeb.???",
        "$build_dir\Raven.Studio.Html5.zip",
        "$base_dir\Raven.Web\bin\Raven.Web.???"  ) | ForEach-Object { Copy-Item "$_" $build_dir\Output\Web\bin }
    @("$base_dir\DefaultConfigs\web.config", 
        "$base_dir\DefaultConfigs\NLog.Ignored.config" ) | ForEach-Object { Copy-Item "$_" $build_dir\Output\Web }
}

task CopyBundles {
    Copy-Item $base_dir\Bundles\Raven.Bundles.Authorization\bin\$global:configuration\Raven.Bundles.Authorization.??? $build_dir\Output\Bundles
    Copy-Item $base_dir\Bundles\Raven.Bundles.CascadeDelete\bin\$global:configuration\Raven.Bundles.CascadeDelete.??? $build_dir\Output\Bundles
    Copy-Item $base_dir\Bundles\Raven.Bundles.Encryption.IndexFileCodec\bin\$global:configuration\Raven.Bundles.Encryption.IndexFileCodec.??? $build_dir\Output\Bundles
    Copy-Item $base_dir\Bundles\Raven.Bundles.UniqueConstraints\bin\$global:configuration\Raven.Bundles.UniqueConstraints.??? $build_dir\Output\Bundles
    Copy-Item $base_dir\Bundles\Raven.Client.Authorization\bin\$global:configuration\Raven.Client.Authorization.??? $build_dir\Output\Bundles
    Copy-Item $base_dir\Bundles\Raven.Client.UniqueConstraints\bin\$global:configuration\Raven.Client.UniqueConstraints.??? $build_dir\Output\Bundles
}

task CopyServer -depends CreateOutpuDirectories {
    $server_files = @( "$base_dir\Raven.Database\bin\$global:configuration\Raven.Database.???", 
        "$base_dir\Raven.Abstractions\bin\$global:configuration\Raven.Abstractions.???", 
        "$build_dir\Raven.Studio.Html5.zip",
        "$base_dir\Raven.Server\bin\$global:configuration\Raven.Server.???",
        "$base_dir\DefaultConfigs\NLog.Ignored.config")
    $server_files | ForEach-Object { Copy-Item "$_" $build_dir\Output\Server }
    Copy-Item $base_dir\DefaultConfigs\RavenDb.exe.config $build_dir\Output\Server\Raven.Server.exe.config
}

function SignFile($filePath){

    if($env:buildlabel -eq 13)
    {
        return
    }
    
    $signTool = "C:\Program Files (x86)\Windows Kits\8.1\bin\x64\signtool.exe"
    if (!(Test-Path $signTool)) 
    {
        
    $signTool = "C:\Program Files (x86)\Windows Kits\8.0\bin\x86\signtool.exe"
    
    if (!(Test-Path $signTool)) 
    {
        $signTool = "C:\Program Files\Microsoft SDKs\Windows\v7.1\Bin\signtool.exe"
    
        if (!(Test-Path $signTool)) 
        {
            throw "Could not find SignTool.exe under the specified path $signTool"
        }
    }
    }
  
    $installerCert = "$base_dir\..\BuildsInfo\RavenDB\certs\installer.pfx"
    if (!(Test-Path $installerCert)) 
    {
        throw "Could not find pfx file under the path $installerCert to sign the installer"
    }
  
    $certPasswordPath = "$base_dir\..\BuildsInfo\RavenDB\certs\installerCertPassword.txt"
    if (!(Test-Path $certPasswordPath)) 
    {
        throw "Could not find the path for the certificate password of the installer"
    }
    
    $certPassword = Get-Content $certPasswordPath
    if ($certPassword -eq $null) 
    {
        throw "Certificate password must be provided"
    }
    
    Write-Host "Signing the following file: $filePath"

    $timeservers = @("http://tsa.starfieldtech.com", "http://timestamp.globalsign.com/scripts/timstamp.dll", "http://timestamp.comodoca.com/authenticode", "http://www.startssl.com/timestamp", "http://timestamp.verisign.com/scripts/timstamp.dll")
    foreach ($time in $timeservers) {
        try {
            Exec { &$signTool sign /f "$installerCert" /p "$certPassword" /d "RavenDB" /du "http://ravendb.net" /t "$time" "$filePath" }
            return
        }
        catch {
            continue
        }
    }
}

task SignServer {
  $serverFile = "$build_dir\Output\Server\Raven.Server.exe"
  SignFile($serverFile)
  
} 

task CopyInstaller {
    if($env:buildlabel -eq 13)
    {
      return
    }

    Copy-Item $base_dir\Raven.Setup\bin\$global:configuration\RavenDB.Setup.exe "$release_dir\$global:uploadCategory-Build-$env:buildlabel.Setup.exe"
}

task SignInstaller {

  $installerFile = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.Setup.exe"
  SignFile($installerFile)
}

task CopyRootFiles {
    cp $base_dir\license.txt $build_dir\Output\license.txt
    cp $base_dir\Scripts\Start.cmd $build_dir\Output\Start.cmd
    cp $base_dir\Scripts\Raven-UpdateBundles.ps1 $build_dir\Output\Raven-UpdateBundles.ps1
    cp $base_dir\Scripts\Raven-GetBundles.ps1 $build_dir\Output\Raven-GetBundles.ps1
    cp $base_dir\readme.md $build_dir\Output\readme.txt
    cp $base_dir\acknowledgments.txt $build_dir\Output\acknowledgments.txt
    cp $base_dir\CommonAssemblyInfo.cs $build_dir\Output\CommonAssemblyInfo.cs
    
    (Get-Content "$build_dir\Output\Start.cmd") | 
        Foreach-Object { $_ -replace "{build}", "$($env:buildlabel)" } |
        Set-Content "$build_dir\Output\Start.cmd" -Encoding ASCII
}

task ZipOutput {
    
    if($env:buildlabel -eq 13)
    {
        return 
    }

    $old = pwd
    cd $build_dir\Output
    
    $file = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
        
    exec { 
        & $tools_dir\zip.exe -9 -A -r `
            $file `
            Client\*.* `
            Smuggler\*.* `
            Backup\*.* `
            Migration\*.* `
            Web\*.* `
            Bundles\*.* `
            Web\bin\*.* `
            Server\*.* `
            Diag\*.* `
            *.*
    }
    
    cd $old
}


task DoReleasePart1 -depends Compile, `
    CompileDnx, `
    CleanOutputDirectory, `
    CreateOutpuDirectories, `
    CopySmuggler, `
    CopyMonitor, `
    CopyBackup, `
    CopyMigration, `
    CopyClient, `
    CopyWeb, `
    CopyBundles, `
    CopyServer, `
    SignServer, `
    CopyRootFiles, `
    CopyRavenTraffic, `
    CopyRavenApiToken, `
    CopyStorageExporter, `
    ZipOutput { 
    
    Write-Host "Done building RavenDB"
}
task DoRelease -depends DoReleasePart1, `
    CopyInstaller, `
    SignInstaller,
    CreateNugetPackages,
    CreateSymbolSources {   
    
    Write-Host "Done building RavenDB"
}

task UploadStable -depends Stable, DoRelease, Upload, UploadNuget, UpdateLiveTest

task UploadUnstable -depends Unstable, DoRelease, Upload, UploadNuget

task UploadNuget -depends InitNuget, PushNugetPackages, PushSymbolSources

task UpdateLiveTest {
    $appPoolName = "RavenDB 3"
    $appPoolState = (Get-WebAppPoolState $appPoolName).Value
    Write-Host "App pool state is: $appPoolState"

    if($appPoolState -ne "Stopped") {
        Stop-WebAppPool $appPoolName -ErrorAction SilentlyContinue # The error is probably because it was already stopped

        # Wait for the apppool to shut down.
        do
        {
            Write-Host "Wait for '$appPoolState' to be stopped"
            Start-Sleep -Seconds 1
            $appPoolState = (Get-WebAppPoolState $appPoolName).Value
        }
        until ($appPoolState -eq "Stopped")
    }

    if(Test-Path "$liveTest_dir\Plugins") {
        Remove-Item "$liveTest_dir\Plugins\*" -Force -Recurse -ErrorAction SilentlyContinue
    } else {
        mkdir "$liveTest_dir\Plugins" -ErrorAction SilentlyContinue
    }
    Copy-Item "$base_dir\Bundles\Raven.Bundles.LiveTest\bin\Release\Raven.Bundles.LiveTest.dll" "$liveTest_dir\Plugins\Raven.Bundles.LiveTest.dll" -ErrorAction SilentlyContinue
    
    Remove-Item "\bin" -Force -Recurse -ErrorAction SilentlyContinue
    mkdir "$liveTest_dir\bin" -ErrorAction SilentlyContinue
    Copy-Item "$build_dir\Output\Web\bin" "$liveTest_dir\" -Recurse -ErrorAction SilentlyContinue

    $appPoolState = (Get-WebAppPoolState $appPoolName).Value
    Write-Host "App pool state is: $appPoolState"

    if ($appPoolState -eq "Stopped") {
        Write-Output "Starting IIS app pool $appPoolName"
        Start-WebAppPool $appPoolName
    } else {
        Write-Output "Restarting IIS app pool $appPoolName"
        Restart-WebAppPool $appPoolName
    }
    # Wait for the apppool to start.
    do
    {
        Write-Host "Wait for '$appPoolState' to be started"
        Start-Sleep -Seconds 1
        $appPoolState = (Get-WebAppPoolState $appPoolName).Value
    }
    until ($appPoolState -eq "Started")

    Write-Output "Done updating $appPoolName"
}

task MonitorLiveTestRunning {
    $appPoolName = "RavenDB 3"
    $appPoolState = (Get-WebAppPoolState $appPoolName).Value
    Write-Host "App pool state is: $appPoolState"

    if ($appPoolState -eq "Stopped") {
        Write-Output "Starting IIS app pool $appPoolName"
        Start-WebAppPool $appPoolName

        # Wait for the apppool to start.
        do
        {
            Write-Host "Wait for '$appPoolState' to be started"
            Start-Sleep -Seconds 1
            $appPoolState = (Get-WebAppPoolState $appPoolName).Value
        }
        until ($appPoolState -eq "Started")
    }

    Write-Output "Done monitoring $appPoolName"
}

task Upload {
    Write-Host "Starting upload"
    if (Test-Path $uploader) {
        $log = $env:push_msg 
        if(($log -eq $null) -or ($log.Length -eq 0)) {
          $log = git log -n 1 --oneline     
        }
        
        $log = $log.Replace('"','''') # avoid problems because of " escaping the output
        
        $zipFile = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
        $installerFile = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.Setup.exe"
        
        $files = @(@($installerFile, $uploadCategory.Replace("RavenDB", "RavenDB Installer")) , @($zipFile, "$uploadCategory"))
        
        foreach ($obj in $files)
        {
            $file = $obj[0]
            $currentUploadCategory = $obj[1]
            write-host "Executing: $uploader ""$currentUploadCategory"" ""$env:buildlabel"" $file ""$log"""
            
            $uploadTryCount = 0
            while ($uploadTryCount -lt 5) {
                $uploadTryCount += 1
                Exec { &$uploader "$currentUploadCategory" "$env:buildlabel" $file "$log" }
                
                if ($lastExitCode -ne 0) {
                    write-host "Failed to upload to S3: $lastExitCode. UploadTryCount: $uploadTryCount"
                }
                else {
                    break
                }
            }
            
            if ($lastExitCode -ne 0) {
                write-host "Failed to upload to S3: $lastExitCode. UploadTryCount: $uploadTryCount. Build will fail."
                throw "Error: Failed to publish build"
            }
        }
    }
    else {
        Write-Host "could not find upload script $uploadScript, skipping upload"
    }
}

task InitNuget {

    $global:nugetVersion = "$version.$env:buildlabel"
    if ($global:uploadCategory -and $global:uploadCategory.EndsWith("-Unstable")){
        $global:nugetVersion += "-Unstable"
    }

}

task PushNugetPackages {
    # Upload packages
    $accessPath = "$base_dir\..\Nuget-Access-Key.txt"
    $sourceFeed = "https://nuget.org/"
    
    if ($global:uploadMode -eq "Vnext3") {
        $accessPath = "$base_dir\..\MyGet-Access-Key.txt"
        $sourceFeed = "http://www.myget.org/F/ravendb3/api/v2/package"
    }
    
    if ( (Test-Path $accessPath) ) {
        $accessKey = Get-Content $accessPath
        $accessKey = $accessKey.Trim()
        
        $nuget_dir = "$build_dir\NuGet"

        # Push to nuget repository
        $packages = Get-ChildItem $nuget_dir *.nuspec -recurse

        $packages | ForEach-Object {
            $tries = 0
            while ($tries -lt 10) {
                try {
                    &"$nuget" push "$($_.BaseName).$global:nugetVersion.nupkg" $accessKey -Source $sourceFeed -Timeout 4800
                    $tries = 100
                } catch {
                    $tries++
                }
            }
        }
        
    }
    else {
        Write-Host "$accessPath does not exit. Cannot publish the nuget package." -ForegroundColor Yellow
    }
}

task CreateNugetPackages -depends Compile, CompileDnx, CompileHtml5, InitNuget {

    Remove-Item $base_dir\RavenDB*.nupkg
    
    $nuget_dir = "$build_dir\NuGet"
    Remove-Item $nuget_dir -Force -Recurse -ErrorAction SilentlyContinue
    New-Item $nuget_dir -Type directory | Out-Null
    
    New-Item $nuget_dir\RavenDB.Client\lib\net45 -Type directory | Out-Null
    @("Raven.Client.Lightweight.???", "Raven.Abstractions.???") |% { Copy-Item "$base_dir\Raven.Client.Lightweight\bin\$global:configuration\$_" $nuget_dir\RavenDB.Client\lib\net45 }
    
    $nuspecPath = "$nuget_dir\RavenDB.Client\RavenDB.Client.nuspec"
    Copy-Item $base_dir\NuGet\RavenDB.Client.nuspec "$nuspecPath"
    
    if ($global:uploadMode -eq "Unstable") 
    {
        [xml] $xmlNuspec = Get-Content("$nuget_dir\RavenDB.Client\RavenDB.Client.nuspec")
    
        New-Item $nuget_dir\RavenDB.Client\lib\dnxcore50 -Type directory | Out-Null
        @("Raven.Client.Lightweight.???", "Raven.Abstractions.???", "Sparrow.???") |% { Copy-Item "$build_dir\DNX\Raven.Client\$global:configuration\dnxcore50\$_" $nuget_dir\RavenDB.Client\lib\dnxcore50 }
        
        $projects = "$base_dir\Raven.Sparrow\Sparrow", "$base_dir\Raven.Client.Lightweight", "$base_dir\Raven.Abstractions"
        AddDependenciesToNuspec $projects "$nuspecPath" "dnxcore50"
    }

    New-Item $nuget_dir\RavenDB.Client.MvcIntegration\lib\net45 -Type directory | Out-Null
    Copy-Item $base_dir\NuGet\RavenDB.Client.MvcIntegration.nuspec $nuget_dir\RavenDB.Client.MvcIntegration\RavenDB.Client.MvcIntegration.nuspec
    @("Raven.Client.MvcIntegration.???") |% { Copy-Item "$base_dir\Raven.Client.MvcIntegration\bin\$global:configuration\$_" $nuget_dir\RavenDB.Client.MvcIntegration\lib\net45 }
        
    New-Item $nuget_dir\RavenDB.Database\lib\net45 -Type directory | Out-Null
    New-Item $nuget_dir\RavenDB.Database\content -Type directory | Out-Null
    New-Item $nuget_dir\RavenDB.Database\tools -Type directory | Out-Null
    Copy-Item $base_dir\NuGet\RavenDB.Database.nuspec $nuget_dir\RavenDB.Database\RavenDB.Database.nuspec
    Copy-Item $base_dir\NuGet\RavenDB.Database.install.ps1 $nuget_dir\RavenDB.Database\tools\install.ps1
    @("Raven.Database.???", "Raven.Abstractions.???") `
         |% { Copy-Item "$base_dir\Raven.Database\bin\$global:configuration\$_" $nuget_dir\RavenDB.Database\lib\net45 }
    Copy-Item "$build_dir\Raven.Studio.Html5.zip" $nuget_dir\RavenDB.Database\content
    Copy-Item $base_dir\NuGet\readme.txt $nuget_dir\RavenDB.Database\ -Recurse
    
    New-Item $nuget_dir\RavenDB.Server -Type directory | Out-Null
    Copy-Item $base_dir\NuGet\RavenDB.Server.nuspec $nuget_dir\RavenDB.Server\RavenDB.Server.nuspec
    New-Item $nuget_dir\RavenDB.Server\tools -Type directory | Out-Null
    @("Raven.Database.???", "Raven.Server.???", "Raven.Abstractions.???") |% { Copy-Item "$base_dir\Raven.Server\bin\$global:configuration\$_" $nuget_dir\RavenDB.Server\tools }
    Copy-Item "$build_dir\Raven.Studio.Html5.zip" $nuget_dir\RavenDB.Server\tools
    @("Raven.Smuggler.???", "Raven.Abstractions.???", "Raven.Database.???") |% { Copy-Item "$base_dir\Raven.Smuggler\bin\$global:configuration\$_" $nuget_dir\RavenDB.Server\tools }
    Copy-Item $base_dir\DefaultConfigs\RavenDb.exe.config $nuget_dir\RavenDB.Server\tools\Raven.Server.exe.config

    New-Item $nuget_dir\RavenDB.Embedded\lib\net45 -Type directory | Out-Null
    Copy-Item $base_dir\NuGet\RavenDB.Embedded.nuspec $nuget_dir\RavenDB.Embedded\RavenDB.Embedded.nuspec
    Copy-Item $base_dir\NuGet\readme.txt $nuget_dir\RavenDB.Embedded\ -Recurse
    
    # Client packages
    @("Authorization", "UniqueConstraints") | Foreach-Object { 
        $name = $_;
        New-Item $nuget_dir\RavenDB.Client.$name\lib\net45 -Type directory | Out-Null
        @("$base_dir\Bundles\Raven.Client.$_\bin\$global:configuration\Raven.Client.$_.???") |% { Copy-Item $_ $nuget_dir\RavenDB.Client.$name\lib\net45 }
        
        $nuspecPath = "$nuget_dir\RavenDB.Client.$name\RavenDB.Client.$name.nuspec"
        Copy-Item $base_dir\NuGet\RavenDB.Client.$name.nuspec "$nuspecPath"
        
        if ($global:uploadMode -eq "Unstable") 
        {
            New-Item $nuget_dir\RavenDB.Client.$name\lib\dnxcore50 -Type directory | Out-Null
            @("$build_dir\DNX\Raven.Client.$name\$global:configuration\dnxcore50\Raven.Client.$_.???") |% { Copy-Item $_ $nuget_dir\RavenDB.Client.$name\lib\dnxcore50 }
            
            $projects = "$base_dir\Bundles\Raven.Client.$name"
            AddDependenciesToNuspec $projects "$nuspecPath" "dnxcore50"
    }
    }
    
    New-Item $nuget_dir\RavenDB.Bundles.Authorization\lib\net45 -Type directory | Out-Null
    Copy-Item $base_dir\NuGet\RavenDB.Bundles.Authorization.nuspec $nuget_dir\RavenDB.Bundles.Authorization\RavenDB.Bundles.Authorization.nuspec
    @("Raven.Bundles.Authorization.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.Authorization\bin\$global:configuration\$_" $nuget_dir\RavenDB.Bundles.Authorization\lib\net45 }
    
    New-Item $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net45 -Type directory | Out-Null
    Copy-Item $base_dir\NuGet\RavenDB.Bundles.CascadeDelete.nuspec $nuget_dir\RavenDB.Bundles.CascadeDelete\RavenDB.Bundles.CascadeDelete.nuspec
    @("Raven.Bundles.CascadeDelete.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.CascadeDelete\bin\$global:configuration\$_" $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net45 }
    
    New-Item $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net45 -Type directory | Out-Null
    Copy-Item $base_dir\NuGet\RavenDB.Bundles.UniqueConstraints.nuspec $nuget_dir\RavenDB.Bundles.UniqueConstraints\RavenDB.Bundles.UniqueConstraints.nuspec
    @("Raven.Bundles.UniqueConstraints.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.UniqueConstraints\bin\$global:configuration\$_" $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net45 }
    
    New-Item $nuget_dir\RavenDB.Tests.Helpers\lib\net45 -Type directory | Out-Null
    Copy-Item $base_dir\NuGet\RavenDB.Tests.Helpers.nuspec $nuget_dir\RavenDB.Tests.Helpers\RavenDB.Tests.Helpers.nuspec
    @("Raven.Tests.Helpers.???", "Raven.Server.???", "Raven.Abstractions.???") |% { Copy-Item "$base_dir\Raven.Tests.Helpers\bin\$global:configuration\$_" $nuget_dir\RavenDB.Tests.Helpers\lib\net45 }
    
    # Sets the package version in all the nuspec as well as any RavenDB package dependency versions
    $packages = Get-ChildItem $nuget_dir *.nuspec -recurse
    $packages |% { 
        $nuspec = [xml](Get-Content $_.FullName)
        $nuspec.package.metadata.version = $global:nugetVersion
        $nuspec | Select-Xml '//dependency' |% {
            if($_.Node.Id.StartsWith('RavenDB')){
                $_.Node.Version = "[$global:nugetVersion]"
            }
        }
        $nuspec.Save($_.FullName);
        Exec { &"$nuget" pack $_.FullName }
    }
}

task PushSymbolSources -depends InitNuget {
    return;

    if ($global:uploadMode -ne "Stable") {
        return; # this takes 20 minutes to run
    }

    # Upload packages
    $accessPath = "$base_dir\..\Nuget-Access-Key.txt"
    $sourceFeed = "https://nuget.org/"
    
    if ($global:uploadMode -eq "Vnext3") {
        $accessPath = "$base_dir\..\MyGet-Access-Key.txt"
        $sourceFeed = "http://www.myget.org/F/ravendb3/api/v2/package"
    }
    
    if ( (Test-Path $accessPath) ) {
        $accessKey = Get-Content $accessPath
        $accessKey = $accessKey.Trim()
        
        $nuget_dir = "$build_dir\NuGet"
    
        $packages = Get-ChildItem $nuget_dir *.nuspec -recurse

        $packages | ForEach-Object {
            try {
                Write-Host "Publish symbol package $($_.BaseName).$global:nugetVersion.symbols.nupkg"
                &"$nuget" push "$($_.BaseName).$global:nugetVersion.symbols.nupkg" $accessKey -Source http://nuget.gw.symbolsource.org/Public/NuGet -Timeout 4800
            } catch {
                Write-Host $error[0]
                $LastExitCode = 0
            }
        }
        
    }
    else {
        Write-Host "$accessPath does not exit. Cannot publish the nuget package." -ForegroundColor Yellow
    }

}

task CreateSymbolSources -depends CreateNugetPackages {
    return;
    
    if ($global:uploadMode -ne "Stable") {
        return; # this takes 20 minutes to run
    }

    $nuget_dir = "$build_dir\NuGet"
    
    $packages = Get-ChildItem $nuget_dir *.nuspec -recurse
    
    # Package the symbols package
    $packages | ForEach-Object { 
        $dirName = [io.path]::GetFileNameWithoutExtension($_)
        Remove-Item $nuget_dir\$dirName\src -Force -Recurse -ErrorAction SilentlyContinue
        New-Item $nuget_dir\$dirName\src -Type directory | Out-Null
        
        $srcDirName1 = $dirName
        $srcDirName1 = $srcDirName1.Replace("RavenDB.", "Raven.")
        $srcDirName1 = $srcDirName1.Replace(".AspNetHost", ".Web")
        $srcDirName1 = $srcDirName1 -replace "Raven.Client$", "Raven.Client.Lightweight"
        $srcDirName1 = $srcDirName1.Replace("Raven.Bundles.", "Bundles\Raven.Bundles.")
        $srcDirName1 = $srcDirName1.Replace("Raven.Client.Authorization", "Bundles\Raven.Client.Authorization")
        $srcDirName1 = $srcDirName1.Replace("Raven.Client.UniqueConstraints", "Bundles\Raven.Client.UniqueConstraints")
        
        $srcDirNames = @($srcDirName1)
        if ($dirName -eq "RavenDB.Server") {
            $srcDirNames += @("Raven.Smuggler")
        }       
        
        foreach ($srcDirName in $srcDirNames) {
            Write-Host $srcDirName
            $csprojFile = $srcDirName -replace ".*\\", ""
            $csprojFile += ".csproj"
        
            Get-ChildItem $srcDirName\*.cs -Recurse |   ForEach-Object {
                $indexOf = $_.FullName.IndexOf($srcDirName)
                $copyTo = $_.FullName.Substring($indexOf + $srcDirName.Length + 1)
                $copyTo = "$nuget_dir\$dirName\src\$copyTo"
                
                New-Item -ItemType File -Path $copyTo -Force | Out-Null
                Copy-Item $_.FullName $copyTo -Recurse -Force
            }

            Write-Host .csprojFile $csprojFile -Fore Yellow
            Write-Host Copy Linked Files of $srcDirName -Fore Yellow
            
            [xml]$csProj = Get-Content $srcDirName\$csprojFile
            Write-Host $srcDirName\$csprojFile -Fore Green
            foreach ($compile in $csProj.Project.ItemGroup.Compile){
                if ($compile.Link.Length -gt 0) {
                    $fileToCopy = $compile.Include
                    $copyToPath = $fileToCopy -replace "(\.\.\\)*", ""
                    
                    
                        Write-Host "Copy $srcDirName\$fileToCopy" -ForegroundColor Magenta
                        Write-Host "To $nuget_dir\$dirName\src\$copyToPath" -ForegroundColor Magenta
                    
                    if ($fileToCopy.EndsWith("\*.cs")) {
                        #Get-ChildItem "$srcDirName\$fileToCopy" | ForEach-Object {
                        #   Copy-Item $_.FullName "$nuget_dir\$dirName\src\$copyToPath".Replace("\*.cs", "\") -Recurse -Force
                        #}
                    } else {
                        New-Item -ItemType File -Path "$nuget_dir\$dirName\src\$copyToPath" -Force | Out-Null
                        Copy-Item "$srcDirName\$fileToCopy" "$nuget_dir\$dirName\src\$copyToPath" -Recurse -Force
                    }
                }
            }
            
            
            foreach ($projectReference in $csProj.Project.ItemGroup.ProjectReference){
                Write-Host "Visiting project $($projectReference.Include) of $dirName" -Fore Green
                if ($projectReference.Include.Length -gt 0) {
                
                    $projectPath = $projectReference.Include
                    Write-Host "Include also linked files of $($projectReference.Include)" -Fore Green

                    $srcDirName2 = [io.path]::GetFileNameWithoutExtension($projectPath)

                    if ($srcDirName2 -eq "Voron") {
                        $srcDirName2 = "Raven.Voron/" + $srcDirName2
                    }

                    Get-ChildItem $srcDirName2\*.cs -Recurse |  ForEach-Object {
                        $indexOf = $_.FullName.IndexOf($srcDirName2)    
                        $copyTo = $_.FullName.Substring($indexOf + $srcDirName2.Length + 1)
                        $copyTo = "$nuget_dir\$dirName\src\$srcDirName2\$copyTo"
                        
                        New-Item -ItemType File -Path $copyTo -Force | Out-Null
                        Copy-Item $_.FullName $copyTo -Recurse -Force
                    }
                    
                    [xml]$global:csProj2;
                    try {
                        [xml]$global:csProj2 = Get-Content "$srcDirName2\$projectPath"
                    } catch {
                        $projectPath = $projectPath.Replace("..\..\", "..\")
                        Write-Host "Try to include also linked files of $($projectReference.Include)" -Fore Green
                        [xml]$global:csProj2 = Get-Content "$srcDirName2\$projectPath"
                    }
                    
                    foreach ($compile in $global:csProj2.Project.ItemGroup.Compile){
                        if ($compile.Link.Length -gt 0) {
                            $fileToCopy = ""
                            if ($srcDirName2.Contains("Bundles\") -and !$srcDirName2.EndsWith("\..")) {
                                $srcDirName2 += "\.."
                            }
                            $fileToCopy = $compile.Include;
                            $copyToPath = $fileToCopy -replace "(\.\.\\)*", ""
                            
                            if ($global:isDebugEnabled) {
                                Write-Host "Copy $srcDirName2\$fileToCopy" -ForegroundColor Magenta
                                Write-Host "To $nuget_dir\$dirName\src\$copyToPath" -ForegroundColor Magenta
                            }
                            
                            if ($fileToCopy.EndsWith("\*.cs")) {
                            # do nothing
                            } else {
                                New-Item -ItemType File -Path "$nuget_dir\$dirName\src\$copyToPath" -Force | Out-Null
                                Copy-Item "$srcDirName2\$fileToCopy" "$nuget_dir\$dirName\src\$copyToPath" -Recurse -Force
                            }
                        }
                    }  
                    
                }
            }
        }
        
        Get-ChildItem "$nuget_dir\$dirName\src\*.dll" -recurse -exclude Raven* | ForEach-Object {
            Remove-Item $_ -force -recurse -ErrorAction SilentlyContinue
        }
        Get-ChildItem "$nuget_dir\$dirName\src\*.pdb" -recurse -exclude Raven* | ForEach-Object {
            Remove-Item $_ -force -recurse -ErrorAction SilentlyContinue
        }
        Get-ChildItem "$nuget_dir\$dirName\src\*.xml" -recurse | ForEach-Object {
            Remove-Item $_ -force -recurse -ErrorAction SilentlyContinue
        }
        
        Remove-Item "$nuget_dir\$dirName\src\bin" -force -recurse -ErrorAction SilentlyContinue
        Remove-Item "$nuget_dir\$dirName\src\obj" -force -recurse -ErrorAction SilentlyContinue
        
        Exec { &"$nuget" pack $_.FullName -Symbols }
    }
}

TaskTearDown {
    
    if ($LastExitCode -ne 0) {
        write-host "TaskTearDown detected an error. Build failed." -BackgroundColor Red -ForegroundColor Yellow
        write-host "Yes, something has failed!!!!!!!!!!!!!!!!!!!!!" -BackgroundColor Red -ForegroundColor Yellow
        # throw "TaskTearDown detected an error. Build failed."
        exit 1
    }
}

function AddDependenciesToNuspec($projects, $nuspecPath, $framework)
{
    [xml] $xmlNuspec = Get-Content("$nuspecPath")
    
    $dnxDependencies = New-Object 'System.Collections.Generic.Dictionary[String,String]'

    $xmlDependencies = $xmlNuspec.SelectSingleNode('//package/metadata/dependencies')
    $xmlFrameworkDependency = $xmlNuspec.CreateElement("group")
    $xmlFrameworkDependency.SetAttribute("targetFramework", $framework)
    
    $xmlDependencies.AppendChild($xmlFrameworkDependency)
    
    foreach ($project in $projects)
    {
        $projectJson = Get-Content "$project\project.json" -Raw | ConvertFrom-Json
        $frameworks = $projectJson.frameworks
        $dependencies = $frameworks."$framework".dependencies

        foreach ($dependency in $dependencies.psobject.properties)
        {
            $dependencyName = $dependency.name;
            $dependencyVersion = $dependency.value;

            $dnxDependencies[$dependencyName] = $dependencyVersion
        }
    }

    foreach ($dependency in $dnxDependencies.Keys)
    {
        $xmlDependency = $xmlNuspec.CreateElement("dependency")
        $xmlDependency.SetAttribute("id", $dependency)
        $xmlDependency.SetAttribute("version", $dnxDependencies[$dependency])

        $xmlFrameworkDependency.AppendChild($xmlDependency)
    }
    
    $xmlNuspec.Save("$nuspecPath")
}
