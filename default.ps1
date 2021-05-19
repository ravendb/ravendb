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
    $version = "3.0"
    $tools_dir = "$base_dir\Tools"
    $release_dir = "$base_dir\Release"
    $uploader = "..\Uploader\S3Uploader.exe"
    $global:configuration = "Release"
    $v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
    $nowarn = "1591 1573 1591 1701 1702 1705"
    
	$global:msbuild = ""
	
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
	
	$global:msbuild = "C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\MSBuild\Current\Bin\MSBuild.exe"
    if (!(Test-Path -Path $global:msbuild)) {
        $global:msbuild = "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\MSBuild\15.0\Bin\MSBuild.exe"
    }
}

task Compile -depends Init, CompileHtml5 {
    
    $commit = Get-Git-Commit-Full

    Write-Host "Compiling with '$global:configuration' configuration" -ForegroundColor Yellow

    &"$global:msbuild" "$sln_file" /p:Configuration=$global:configuration /p:nowarn="$nowarn" /maxcpucount /verbosity:minimal
    
    Write-Host "msbuild exit code: $LastExitCode"

    if( $LastExitCode -ne 0){
        throw "Failed to build"
    }
    
    if ($commit -ne "0000000000000000000000000000000000000000") {
        exec { &"$tools_dir\GitLink.Custom.exe" "$base_dir" /u https://github.com/ravendb/ravendb /c $global:configuration /b master /s "$commit" /f "$sln_file_name" }
    }
    
    exec { &"$tools_dir\Assembly.Validator.exe" "$lib_dir" "$lib_dir\Sources\" }
}

task CompileHtml5 {
    
    "{ ""BuildVersion"": $env:buildlabel }" | Out-File "Raven.Studio.Html5\version.json" -Encoding UTF8
    
    Write-Host "Compiling HTML5" -ForegroundColor Yellow

    &"$global:msbuild" "Raven.Studio.Html5\Raven.Studio.Html5.csproj" /p:Configuration=$global:configuration /p:nowarn="$nowarn" /maxcpucount /verbosity:minimal
    
    Remove-Item $build_dir\Html5 -Force -Recurse -ErrorAction SilentlyContinue | Out-Null
    Remove-Item $build_dir\Raven.Studio.Html5.zip -Force -ErrorAction SilentlyContinue | Out-Null
    
    Set-Location $base_dir\Raven.Studio.Html5
    exec { & $tools_dir\Pvc\pvc.exe optimized-build }
    
    Copy-Item $base_dir\Raven.Studio.Html5\optimized-build $build_dir\Html5 -Recurse

    Set-Location $build_dir\Html5
    exec { & $tools_dir\zip.exe -9 -A -r $build_dir\Raven.Studio.Html5.zip *.* }
    Set-Location $base_dir
}

task FullStorageTest {
    $global:full_storage_test = $true
}

task Test -depends Compile {
    Clear-Host

    $test_prjs = New-Object System.Collections.ArrayList

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
        
        if ($Env:JENKINS_URL) {
            $dll_dir = Split-Path $_ -Parent
            &"$xUnit" "$_" "/nunit" "$dll_dir\testResults.xml"
        } else {
            &"$xUnit" "$_"
        }

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

task Hotfix {
    $global:uploadCategory = "RavenDB-Hotfix"
    $global:uploadMode = "Unstable"
    $global:configuration = "Release"
}

task Stable {
    $global:uploadCategory = "RavenDB"
    $global:uploadMode = "Stable"
    $global:configuration = "Release"
}

task Custom {
    $global:uploadCategory = "RavenDB-Custom"
    $global:uploadMode = "Unstable"
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
    New-Item $build_dir\Output\Diag\StorageExporter -Type directory | Out-Null
}

task CleanOutputDirectory { 
    Remove-Item $build_dir\Output -Recurse -Force -ErrorAction SilentlyContinue
}

task CopySmuggler {
    Copy-Item $base_dir\Raven.Smuggler\bin\$global:configuration\Raven.Abstractions.??? $build_dir\Output\Smuggler
    Copy-Item $base_dir\Raven.Smuggler\bin\$global:configuration\Raven.Client.Lightweight.??? $build_dir\Output\Smuggler
    Copy-Item $base_dir\Raven.Smuggler\bin\$global:configuration\Jint.Raven.??? $build_dir\Output\Smuggler
    Copy-Item $base_dir\Raven.Smuggler\bin\$global:configuration\System.Reactive.Core.??? $build_dir\Output\Smuggler
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
                $signTool = "C:\Program Files (x86)\Microsoft SDKs\ClickOnce\SignTool\signtool.exe"

                if (!(Test-Path $signTool)) 
                {
                    throw "Could not find SignTool.exe under the specified path $signTool"
                }
            }
        }
    }

    $installerCert = "$base_dir\..\BuildsInfo\RavenDB\certs\code-sign.pfx"
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
            Exec { &$signTool sign /f "$installerCert" /p "$certPassword" /d "RavenDB" /du "http://ravendb.net" /t "$time" /v /debug "$filePath" }
            return
        }
        catch {
            continue
        }
    }

    throw "Could not reach any of the timeservers"
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
        Set-Content "$build_dir\Output\Start.cmd" -Encoding Default
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
    CleanOutputDirectory, `
    CreateOutpuDirectories, `
    CopySmuggler, `
    CopyBackup, `
    CopyMigration, `
    CopyClient, `
    CopyWeb, `
    CopyBundles, `
    CopyServer, `
    SignServer, `
    CopyRootFiles, `
    CopyRavenTraffic, `
    CopyStorageExporter, `
    ZipOutput { 
    
    Write-Host "Done building RavenDB"
}
task DoRelease -depends DoReleasePart1, `
    CopyInstaller, `
    SignInstaller,
    CreateNugetPackages {   
    
    Write-Host "Done building RavenDB"
}

task UploadStable -depends Stable, DoRelease, Upload

task UploadUnstable -depends Unstable, DoRelease, Upload

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
        
        $files = @(@($installerFile, $global:uploadCategory.Replace("RavenDB", "RavenDB Installer")) , @($zipFile, "$global:uploadCategory"))
        
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
    if ($global:uploadCategory -and $global:uploadCategory -ne "RavenDB"){
        $global:nugetVersion += "-" + $global:uploadCategory.Replace("RavenDB-", "")
    }

}

task CreateNugetPackages -depends Compile, CompileHtml5, InitNuget {

    Remove-Item $base_dir\RavenDB*.nupkg
    
    $nuget_dir = "$build_dir\NuGet"
    Remove-Item $nuget_dir -Force -Recurse -ErrorAction SilentlyContinue
    New-Item $nuget_dir -Type directory | Out-Null
    
    New-Item $nuget_dir\RavenDB.Client\lib\net45 -Type directory | Out-Null  
    @("Raven.Client.Lightweight.???", "Raven.Abstractions.???") |% { Copy-Item "$base_dir\Raven.Client.Lightweight\bin\$global:configuration\$_" $nuget_dir\RavenDB.Client\lib\net45 }
    
    $nuspecPath = "$nuget_dir\RavenDB.Client\RavenDB.Client.nuspec"
    Copy-Item $base_dir\NuGet\RavenDB.Client.nuspec "$nuspecPath"

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
    @("Raven.Smuggler.???", "Raven.Abstractions.???") |% { Copy-Item "$base_dir\Raven.Smuggler\bin\$global:configuration\$_" $nuget_dir\RavenDB.Server\tools }
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

TaskTearDown {
    
    if ($LastExitCode -ne 0) {
        write-host "TaskTearDown detected an error. Build failed." -BackgroundColor Red -ForegroundColor Yellow
        write-host "Yes, something has failed!!!!!!!!!!!!!!!!!!!!!" -BackgroundColor Red -ForegroundColor Yellow
        # throw "TaskTearDown detected an error. Build failed."
        exit 1
    }
}
