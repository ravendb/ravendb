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
	$build_dir = "$base_dir\build"
	$sln_file = "$base_dir\zzz_RavenDB_Release.sln"
	$version = "3.0"
	$tools_dir = "$base_dir\Tools"
	$release_dir = "$base_dir\Release"
	$uploader = "..\Uploader\S3Uploader.exe"
	$global:configuration = "Debug"
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

task Init -depends Verify40, Clean {

	$commit = Get-Git-Commit
	(Get-Content "$base_dir\CommonAssemblyInfo.cs") | 
		Foreach-Object { $_ -replace ".13", ".$($env:buildlabel)" } |
		Foreach-Object { $_ -replace "{commit}", $commit } |
		Set-Content "$base_dir\CommonAssemblyInfo.cs" -Encoding UTF8
	
	New-Item $release_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
	New-Item $build_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
}

task Compile -depends Init {
	
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	Write-Host "Compiling with '$global:configuration' configuration" -ForegroundColor Yellow
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$sln_file" /p:Configuration=$global:configuration /p:nowarn="1591 1573" }
}

task CompileHtml5 {
	
	Remove-Item $build_dir\Html5 -Force -Recurse -ErrorAction SilentlyContinue | Out-Null
	Remove-Item $build_dir\Raven.Studio.Html5.zip -Force -ErrorAction SilentlyContinue | Out-Null
	Copy-Item $base_dir\Raven.Studio.Html5 $build_dir\Html5 -Recurse

	Remove-Item $build_dir\Html5\web*.config -Force -Recurse
	Remove-Item $build_dir\Html5\packages.config -Force -Recurse
	Remove-Item $build_dir\Html5\Raven.Studio.Html5.csproj* -Force -Recurse
	Remove-Item $build_dir\Html5\App_Readme -Force -Recurse  -ErrorAction SilentlyContinue
	Remove-Item $build_dir\Html5\bin -Force -Recurse
	Remove-Item $build_dir\Html5\obj -Force -Recurse
	Remove-Item $build_dir\Html5\Properties -Force -Recurse
	Remove-Item $build_dir\Html5\build -Force -Recurse -ErrorAction SilentlyContinue | Out-Null

	Set-Location $build_dir\Html5
	exec { & $tools_dir\zip.exe -9 -A -r $build_dir\Raven.Studio.Html5.zip *.* }
	Set-Location $base_dir
}

task Java {

$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "RavenDB.sln" /p:Configuration=$global:configuration /p:nowarn="1591 1573" }

}

task FullStorageTest {
	$global:full_storage_test = $true
}

task Test -depends Compile {
	Clear-Host

	$test_prjs = @( `
		"$base_dir\Raven.Tests\bin\$global:configuration\Raven.Tests.dll", `
		"$base_dir\Raven.Tests.Bundles\bin\$global:configuration\Raven.Tests.Bundles.dll", `
		"$base_dir\Raven.Tests.Issues\bin\$global:configuration\Raven.Tests.Issues.dll",  `
		"$base_dir\Raven.Tests.MailingList\bin\$global:configuration\Raven.Tests.MailingList.dll", `
		"$base_dir\Raven.SlowTests\bin\$global:configuration\Raven.SlowTests.dll",`
		"$base_dir\Raven.DtcTests\bin\$global:configuration\Raven.DtcTests.dll" )
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
task Vnext3 {
	$global:uploadCategory = "RavenDB-Unstable"
	$global:uploadMode = "Vnext3"
	$global:configuration = "Release"
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
	Copy-Item $build_dir\Raven.Client.Lightweight.??? $build_dir\Output\Backup
}

task CopyClient {
	$client_dlls = @( "$base_dir\Raven.Client.Lightweight\bin\$global:configuration\Raven.Client.Lightweight.???")
	$client_dlls | ForEach-Object { Copy-Item "$_" $build_dir\Output\Client }
}

task CopyWeb {
	@( "$base_dir\Raven.Database\bin\$global:configuration\Raven.Database.???", 
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
	
	$signTool = "C:\Program Files (x86)\Windows Kits\8.0\bin\x86\signtool.exe"
	if (!(Test-Path $signTool)) 
	{
		$signTool = "C:\Program Files\Microsoft SDKs\Windows\v7.1\Bin\signtool.exe"
	
		if (!(Test-Path $signTool)) 
		{
			throw "Could not find SignTool.exe under the specified path $signTool"
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
	Exec { &$signTool sign /f "$installerCert" /p "$certPassword" /d "RavenDB" /du "http://ravendb.net" /t "http://timestamp.verisign.com/scripts/timstamp.dll" "$filePath" }
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

task CreateDocs {
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	if($env:buildlabel -eq 13)
	{
	  return 
	}

	if ($global:uploadMode -ne "Stable"){
		return; # this takes 8 minutes to run
	}
	 
	# we expliclty allows this to fail
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Raven.Docs.shfbproj" /p:OutDir="$build_dir\" }
}

task CopyRootFiles -depends CreateDocs {
	cp $base_dir\license.txt $build_dir\Output\license.txt
	cp $base_dir\Scripts\Start.cmd $build_dir\Output\Start.cmd
	cp $base_dir\Scripts\Raven-UpdateBundles.ps1 $build_dir\Output\Raven-UpdateBundles.ps1
	cp $base_dir\Scripts\Raven-GetBundles.ps1 $build_dir\Output\Raven-GetBundles.ps1
	cp $base_dir\readme.md $build_dir\Output\readme.txt
	cp $base_dir\Help\Documentation.chm $build_dir\Output\Documentation.chm  -ErrorAction SilentlyContinue
	cp $base_dir\acknowledgments.txt $build_dir\Output\acknowledgments.txt
	cp $base_dir\CommonAssemblyInfo.cs $build_dir\Output\CommonAssemblyInfo.cs
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
			Web\*.* `
			Bundles\*.* `
			Web\bin\*.* `
			Server\*.* `
			*.*
	}
	
	cd $old
}


task DoReleasePart1 -depends Compile, `
	CompileHtml5, `
	CleanOutputDirectory, `
	CreateOutpuDirectories, `
	CopySmuggler, `
	CopyBackup, `
	CopyClient, `
	CopyWeb, `
	CopyBundles, `
	CopyServer, `
	SignServer, `
	CopyRootFiles, `
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

task UploadStable -depends Stable, DoRelease, Upload, UploadNuget

task UploadUnstable -depends Unstable, DoRelease, Upload, UploadNuget

task UploadVnext3 -depends Vnext3, DoRelease, Upload, UploadNuget

task UploadNuget -depends InitNuget, PushNugetPackages, PushSymbolSources

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
					&"$base_dir\.nuget\NuGet.exe" push "$($_.BaseName).$global:nugetVersion.nupkg" $accessKey -Source $sourceFeed -Timeout 4800
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

task CreateNugetPackages -depends Compile, InitNuget {

	Remove-Item $base_dir\RavenDB*.nupkg
	
	$nuget_dir = "$build_dir\NuGet"
	Remove-Item $nuget_dir -Force -Recurse -ErrorAction SilentlyContinue
	New-Item $nuget_dir -Type directory | Out-Null
	
	New-Item $nuget_dir\RavenDB.Client\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Client.nuspec $nuget_dir\RavenDB.Client\RavenDB.Client.nuspec
	
	@("Raven.Client.Lightweight.???") |% { Copy-Item "$base_dir\Raven.Client.Lightweight\bin\$global:configuration\$_" $nuget_dir\RavenDB.Client\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Client.MvcIntegration\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Client.MvcIntegration.nuspec $nuget_dir\RavenDB.Client.MvcIntegration\RavenDB.Client.MvcIntegration.nuspec
	@("Raven.Client.MvcIntegration.???") |% { Copy-Item "$base_dir\Raven.Client.MvcIntegration\bin\$global:configuration\$_" $nuget_dir\RavenDB.Client.MvcIntegration\lib\net45 }
		
	New-Item $nuget_dir\RavenDB.Database\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Database.nuspec $nuget_dir\RavenDB.Database\RavenDB.Database.nuspec
	@("Raven.Database.???") `
		 |% { Copy-Item "$base_dir\Raven.Database\bin\$global:configuration\$_" $nuget_dir\RavenDB.Database\lib\net45 }
	Copy-Item "$build_dir\Raven.Studio.Html5.zip" $nuget_dir\RavenDB.Database\lib\net45
	
	New-Item $nuget_dir\RavenDB.Server -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Server.nuspec $nuget_dir\RavenDB.Server\RavenDB.Server.nuspec
	New-Item $nuget_dir\RavenDB.Server\tools -Type directory | Out-Null
	@("Raven.Database.???", "Raven.Server.???") |% { Copy-Item "$base_dir\Raven.Server\bin\$global:configuration\$_" $nuget_dir\RavenDB.Server\tools }
	Copy-Item "$build_dir\Raven.Studio.Html5.zip" $nuget_dir\RavenDB.Server\tools
	@("Raven.Smuggler.???") |% { Copy-Item "$base_dir\Raven.Smuggler\bin\$global:configuration\$_" $nuget_dir\RavenDB.Server\tools }
	Copy-Item $base_dir\DefaultConfigs\RavenDb.exe.config $nuget_dir\RavenDB.Server\tools\Raven.Server.exe.config

	New-Item $nuget_dir\RavenDB.Embedded\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Embedded.nuspec $nuget_dir\RavenDB.Embedded\RavenDB.Embedded.nuspec
	
	# Client packages
	@("Authorization", "UniqueConstraints") | Foreach-Object { 
		$name = $_;
		New-Item $nuget_dir\RavenDB.Client.$name\lib\net45 -Type directory | Out-Null
		Copy-Item $base_dir\NuGet\RavenDB.Client.$name.nuspec $nuget_dir\RavenDB.Client.$name\RavenDB.Client.$name.nuspec
		@("$base_dir\Bundles\Raven.Client.$_\bin\$global:configuration\Raven.Client.$_.???") |% { Copy-Item $_ $nuget_dir\RavenDB.Client.$name\lib\net45 }
	}
	
	New-Item $nuget_dir\RavenDB.Bundles.Authorization\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.Authorization.nuspec $nuget_dir\RavenDB.Bundles.Authorization\RavenDB.Bundles.Authorization.nuspec
	@("Raven.Bundles.Authorization.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.Authorization.$_\bin\$global:configuration\$_" $nuget_dir\RavenDB.Bundles.Authorization\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.CascadeDelete.nuspec $nuget_dir\RavenDB.Bundles.CascadeDelete\RavenDB.Bundles.CascadeDelete.nuspec
	@("Raven.Bundles.CascadeDelete.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.CascadeDelete.$_\bin\$global:configuration\$_" $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.UniqueConstraints.nuspec $nuget_dir\RavenDB.Bundles.UniqueConstraints\RavenDB.Bundles.UniqueConstraints.nuspec
	@("Raven.Bundles.UniqueConstraints.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.UniqueConstraints.$_\bin\$global:configuration\$_" $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Tests.Helpers\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Tests.Helpers.nuspec $nuget_dir\RavenDB.Tests.Helpers\RavenDB.Tests.Helpers.nuspec
	@("Raven.Tests.Helpers.???", "Raven.Server.???") |% { Copy-Item "$base_dir\Raven.Tests.Helpers.$_\bin\$global:configuration\$_" $nuget_dir\RavenDB.Tests.Helpers\lib\net45 }
	New-Item $nuget_dir\RavenDB.Tests.Helpers\content -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenTests $nuget_dir\RavenDB.Tests.Helpers\content\RavenTests -Recurse
	
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
		Exec { &"$base_dir\.nuget\nuget.exe" pack $_.FullName }
	}
}

task PushSymbolSources -depends InitNuget {
	
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
				&"$base_dir\.nuget\NuGet.exe" push "$($_.BaseName).$global:nugetVersion.symbols.nupkg" $accessKey -Source http://nuget.gw.symbolsource.org/Public/NuGet -Timeout 4800
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
		
			Get-ChildItem $srcDirName\*.cs -Recurse |	ForEach-Object {
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
						#	Copy-Item $_.FullName "$nuget_dir\$dirName\src\$copyToPath".Replace("\*.cs", "\") -Recurse -Force
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

					Get-ChildItem $srcDirName2\*.cs -Recurse |	ForEach-Object {
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
							
							New-Item -ItemType File -Path "$nuget_dir\$dirName\src\$copyToPath" -Force | Out-Null
							Copy-Item "$srcDirName2\$fileToCopy" "$nuget_dir\$dirName\src\$copyToPath" -Recurse -Force
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
		
		Exec { &"$base_dir\.nuget\nuget.exe" pack $_.FullName -Symbols }
	}
}

TaskTearDown {
	
	if ($LastExitCode -ne 0) {
		write-host "TaskTearDown detected an error. Build failed." -BackgroundColor Red -ForegroundColor Yellow
		write-host "Yes, something was failed!!!!!!!!!!!!!!!!!!!!!" -BackgroundColor Red -ForegroundColor Yellow
		# throw "TaskTearDown detected an error. Build failed."
		exit 1
	}
}
