Include ".\build_utils.ps1"

properties {
	$base_dir  = resolve-path .
	$lib_dir = "$base_dir\SharedLibs"
	$packages_dir = "$base_dir\packages"
	$buildartifacts_dir = "$base_dir\build"
	$sln_file = "$base_dir\zzz_RavenDB_Release.sln"
	$version = "2.5"
	$tools_dir = "$base_dir\Tools"
	$release_dir = "$base_dir\Release"
	$uploader = "..\Uploader\S3Uploader.exe"
	$global:configuration = "Release"
	
	$core_db_dlls = @(
        "$base_dir\Raven.Database\bin\Release\Raven.Abstractions.???", 
        (Get-DependencyPackageFiles 'NLog.2'), 
        (Get-DependencyPackageFiles Microsoft.Web.Infrastructure), 
        "$base_dir\Raven.Database\bin\Release\Jint.Raven.???",
				"$base_dir\Raven.Database\bin\Release\Lucene.Net.???",
				"$base_dir\Raven.Database\bin\Release\Microsoft.Data.Edm.???",
				"$base_dir\Raven.Database\bin\Release\Microsoft.WindowsAzure.Storage.???",
				"$base_dir\Raven.Database\bin\Release\Microsoft.Data.OData.???",
				"$base_dir\Raven.Database\bin\Release\Microsoft.WindowsAzure.ConfigurationManager.???",
				"$base_dir\Raven.Database\bin\Release\Lucene.Net.Contrib.Spatial.NTS.???", 
				"$base_dir\Raven.Database\bin\Release\Spatial4n.Core.NTS.???", 
				"$base_dir\Raven.Database\bin\Release\GeoAPI.dll", 
				"$base_dir\Raven.Database\bin\Release\NetTopologySuite.dll", 
				"$base_dir\Raven.Database\bin\Release\PowerCollections.dll", 
				"$base_dir\Raven.Database\bin\Release\ICSharpCode.NRefactory.???", 
				"$base_dir\Raven.Database\bin\Release\ICSharpCode.NRefactory.CSharp.???", 
				"$base_dir\Raven.Database\bin\Release\Mono.Cecil.???", 
				"$base_dir\Raven.Database\bin\Release\Esent.Interop.???", 
				"$base_dir\Raven.Database\bin\Release\Raven.Database.???", 
				"$base_dir\Raven.Database\bin\Release\AWS.Extensions.???", 
				"$base_dir\Raven.Database\bin\Release\AWSSDK.???" ) 
	
	$web_dlls = ( @( "$base_dir\Raven.Web\bin\Raven.Web.???"  ) + $core_db_dlls)
	
	$web_files = @("..\DefaultConfigs\web.config", "..\DefaultConfigs\NLog.Ignored.config" )
	
	$server_files = ( @( "$base_dir\Raven.Server\bin\Release\Raven.Server.???", "$base_dir\Raven.Server\bin\Release\Raven.Studio.xap", "$base_dir\DefaultConfigs\NLog.Ignored.config") + $core_db_dlls )
		
	$client_dlls = @( (Get-DependencyPackageFiles 'NLog.2'), "$base_dir\Raven.Client.MvcIntegration\bin\Release\Raven.Client.MvcIntegration.???", 
					"$base_dir\Raven.Client.Lightweight\bin\Release\Raven.Abstractions.???", "$base_dir\Raven.Client.Lightweight\bin\Release\Raven.Client.Lightweight.???")
		
	$silverlight_dlls = @("$base_dir\Raven.Client.Silverlight\bin\Release\Raven.Client.Silverlight.???",
	"$base_dir\Raven.Client.Silverlight\bin\Release\AsyncCtpLibrary_Silverlight5.???", 
	"$base_dir\Raven.Client.Silverlight\bin\Release\DH.Scrypt.???", "$base_dir\Raven.Client.Silverlight\bin\Release\Microsoft.CompilerServices.AsyncTargetingPack.Silverlight5.???")
 
	$all_client_dlls = ( @( "$base_dir\Raven.Client.Embedded\bin\Release\Raven.Client.Embedded.???") + $client_dlls + $core_db_dlls )
	  
	$test_prjs = @("$base_dir\Raven.Tests\bin\Release\Raven.Tests.dll",
	"$base_dir\Raven.Bundles.Tests\bin\Release\Raven.Bundles.Tests.dll" )
}

task default -depends Stable,Release

task Verify40 {
	if( (ls "$env:windir\Microsoft.NET\Framework\v4.0*") -eq $null ) {
		throw "Building Raven requires .NET 4.0, which doesn't appear to be installed on this machine"
	}
}


task Clean {
	Remove-Item -force -recurse $buildartifacts_dir -ErrorAction SilentlyContinue
	Remove-Item -force -recurse $release_dir -ErrorAction SilentlyContinue
}

task Init -depends Verify40, Clean {

	if($env:BUILD_NUMBER -ne $null) {
		$env:buildlabel  = $env:BUILD_NUMBER
	}
	if($env:buildlabel -eq $null) {
		$env:buildlabel = "13"
	}
	
	$commit = Get-Git-Commit
	(Get-Content "$base_dir\CommonAssemblyInfo.cs") | 
		Foreach-Object { $_ -replace ".13", ".$($env:buildlabel)" } |
		Foreach-Object { $_ -replace "{commit}", $commit } |
		Set-Content "$base_dir\CommonAssemblyInfo.cs" -Encoding UTF8
	
	New-Item $release_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
	New-Item $buildartifacts_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
}

task Compile -depends Init {
	
	"Dummy file so msbuild knows there is one here before embedding as resource." | Out-File "$base_dir\Raven.Database\Server\WebUI\Raven.Studio.xap"
	
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	# exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Utilities\Raven.ProjectRewriter\Raven.ProjectRewriter.csproj" /p:OutDir="$buildartifacts_dir\" }
	# exec { &"$build_dir\Raven.ProjectRewriter.exe" }
	
	$dat = "$base_dir\..\BuildsInfo\RavenDB\Settings.dat"
	$datDest = "$base_dir\Raven.Studio\Settings.dat"
	echo $dat
	if (Test-Path $dat) {
		Copy-Item $dat $datDest -force
	}
	ElseIf ((Test-Path $datDest) -eq $false) {
		New-Item $datDest -type file -force
	}
	
	Write-Host "Compiling with '$global:configuration' configuration" -ForegroundColor Yellow
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$sln_file" /p:Configuration=$global:configuration /p:nowarn="1591 1573" }
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
	
	Write-Host $test_prjs
	
	$xUnit = Get-PackagePath xunit.runners
	$xUnit = "$xUnit\tools\xunit.console.clr4.exe"
	Write-Host "xUnit location: $xUnit"
	
	$test_prjs | ForEach-Object { 
		if($global:full_storage_test) {
			$env:raventest_storage_engine = 'esent';
			Write-Host "Testing $_ (esent)"
			exec { &"$xUnit" "$_" }
		}
		else {
			$env:raventest_storage_engine = $null;
			Write-Host "Testing $_ (default)"
			exec { &"$xUnit" "$_" }
		}
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
		exec { &"$base_dir\Raven.Performance\bin\Release\Raven.Performance.exe" "--database-location=$RavenDbStableLocation --build-number=$_ --data-location=$DataLocation --logs-location=$LogsLocation" }
	}
}

task TestSilverlight -depends Compile, CopyServer  {
	try
	{
		$process = Start-Process "$buildartifacts_dir\Output\Server\Raven.Server.exe" "--ram --set=Raven/Port==8079" -PassThru
	
		$statLight = Get-PackagePath StatLight
		$statLight = "$statLight\tools\StatLight.exe"
		&$statLight "--XapPath=.\build\sl5\Raven.Tests.Silverlight.xap" "--OverrideTestProvider=MSTestWithCustomProvider" "--ReportOutputFile=.\build\sl5\Raven.Tests.Silverlight.Results.xml" 
	}
	finally
	{
		if ($process -ne $null) {
			Stop-Process -InputObject $process
		}
	}
}

task TestWinRT -depends Compile, CopyServer {
	try
	{
		exec { CheckNetIsolation LoopbackExempt -a -n=68089da0-d0b7-4a09-97f5-30a1e8f9f718_pjnejtz0hgswm }
		
		$process = Start-Process "$buildartifacts_dir\Output\Server\Raven.Server.exe" "--ram --set=Raven/Port==8079" -PassThru
	
		$testRunner = "C:\Program Files (x86)\Microsoft Visual Studio 11.0\Common7\IDE\CommonExtensions\Microsoft\TestWindow\vstest.console.exe"
	
		@("Raven.Tests.WinRT.dll") | ForEach-Object { 
			Write-Host "Testing $_"
			
			if($global:full_storage_test) {
				$env:raventest_storage_engine = 'esent';
				Write-Host "Testing $_ (esent)"
				&"$testRunner" "$_"
			}
			else {
				$env:raventest_storage_engine = $null;
				Write-Host "Testing $_ (default)"
				&"$testRunner" "$_"
			}
		}
	}
	finally
	{
		Stop-Process -InputObject $process
	}
}

task ReleaseNoTests -depends Stable,DoRelease {

}

task Vnext3 {
	$global:uploadCategory = "RavenDB-Unstable"
	$global:uploadMode = "Vnext3"
}

task Unstable {
	$global:uploadCategory = "RavenDB-Unstable"
	$global:uploadMode = "Unstable"
}

task Stable {
	$global:uploadCategory = "RavenDB"
	$global:uploadMode = "Stable"
}

task RunTests -depends Test,TestSilverlight,TestWinRT

task RunAllTests -depends FullStorageTest,RunTests,StressTest

task Release -depends RunTests,DoRelease



task CreateOutpuDirectories -depends CleanOutputDirectory {
	New-Item $buildartifacts_dir\Output -Type directory -ErrorAction SilentlyContinue | Out-Null
	New-Item $buildartifacts_dir\Output\Server -Type directory | Out-Null
	New-Item $buildartifacts_dir\Output\Web -Type directory | Out-Null
	New-Item $buildartifacts_dir\Output\Web\bin -Type directory | Out-Null
	New-Item $buildartifacts_dir\Output\EmbeddedClient -Type directory | Out-Null
	New-Item $buildartifacts_dir\Output\Client -Type directory | Out-Null
	New-Item $buildartifacts_dir\Output\Silverlight -Type directory | Out-Null
	New-Item $buildartifacts_dir\Output\Bundles -Type directory | Out-Null
	New-Item $buildartifacts_dir\Output\Smuggler -Type directory | Out-Null
	New-Item $buildartifacts_dir\Output\Backup -Type directory | Out-Null
}

task CleanOutputDirectory { 
	Remove-Item $buildartifacts_dir\Output -Recurse -Force -ErrorAction SilentlyContinue
}

task CopyEmbeddedClient { 
	$all_client_dlls | ForEach-Object { Copy-Item "$_" $buildartifacts_dir\Output\EmbeddedClient }
}

task CopySilverlight { 
	$silverlight_dlls + @((Get-DependencyPackageFiles 'NLog.2' -FrameworkVersion sl4)) | 
		ForEach-Object { Copy-Item "$_" $buildartifacts_dir\Output\Silverlight }
}

task CopySmuggler {
	Copy-Item $base_dir\Raven.Smuggler\bin\Release\Raven.Abstractions.??? $buildartifacts_dir\Output\Smuggler
	Copy-Item $base_dir\Raven.Smuggler\bin\Release\Raven.Client.Lightweight.??? $buildartifacts_dir\Output\Smuggler
	Copy-Item $base_dir\Raven.Smuggler\bin\Release\Jint.Raven.??? $buildartifacts_dir\Output\Smuggler
	Copy-Item $base_dir\Raven.Smuggler\bin\Release\System.Reactive.Core.??? $buildartifacts_dir\Output\Smuggler
	Copy-Item $base_dir\Raven.Smuggler\bin\Release\Raven.Smuggler.??? $buildartifacts_dir\Output\Smuggler
}

task CopyBackup {
	Copy-Item $base_dir\Raven.Backup\bin\Release\Raven.Abstractions.??? $buildartifacts_dir\Output\Backup
	Copy-Item $base_dir\Raven.Backup\bin\Release\Raven.Backup.??? $buildartifacts_dir\Output\Backup
}

task CopyClient {
	$client_dlls | ForEach-Object { Copy-Item "$_" $buildartifacts_dir\Output\Client }
}

task CopyWeb {
	$web_dlls | ForEach-Object { Copy-Item "$_" $buildartifacts_dir\Output\Web\bin }
	$web_files | ForEach-Object { Copy-Item "$_" $buildartifacts_dir\Output\Web }
}

task CopyBundles {
	Copy-Item $base_dir\Bundles\Raven.Bundles.Authorization\bin\Release\Raven.Bundles.Authorization.??? $buildartifacts_dir\Output\Bundles
	Copy-Item $base_dir\Bundles\Raven.Bundles.CascadeDelete\bin\Release\Raven.Bundles.CascadeDelete.??? $buildartifacts_dir\Output\Bundles
	Copy-Item $base_dir\Bundles\Raven.Bundles.Encryption.IndexFileCodec\bin\Release\Raven.Bundles.Encryption.IndexFileCodec.??? $buildartifacts_dir\Output\Bundles
	Copy-Item $base_dir\Bundles\Raven.Bundles.IndexReplication\bin\Release\Raven.Bundles.IndexReplication.??? $buildartifacts_dir\Output\Bundles
	Copy-Item $base_dir\Bundles\Raven.Bundles.UniqueConstraints\bin\Release\Raven.Bundles.UniqueConstraints.??? $buildartifacts_dir\Output\Bundles
	Copy-Item $base_dir\Bundles\Raven.Client.Authorization\bin\Release\Raven.Client.Authorization.??? $buildartifacts_dir\Output\Bundles
	Copy-Item $base_dir\Bundles\Raven.Client.UniqueConstraints\bin\Release\Raven.Client.UniqueConstraints.??? $buildartifacts_dir\Output\Bundles
}

task CopyServer -depends CreateOutpuDirectories {
	$server_files | ForEach-Object { Copy-Item "$_" $buildartifacts_dir\Output\Server }
	Copy-Item $base_dir\DefaultConfigs\RavenDb.exe.config $buildartifacts_dir\Output\Server\Raven.Server.exe.config
}

task CopyInstaller {
	if($env:buildlabel -eq 13)
	{
	  return
	}

	Copy-Item $buildartifacts_dir\RavenDB.Setup.exe "$release_dir\$global:uploadCategory-Build-$env:buildlabel.Setup.exe"
}

task SignInstaller {
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
  
  $installerFile = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.Setup.exe"
    
  Exec { &$signTool sign /f "$installerCert" /p "$certPassword" /d "RavenDB" /du "http://ravendb.net" /t "http://timestamp.verisign.com/scripts/timstamp.dll" "$installerFile" }
} 

task CreateDocs {
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	if($env:buildlabel -eq 13)
	{
	  return 
	}
	 
	# we expliclty allows this to fail
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Raven.Docs.shfbproj" /p:OutDir="$buildartifacts_dir\" }
}

task CopyRootFiles -depends CreateDocs {
	cp $base_dir\license.txt $buildartifacts_dir\Output\license.txt
	cp $base_dir\Scripts\Start.cmd $buildartifacts_dir\Output\Start.cmd
	cp $base_dir\Scripts\Raven-UpdateBundles.ps1 $buildartifacts_dir\Output\Raven-UpdateBundles.ps1
	cp $base_dir\Scripts\Raven-GetBundles.ps1 $buildartifacts_dir\Output\Raven-GetBundles.ps1
	cp $base_dir\readme.md $buildartifacts_dir\Output\readme.txt
	cp $base_dir\Help\Documentation.chm $buildartifacts_dir\Output\Documentation.chm  -ErrorAction SilentlyContinue
	cp $base_dir\acknowledgments.txt $buildartifacts_dir\Output\acknowledgments.txt
	cp $base_dir\CommonAssemblyInfo.cs $buildartifacts_dir\Output\CommonAssemblyInfo.cs
}

task ZipOutput {
	
	if($env:buildlabel -eq 13)
	{
		return 
	}

	$old = pwd
	cd $buildartifacts_dir\Output
	
	$file = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
		
	exec { 
		& $tools_dir\zip.exe -9 -A -r `
			$file `
			EmbeddedClient\*.* `
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

task ResetBuildArtifcats {
	git checkout "Raven.Database\RavenDB.snk"
}

task DoRelease -depends Compile, `
	CleanOutputDirectory, `
	CreateOutpuDirectories, `
	CopyEmbeddedClient, `
	CopySmuggler, `
	CopyBackup, `
	CopyClient, `
	CopySilverlight, `
	CopyWeb, `
	CopyBundles, `
	CopyServer, `
	CopyRootFiles, `
	ZipOutput, `
	CopyInstaller, `
	SignInstaller, `
	CreateNugetPackages, `
	PublishSymbolSources, `
	ResetBuildArtifcats {	
	Write-Host "Done building RavenDB"
}


task Upload -depends DoRelease {
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
			while ($uploadTryCount -lt 5){
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

task UploadStable -depends Stable, DoRelease, Upload	

task UploadUnstable -depends Unstable, DoRelease, Upload

task UploadVnext3 -depends Vnext3, DoRelease, Upload

task CreateNugetPackages -depends Compile {

	Remove-Item $base_dir\RavenDB*.nupkg
	
	$nuget_dir = "$buildartifacts_dir\NuGet"
	Remove-Item $nuget_dir -Force -Recurse -ErrorAction SilentlyContinue
	New-Item $nuget_dir -Type directory | Out-Null
	
	New-Item $nuget_dir\RavenDB.Client\lib\net45 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Client\lib\sl50 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Client.nuspec $nuget_dir\RavenDB.Client\RavenDB.Client.nuspec
	
	@("Raven.Abstractions.???", "Raven.Client.Lightweight.???") |% { Copy-Item "$base_dir\Raven.Client.Lightweight\bin\Release\$_" $nuget_dir\RavenDB.Client\lib\net45 }
	@("Raven.Client.Silverlight.???", "AsyncCtpLibrary_Silverlight5.???") |% { Copy-Item "$base_dir\Raven.Client.Silverlight\bin\Release\$_" $nuget_dir\RavenDB.Client\lib\sl50	}
	
	New-Item $nuget_dir\RavenDB.Client.MvcIntegration\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Client.MvcIntegration.nuspec $nuget_dir\RavenDB.Client.MvcIntegration\RavenDB.Client.MvcIntegration.nuspec
	@("Raven.Client.MvcIntegration.???") |% { Copy-Item "$base_dir\Raven.Client.MvcIntegration\bin\Release\$_" $nuget_dir\RavenDB.Client.MvcIntegration\lib\net45 }
		
	New-Item $nuget_dir\RavenDB.Database\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Database.nuspec $nuget_dir\RavenDB.Database\RavenDB.Database.nuspec
	@("Raven.Abstractions.???", "Raven.Database.???", "BouncyCastle.Crypto.???",
		 "Esent.Interop.???", "ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Lucene.Net.???", "Lucene.Net.Contrib.Spatial.NTS.???",
		 "Jint.Raven.???", "Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll", "AWS.Extensions.???", "AWSSDK.???") `
		 |% { Copy-Item "$base_dir\Raven.Database\bin\Release\$_" $nuget_dir\RavenDB.Database\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Server -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Server.nuspec $nuget_dir\RavenDB.Server\RavenDB.Server.nuspec
	New-Item $nuget_dir\RavenDB.Server\tools -Type directory | Out-Null
	@("Esent.Interop.???", "ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Lucene.Net.???", "Lucene.Net.Contrib.Spatial.NTS.???",
		"Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll",	"NewtonSoft.Json.???", "NLog.???", "Jint.Raven.???",
		"Raven.Abstractions.???", "Raven.Database.???", "Raven.Server.???", "Raven.Smuggler.???", "AWS.Extensions.???", "AWSSDK.???") |% { Copy-Item "$base_dir\Raven.Server\bin\Release\$_" $nuget_dir\RavenDB.Server\tools }
	Copy-Item $base_dir\DefaultConfigs\RavenDb.exe.config $nuget_dir\RavenDB.Server\tools\Raven.Server.exe.config

	New-Item $nuget_dir\RavenDB.Embedded\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Embedded.nuspec $nuget_dir\RavenDB.Embedded\RavenDB.Embedded.nuspec
	@("Raven.Client.Embedded.???") |% { Copy-Item "$base_dir\Raven.Client.Embedded\bin\Release\net45\$_" $nuget_dir\RavenDB.Embedded\lib\net45 }
	
	# Client packages
	@("Authorization", "UniqueConstraints") | Foreach-Object { 
		$name = $_;
		New-Item $nuget_dir\RavenDB.Client.$name\lib\net45 -Type directory | Out-Null
		Copy-Item $base_dir\NuGet\RavenDB.Client.$name.nuspec $nuget_dir\RavenDB.Client.$name\RavenDB.Client.$name.nuspec
		@("$base_dir\Bundles\Raven.Client.$_\bin\Release\net45\Raven.Client.$_.???") |% { Copy-Item $_ $nuget_dir\RavenDB.Client.$name\lib\net45 }
	}
	
	New-Item $nuget_dir\RavenDB.Bundles.Authorization\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.Authorization.nuspec $nuget_dir\RavenDB.Bundles.Authorization\RavenDB.Bundles.Authorization.nuspec
	@("Raven.Bundles.Authorization.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.Authorization.$_\bin\Release\$_" $nuget_dir\RavenDB.Bundles.Authorization\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.CascadeDelete.nuspec $nuget_dir\RavenDB.Bundles.CascadeDelete\RavenDB.Bundles.CascadeDelete.nuspec
	@("Raven.Bundles.CascadeDelete.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.CascadeDelete.$_\bin\Release\$_" $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Bundles.IndexReplication\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.IndexReplication.nuspec $nuget_dir\RavenDB.Bundles.IndexReplication\RavenDB.Bundles.IndexReplication.nuspec
	@("Raven.Bundles.IndexReplication.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.IndexReplication.$_\bin\Release\$_" $nuget_dir\RavenDB.Bundles.IndexReplication\lib\net45 }

	New-Item $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.UniqueConstraints.nuspec $nuget_dir\RavenDB.Bundles.UniqueConstraints\RavenDB.Bundles.UniqueConstraints.nuspec
	@("Raven.Bundles.UniqueConstraints.???") |% { Copy-Item "$base_dir\Bundles\Raven.Bundles.UniqueConstraints.$_\bin\Release\$_" $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Tests.Helpers\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Tests.Helpers.nuspec $nuget_dir\RavenDB.Tests.Helpers\RavenDB.Tests.Helpers.nuspec
	@("Raven.Tests.Helpers.???", "Raven.Server.???") |% { Copy-Item "$base_dir\Raven.Tests.Helpers.$_\bin\Release\$_" $nuget_dir\RavenDB.Tests.Helpers\lib\net45 }
	New-Item $nuget_dir\RavenDB.Tests.Helpers\content -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenTests $nuget_dir\RavenDB.Tests.Helpers\content\RavenTests -Recurse
	
	$global:nugetVersion = "$version.$env:buildlabel"
	if ($global:uploadCategory -and $global:uploadCategory.EndsWith("-Unstable")){
		$global:nugetVersion += "-Unstable"
	}
	
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
		
		# Push to nuget repository
		$packages | ForEach-Object {
			Exec { &"$base_dir\.nuget\NuGet.exe" push "$($_.BaseName).$global:nugetVersion.nupkg" $accessKey -Source $sourceFeed }
		}
		
	}
	else {
		Write-Host "$accessPath does not exit. Cannot publish the nuget package." -ForegroundColor Yellow
	}
}

task PublishSymbolSources -depends CreateNugetPackages {

	$nuget_dir = "$buildartifacts_dir\NuGet"
	
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
		$srcDirName1 = $srcDirName1.Replace("Raven.Embedded", "Raven.Client.Embedded")
		
		$srcDirNames = @($srcDirName1)
		if ($dirName -eq "RavenDB.Client") {
			$srcDirNames += @("Raven.Client.Silverlight")
		}
		elseif ($dirName -eq "RavenDB.Server") {
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
						$copyTo = "$nuget_dir\$dirName\src\$copyTo"
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
		
		Get-ChildItem "$nuget_dir\$dirName\*.dll" -recurse -exclude Raven* | ForEach-Object {
			Remove-Item $_ -force -recurse -ErrorAction SilentlyContinue
		}
		Get-ChildItem "$nuget_dir\$dirName\*.pdb" -recurse -exclude Raven* | ForEach-Object {
			Remove-Item $_ -force -recurse -ErrorAction SilentlyContinue
		}
		Get-ChildItem "$nuget_dir\$dirName\*.xml" -recurse | ForEach-Object {
			Remove-Item $_ -force -recurse -ErrorAction SilentlyContinue
		}
		
		Remove-Item "$nuget_dir\$dirName\src\bin" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$nuget_dir\$dirName\src\obj" -force -recurse -ErrorAction SilentlyContinue
		
		Exec { &"$base_dir\.nuget\nuget.exe" pack $_.FullName -Symbols }
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

TaskTearDown {
	
	if ($LastExitCode -ne 0) {
		write-host "TaskTearDown detected an error. Build failed." -BackgroundColor Red -ForegroundColor Yellow
		write-host "Yes, something was failed!!!!!!!!!!!!!!!!!!!!!" -BackgroundColor Red -ForegroundColor Yellow
		# throw "TaskTearDown detected an error. Build failed."
		exit 1
	}
}
