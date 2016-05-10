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
	$build_dir = "$base_dir\build"
	$packages_dir = "$base_dir\packages"
	$buildartifacts_dir = "$build_dir\"
	$sln_file = "$base_dir\zzz_RavenDB_Release.sln"
	$version = "2.5"
	$tools_dir = "$base_dir\Tools"
	$release_dir = "$base_dir\Release"
	$uploader = "..\Uploader\S3Uploader.exe"
	$global:configuration = "Release"
	
	$core_db_dlls = @(
        "Raven.Abstractions.???", 
        (Get-DependencyPackageFiles 'NLog.2'), 
        (Get-DependencyPackageFiles Microsoft.Web.Infrastructure), 
        "Jint.Raven.???",
				"Lucene.Net.???",
				"Microsoft.Data.Edm.???",
				"Microsoft.WindowsAzure.Storage.???",
				"Microsoft.Data.OData.???",
				"Microsoft.WindowsAzure.ConfigurationManager.???",
				"Lucene.Net.Contrib.Spatial.NTS.???", "Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll", 
				"ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", 
				"Esent.Interop.???", 
				"Raven.Database.???", 
				"AWS.Extensions.???", "AWSSDK.???" ,
				"Microsoft.CompilerServices.AsyncTargetingPack.Net4.???" ) 
	
	$web_dlls = ( @( "Raven.Web.???"  ) + $core_db_dlls) |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\web\$_"
		}
	
	$web_files = @("..\DefaultConfigs\web.config", "..\DefaultConfigs\NLog.Ignored.config" )
	
	$server_files = ( @( "Raven.Server.???", "sl5\Raven.Studio.xap", "..\DefaultConfigs\NLog.Ignored.config") + $core_db_dlls ) |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\$_"
		}
		
	$client_dlls = @( (Get-DependencyPackageFiles 'NLog.2'), "Raven.Client.MvcIntegration.???", 
					"Raven.Abstractions.???", "Raven.Client.Lightweight.???", "Microsoft.CompilerServices.AsyncTargetingPack.Net4.???") |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\$_"
		}
		
	$silverlight_dlls = @("Raven.Client.Silverlight.???", "AsyncCtpLibrary_Silverlight5.???", "DH.Scrypt.???", "Microsoft.CompilerServices.AsyncTargetingPack.Silverlight5.???") |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\sl5\$_"
		}
 
	$all_client_dlls = ( @( "Raven.Client.Embedded.???") + $client_dlls + $core_db_dlls ) |
		ForEach-Object { 
			if ([System.IO.Path]::IsPathRooted($_)) { return $_ }
			return "$build_dir\$_"
		}
	  
		$test_prjs = @("Raven.Tests.dll","Raven.Bundles.Tests.dll" )
}

task default -depends Test, DoReleasePart1

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

	$commit = Get-Git-Commit
	(Get-Content "$base_dir\CommonAssemblyInfo.cs") | 
		Foreach-Object { $_ -replace ".13", ".$($env:buildlabel)" } |
		Foreach-Object { $_ -replace "{commit}", $commit } |
		Set-Content "$base_dir\CommonAssemblyInfo.cs" -Encoding UTF8
	
	New-Item $release_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
	New-Item $build_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
}

task Compile -depends Init {
	
	"Dummy file so msbuild knows there is one here before embedding as resource." | Out-File "$base_dir\Raven.Database\Server\WebUI\Raven.Studio.xap"
	
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Utilities\Raven.ProjectRewriter\Raven.ProjectRewriter.csproj" /p:OutDir="$buildartifacts_dir\" }
	exec { &"$build_dir\Raven.ProjectRewriter.exe" }
	
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
	remove-item "$build_dir\nlog.config" -force  -ErrorAction SilentlyContinue
}

task FullStorageTest {
	$global:full_storage_test = $true
}

task Test -depends Compile {
	Clear-Host
	
	Write-Host $test_prjs
	
	$xUnit = "$lib_dir\xunit\xunit.console.clr4.exe"
	Write-Host "xUnit location: $xUnit"
	
	$test_prjs | ForEach-Object { 
		if($global:full_storage_test) {
			$env:raventest_storage_engine = 'esent';
			Write-Host "Testing $build_dir\$_ (esent)"
			exec { &"$xUnit" "$build_dir\$_" }
		}
		else {
			$env:raventest_storage_engine = $null;
			Write-Host "Testing $build_dir\$_ (default)"
			exec { &"$xUnit" "$build_dir\$_" }
		}
	}
}

task StressTest -depends Compile {
	
	$xUnit = Get-PackagePath xunit.runners
	$xUnit = "$xUnit\tools\xunit.console.clr4.exe"
	
	@("Raven.StressTests.dll") | ForEach-Object { 
		Write-Host "Testing $build_dir\$_"
		
		if($global:full_storage_test) {
			$env:raventest_storage_engine = 'esent';
			Write-Host "Testing $build_dir\$_ (esent)"
			&"$xUnit" "$build_dir\$_"
		}
		else {
			$env:raventest_storage_engine = $null;
			Write-Host "Testing $build_dir\$_ (default)"
			&"$xUnit" "$build_dir\$_"
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
		exec { &"$build_dir\Raven.Performance.exe" "--database-location=$RavenDbStableLocation --build-number=$_ --data-location=$DataLocation --logs-location=$LogsLocation" }
	}
}

task TestSilverlight -depends Compile, CopyServer  {
	try
	{
		$process = Start-Process "$build_dir\Output\Server\Raven.Server.exe" "--ram --set=Raven/Port==8079" -PassThru
	
		$statLight = Get-PackagePath StatLight
		$statLight = "$statLight\tools\StatLight.exe"
		&$statLight "--XapPath=.\build\sl5\Raven.Tests.Silverlight.xap" "--OverrideTestProvider=MSTestWithCustomProvider" "--ReportOutputFile=.\build\Raven.Tests.Silverlight.Results.xml" 
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
		$process = Start-Process "$build_dir\Output\Server\Raven.Server.exe" "--ram --set=Raven/Port==8079" -PassThru
	
		$xUnit = Get-PackagePath xunit.runners
		$xUnit = "$xUnit\tools\xunit.console.clr4.exe"
	
		@("Raven.Tests.WinRT.dll") | ForEach-Object { 
			Write-Host "Testing $build_dir\$_"
			
			if($global:full_storage_test) {
				$env:raventest_storage_engine = 'esent';
				Write-Host "Testing $build_dir\$_ (esent)"
				&"$xUnit" "$build_dir\$_"
			}
			else {
				$env:raventest_storage_engine = $null;
				Write-Host "Testing $build_dir\$_ (default)"
				&"$xUnit" "$build_dir\$_"
			}
		}
	}
	finally
	{
		Stop-Process -InputObject $process
	}
}

task Vnext3 {
	$global:uploadCategory = "RavenDB-Unstable"
	$global:uploadMode = "Vnext3"
}

task Unstable {
	$global:uploadCategory = "RavenDB-Unstable"
	$global:uploadMode = "Unstable"
}

task Hotfix {
    $global:uploadCategory = "RavenDB-Hotfix"
    $global:uploadMode = "Unstable"
    $global:configuration = "Release"
}

task Stable {
	$global:uploadCategory = "RavenDB"
	$global:uploadMode = "Stable"
}

task RunTests -depends Test,TestSilverlight

task RunAllTests -depends FullStorageTest,Test,TestSilverlight,StressTest

task CopySamples {
	Remove-Item "$build_dir\Output\Samples\" -recurse -force -ErrorAction SilentlyContinue 

	Copy-Item "$base_dir\.nuget\" "$build_dir\Output\Samples\.nuget" -recurse -force
	Copy-Item "$base_dir\CommonAssemblyInfo.cs" "$build_dir\Output\Samples\CommonAssemblyInfo.cs" -force
	Copy-Item "$base_dir\Raven.Samples.sln" "$build_dir\Output\Samples" -force
	Copy-Item $base_dir\Raven.VisualHost "$build_dir\Output\Samples\Raven.VisualHost" -recurse -force
	
	$samples =  Get-ChildItem $base_dir\Samples | Where-Object { $_.PsIsContainer }
	$samples = $samples
	foreach ($sample in $samples) {
		Write-Output $sample
		Copy-Item "$base_dir\Samples\$sample" "$build_dir\Output\Samples\$sample" -recurse -force
		
		Remove-Item "$sample_dir\bin" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\obj" -force -recurse -ErrorAction SilentlyContinue

		Remove-Item "$sample_dir\Servers\Shard1\Data" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard2\Data" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard1\Plugins" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard2\Plugins" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard1\RavenDB.exe" -force -recurse -ErrorAction SilentlyContinue
		Remove-Item "$sample_dir\Servers\Shard2\RavenDB.exe" -force -recurse -ErrorAction SilentlyContinue 
	}
	
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Utilities\Raven.Samples.PrepareForRelease\Raven.Samples.PrepareForRelease.csproj" /p:OutDir="$buildartifacts_dir\" }
	exec { &"$build_dir\Raven.Samples.PrepareForRelease.exe" "$build_dir\Output\Samples\Raven.Samples.sln" "$build_dir\Output" }
}

task CreateOutpuDirectories -depends CleanOutputDirectory {
	New-Item $build_dir\Output -Type directory -ErrorAction SilentlyContinue | Out-Null
	New-Item $build_dir\Output\Server -Type directory | Out-Null
	New-Item $build_dir\Output\Web -Type directory | Out-Null
	New-Item $build_dir\Output\Web\bin -Type directory | Out-Null
	New-Item $build_dir\Output\EmbeddedClient -Type directory | Out-Null
	New-Item $build_dir\Output\Client -Type directory | Out-Null
	New-Item $build_dir\Output\Silverlight -Type directory | Out-Null
	New-Item $build_dir\Output\Bundles -Type directory | Out-Null
	New-Item $build_dir\Output\Samples -Type directory | Out-Null
	New-Item $build_dir\Output\Smuggler -Type directory | Out-Null
	New-Item $build_dir\Output\Backup -Type directory | Out-Null
}

task CleanOutputDirectory { 
	Remove-Item $build_dir\Output -Recurse -Force -ErrorAction SilentlyContinue
}

task CopyEmbeddedClient { 
	$all_client_dlls | ForEach-Object { Copy-Item "$_" $build_dir\Output\EmbeddedClient }
}

task CopySilverlight { 
	$silverlight_dlls + @((Get-DependencyPackageFiles 'NLog.2' -FrameworkVersion sl4)) | 
		ForEach-Object { Copy-Item "$_" $build_dir\Output\Silverlight }
}

task CopySmuggler {
	Copy-Item $build_dir\Raven.Abstractions.??? $build_dir\Output\Smuggler
	Copy-Item $build_dir\Raven.Client.Lightweight.??? $build_dir\Output\Smuggler
	Copy-Item $build_dir\Jint.Raven.??? $build_dir\Output\Smuggler
	Copy-Item $build_dir\System.Reactive.Core.??? $build_dir\Output\Smuggler
	Copy-Item $build_dir\Microsoft.CompilerServices.AsyncTargetingPack.Net4.??? $build_dir\Output\Smuggler
	Copy-Item $build_dir\Raven.Smuggler.??? $build_dir\Output\Smuggler
}

task CopyBackup {
	Copy-Item $build_dir\Raven.Abstractions.??? $build_dir\Output\Backup
	Copy-Item $build_dir\Raven.Backup.??? $build_dir\Output\Backup
	Copy-Item $build_dir\Raven.Client.Lightweight.??? $build_dir\Output\Backup
}

task CopyClient {
	$client_dlls | ForEach-Object { Copy-Item "$_" $build_dir\Output\Client }
}

task CopyWeb {
	$web_dlls | ForEach-Object { Copy-Item "$_" $build_dir\Output\Web\bin }
	$web_files | ForEach-Object { Copy-Item "$build_dir\$_" $build_dir\Output\Web }
}

task CopyBundles {
	$items = (Get-ChildItem $build_dir\Raven.Bundles.*.???) + (Get-ChildItem $build_dir\Raven.Client.*.???) | 
				Where-Object { $_.Name.Contains(".Tests.") -eq $false } | ForEach-Object { $_.FullName }
	Copy-Item $items $build_dir\Output\Bundles
}

task CopyServer -depends CreateOutpuDirectories {
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

	Copy-Item $build_dir\RavenDB.Setup.exe "$release_dir\$global:uploadCategory-Build-$env:buildlabel.Setup.exe"
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
			EmbeddedClient\*.* `
			Client\*.* `
			Samples\*.* `
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
	SignServer, `
	CopyRootFiles, `
	CopySamples, `
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
        $global:nugetVersion += "-" + $global:uploadCategory
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
	
	New-Item $nuget_dir\RavenDB.Client\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Client\lib\net45 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Client\lib\sl50 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Client.nuspec $nuget_dir\RavenDB.Client\RavenDB.Client.nuspec
	
	@("Raven.Abstractions.???", "Raven.Client.Lightweight.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Client\lib\net40 }
	@("Raven.Abstractions.???", "Raven.Client.Lightweight.???") |% { Copy-Item "$build_dir\net45\$_" $nuget_dir\RavenDB.Client\lib\net45 }
	@("Raven.Client.Silverlight.???", "AsyncCtpLibrary_Silverlight5.???") |% { Copy-Item "$build_dir\sl5\$_" $nuget_dir\RavenDB.Client\lib\sl50	}
	
	New-Item $nuget_dir\RavenDB.Client.MvcIntegration\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Client.MvcIntegration\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Client.MvcIntegration.nuspec $nuget_dir\RavenDB.Client.MvcIntegration\RavenDB.Client.MvcIntegration.nuspec
	@("Raven.Client.MvcIntegration.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Client.MvcIntegration\lib\net40 }
	@("Raven.Client.MvcIntegration.???") |% { Copy-Item "$build_dir\net45\$_" $nuget_dir\RavenDB.Client.MvcIntegration\lib\net45 }
		
	New-Item $nuget_dir\RavenDB.Database\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Database\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Database.nuspec $nuget_dir\RavenDB.Database\RavenDB.Database.nuspec
	@("Raven.Abstractions.???", "Raven.Database.???", "BouncyCastle.Crypto.???",
		 "Esent.Interop.???", "ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Lucene.Net.???", "Lucene.Net.Contrib.Spatial.NTS.???",
		 "Jint.Raven.???", "Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll", "AWS.Extensions.???", "AWSSDK.???") `
		 |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Database\lib\net40 }
	@("Raven.Abstractions.???", "Raven.Database.???", "BouncyCastle.Crypto.???",
		 "Esent.Interop.???", "ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Lucene.Net.???", "Lucene.Net.Contrib.Spatial.NTS.???",
		 "Jint.Raven.???", "Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll", "AWS.Extensions.???", "AWSSDK.???") `
		 |% { Copy-Item "$build_dir\net45\$_" $nuget_dir\RavenDB.Database\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Server -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Server.nuspec $nuget_dir\RavenDB.Server\RavenDB.Server.nuspec
	New-Item $nuget_dir\RavenDB.Server\tools -Type directory | Out-Null
	@("Esent.Interop.???", "ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Lucene.Net.???", "Lucene.Net.Contrib.Spatial.NTS.???",
		"Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll",	"NewtonSoft.Json.???", "NLog.???", "Jint.Raven.???",
		"Raven.Abstractions.???", "Raven.Database.???", "Raven.Server.???", "Raven.Smuggler.???", "Raven.Client.Lightweight.???", "AWS.Extensions.???", "AWSSDK.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Server\tools }
	Copy-Item (Get-DependencyPackageFiles 'Microsoft.CompilerServices.AsyncTargetingPack' -FrameworkVersion net40) $nuget_dir\RavenDB.Server\tools
	Copy-Item $base_dir\DefaultConfigs\RavenDb.exe.config $nuget_dir\RavenDB.Server\tools\Raven.Server.exe.config

	New-Item $nuget_dir\RavenDB.Embedded\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Embedded\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Embedded.nuspec $nuget_dir\RavenDB.Embedded\RavenDB.Embedded.nuspec
	@("Raven.Client.Embedded.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Embedded\lib\net40 }
	@("Raven.Client.Embedded.???") |% { Copy-Item "$build_dir\net45\$_" $nuget_dir\RavenDB.Embedded\lib\net45 }
	
	# Client packages
	@("Authorization", "UniqueConstraints") | Foreach-Object { 
		$name = $_;
		New-Item $nuget_dir\RavenDB.Client.$name\lib\net40 -Type directory | Out-Null
		New-Item $nuget_dir\RavenDB.Client.$name\lib\net45 -Type directory | Out-Null
		Copy-Item $base_dir\NuGet\RavenDB.Client.$name.nuspec $nuget_dir\RavenDB.Client.$name\RavenDB.Client.$name.nuspec
		@("Raven.Client.$_.???") |% { Copy-Item $build_dir\$_ $nuget_dir\RavenDB.Client.$name\lib\net40 }
		@("Raven.Client.$_.???") |% { Copy-Item $build_dir\net45\$_ $nuget_dir\RavenDB.Client.$name\lib\net45 }
	}
	
	New-Item $nuget_dir\RavenDB.Bundles.Authorization\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Bundles.Authorization\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.Authorization.nuspec $nuget_dir\RavenDB.Bundles.Authorization\RavenDB.Bundles.Authorization.nuspec
	@("Raven.Bundles.Authorization.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Bundles.Authorization\lib\net40 }
	@("Raven.Bundles.Authorization.???") |% { Copy-Item "$build_dir\net45\$_" $nuget_dir\RavenDB.Bundles.Authorization\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.CascadeDelete.nuspec $nuget_dir\RavenDB.Bundles.CascadeDelete\RavenDB.Bundles.CascadeDelete.nuspec
	@("Raven.Bundles.CascadeDelete.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net40 }
	@("Raven.Bundles.CascadeDelete.???") |% { Copy-Item "$build_dir\net45\$_" $nuget_dir\RavenDB.Bundles.CascadeDelete\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.Bundles.IndexReplication\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Bundles.IndexReplication\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.IndexReplication.nuspec $nuget_dir\RavenDB.Bundles.IndexReplication\RavenDB.Bundles.IndexReplication.nuspec
	@("Raven.Bundles.IndexReplication.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Bundles.IndexReplication\lib\net40 }
	@("Raven.Bundles.IndexReplication.???") |% { Copy-Item "$build_dir\net45\$_" $nuget_dir\RavenDB.Bundles.IndexReplication\lib\net45 }

	New-Item $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Bundles.UniqueConstraints.nuspec $nuget_dir\RavenDB.Bundles.UniqueConstraints\RavenDB.Bundles.UniqueConstraints.nuspec
	@("Raven.Bundles.UniqueConstraints.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net40 }
	@("Raven.Bundles.UniqueConstraints.???") |% { Copy-Item "$build_dir\net45\$_" $nuget_dir\RavenDB.Bundles.UniqueConstraints\lib\net45 }
	
	New-Item $nuget_dir\RavenDB.AspNetHost\content -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.AspNetHost\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.AspNetHost\lib\net45 -Type directory | Out-Null
	@("Raven.Web.???") |% { Copy-Item "$build_dir\web\$_" $nuget_dir\RavenDB.AspNetHost\lib\net40 }
	@("Raven.Web.???") |% { Copy-Item "$build_dir\web\net45\$_" $nuget_dir\RavenDB.AspNetHost\lib\net45 }
	Copy-Item $base_dir\NuGet\RavenDB.AspNetHost.nuspec $nuget_dir\RavenDB.AspNetHost\RavenDB.AspNetHost.nuspec
	Copy-Item $base_dir\DefaultConfigs\NuGet.AspNetHost.Web.config $nuget_dir\RavenDB.AspNetHost\content\Web.config.transform
	
	New-Item $nuget_dir\RavenDB.Tests.Helpers\lib\net40 -Type directory | Out-Null
	New-Item $nuget_dir\RavenDB.Tests.Helpers\lib\net45 -Type directory | Out-Null
	Copy-Item $base_dir\NuGet\RavenDB.Tests.Helpers.nuspec $nuget_dir\RavenDB.Tests.Helpers\RavenDB.Tests.Helpers.nuspec
	@("Raven.Tests.Helpers.???", "Raven.Server.???") |% { Copy-Item "$build_dir\$_" $nuget_dir\RavenDB.Tests.Helpers\lib\net40 }
	@("Raven.Tests.Helpers.???", "Raven.Server.???") |% { Copy-Item "$build_dir\net45\$_" $nuget_dir\RavenDB.Tests.Helpers\lib\net45 }
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
		return; # this brake the build

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
	
	
		return; # this takes 20 minutes to run
	

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
