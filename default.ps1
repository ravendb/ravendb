properties {
  $base_dir  = resolve-path .
  $lib_dir = "$base_dir\SharedLibs"
  $build_dir = "$base_dir\build"
  $buildartifacts_dir = "$build_dir\"
  $sln_file = "$base_dir\zzz_RavenDB_Release.sln"
  $version = "1.0.0.0"
  $tools_dir = "$base_dir\Tools"
  $release_dir = "$base_dir\Release"
  $uploader = "..\Uploader\S3Uploader.exe"
  $gemPusher = "..\Uploader\push_gem.ps1"
}

include .\psake_ext.ps1

task default -depends OpenSource,Release


task Verify40 {
	if( (ls "$env:windir\Microsoft.NET\Framework\v4.0*") -eq $null ) {
		throw "Building Raven requires .NET 4.0, which doesn't appear to be installed on this machine"
	}
}

task Clean {
  remove-item -force -recurse $buildartifacts_dir -ErrorAction SilentlyContinue
  remove-item -force -recurse $release_dir -ErrorAction SilentlyContinue
}

task Init -depends Verify40, Clean {
	
	if($env:buildlabel -eq $null) {
		$env:buildlabel = "13"
	}
	
	$asmInfos = ls -path $base_dir -include AssemblyInfo.cs -recurse | 
					Where { $_ -notmatch [regex]::Escape($lib_dir) } | 
					Where { $_ -notmatch [regex]::Escape($tools_dir) }
	
	foreach($asmInfo in $asmInfos) {
		
		$propertiesDir = [System.IO.Path]::GetDirectoryName($asmInfo.FullName)
		$projectDir = [System.IO.Path]::GetDirectoryName($propertiesDir)
		$projectName = [System.IO.Path]::GetFileName($projectDir)
		
		Generate-Assembly-Info `
			-file $asmInfo.FullName `
			-title "$projectName $version" `
			-description "A linq enabled document database for .NET" `
			-company "Hibernating Rhinos" `
			-product "RavenDB $version" `
			-version $version `
			-copyright "Copyright © Hibernating Rhinos and Ayende Rahien 2004 - 2010" `
			-clsCompliant "true"
	}
		
	new-item $release_dir -itemType directory
	new-item $buildartifacts_dir -itemType directory
	
	copy $tools_dir\xUnit\*.* $build_dir
	 
	if($global:commercial) {
		exec { .\Utilities\Binaries\Raven.ProjectRewriter.exe }
		cp "..\RavenDB_Commercial.snk" "Raven.Database\RavenDB.snk"
	}
	else {
		cp "Raven.Database\Raven.Database.csproj" "Raven.Database\Raven.Database.g.csproj"
	}
}

task Compile -depends Init {
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
    exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$sln_file" /p:OutDir="$buildartifacts_dir\" }
    
    Write-Host "Merging..."
    $old = pwd
    cd $build_dir
     
    exec { ..\Utilities\Binaries\Raven.Merger.exe }
    
    cd $old
    
    Write-Host "Finished merging"
    
    exec { & "C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Bundles\Raven.Bundles.sln" /p:OutDir="$buildartifacts_dir\" }
    exec { & "C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Samples\Raven.Samples.sln" /p:OutDir="$buildartifacts_dir\" }
}

task Test -depends Compile {
  $old = pwd
  cd $build_dir
  exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\Raven.Tests.dll" } 
  exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\Raven.Scenarios.dll" }
  exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\Raven.Client.Tests.dll" }
  exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\Raven.Bundles.Tests.dll" }
  cd $old
}

task ReleaseNoTests -depends OpenSource,DoRelease {

}

task Commercial {
	$global:commercial = $true
	$global:uploadCategory = "RavenDB-Commercial"
}

task Unstable {
	$global:commercial = $false
	$global:uploadCategory = "RavenDB-Unstable"
}

task OpenSource {
	$global:commercial = $false
	$global:uploadCategory = "RavenDB"
}

task Release -depends Test,DoRelease { 
}

task CopySamples {
  $samples = @("MvcMusicStore", "Raven.Sample.ShardClient", "Raven.Sample.Failover", "Raven.Sample.Replication", "Raven.Sample.EventSourcing", "Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator")
	$exclude = @("bin", "obj", "Data", "Plugins")
	
	foreach ($sample in $samples) {
      echo $sample 
      
      Delete-Sample-Data-For-Release "$base_dir\Samples\$sample"
      
      cp "$base_dir\Samples\$sample" "$build_dir\Output\Samples" -recurse -force
      
      Delete-Sample-Data-For-Release "$build_dir\Output\Samples\$sample" 
	}
}

task CreateOutpuDirectories -depends CleanOutputDirectory {
	mkdir $build_dir\Output
	mkdir $build_dir\Output\lib
	mkdir $build_dir\Output\Web
	mkdir $build_dir\Output\Web\bin
	mkdir $build_dir\Output\Server
	mkdir $build_dir\Output\EmbeddedClient
	mkdir $build_dir\Output\Client-3.5
	mkdir $build_dir\Output\Client
	mkdir $build_dir\Output\Bundles
	mkdir $build_dir\Output\Samples
}

task CleanOutputDirectory { 
	remove-item $build_dir\Output -Recurse -Force  -ErrorAction SilentlyContinue
}

task CopyEmbeddedClient { 
	cp $build_dir\Raven.Client.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Database.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Esent.Interop.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Lucene.Net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Spatial.Net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\log4net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Storage.Esent.dll $build_dir\Output\EmbeddedClient

}

task CopySmuggler { 
	cp $build_dir\RavenSmuggler.exe $build_dir\Output
}

task CopyClient {
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Client
	cp $build_dir\Raven.Client.Lightweight.dll $build_dir\Output\Client
}

task CopyClient35 {
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Client-3.5
	cp $build_dir\Raven.Client-3.5.dll $build_dir\Output\Client-3.5
}

task CopyWeb { 
	cp $build_dir\Raven.Web.dll $build_dir\Output\Web\bin
	cp $build_dir\log4net.dll $build_dir\Output\Web\bin
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Web\bin
	cp $build_dir\Lucene.Net.dll $build_dir\Output\Web\bin
	cp $build_dir\Spatial.Net.dll $build_dir\Output\Web\bin
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\Web\bin
	cp $build_dir\Rhino.Licensing.dll $build_dir\Output\Web\bin
	cp $build_dir\Esent.Interop.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Database.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Storage.Esent.dll $build_dir\Output\Web\bin
	
	cp $base_dir\DefaultConfigs\web.config $build_dir\Output\Web\web.config
	
}

task CopyBundles {
	cp $build_dir\Raven.Bundles.*.dll $build_dir\Output\Bundles
	del $build_dir\Output\Bundles\Raven.Bundles.Tests.dll
}

task CopyGems -depends CreateOutpuDirectories {
	cp $build_dir\Raven.Client.Lightweight.dll $build_dir\Output\lib
}

task CopyServer {
	cp $build_dir\Raven.Server.exe $build_dir\Output\Server
	cp $build_dir\log4net.dll $build_dir\Output\Server
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Server
	cp $build_dir\Lucene.Net.dll $build_dir\Output\Server
	cp $build_dir\Spatial.Net.dll $build_dir\Output\Server
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\Server
	cp $build_dir\Rhino.Licensing.dll $build_dir\Output\Server
	cp $build_dir\Esent.Interop.dll $build_dir\Output\Server
	cp $build_dir\Raven.Database.dll $build_dir\Output\Server
	cp $build_dir\Raven.Storage.Esent.dll $build_dir\Output\Server
	cp $base_dir\DefaultConfigs\RavenDb.exe.config $build_dir\Output\Server\Raven.Server.exe.config
}

task CopyDocFiles {
	cp $base_dir\license.txt $build_dir\Output\license.txt
	cp $base_dir\readme.txt $build_dir\Output\readme.txt
	cp $base_dir\acknowledgements.txt $build_dir\Output\acknowledgements.txt
}

task CreateGem -depends CopyGems {
	exec { 		
		$currentDate = [System.DateTime]::Today.ToString("yyyyMMdd")
		[System.IO.File]::WriteAllText( "$build_dir\Output\VERSION", "$version.$env:buildlabel.$currentDate", [System.Text.Encoding]::ASCII)
		$global:gem_result = "ravendb-$version.$env:buildlabel.$currentDate.gem"
		$old = pwd
		cd $build_dir\Output
		del $build_dir\Output\*.gem
		exec { & "$tools_dir\IronRuby\bin\igem.bat" build "$base_dir\ravendb.gemspec" }
		cd $old
	}
}

task DoRelease -depends Compile, `
	CleanOutputDirectory,`
	CreateOutpuDirectories, `
	CopyEmbeddedClient, `
	CopySmuggler, `
	CopyClient, `
	CopyClient35, `
	CopyWeb, `
	CopyBundles, `
	CopyGems, `
	CopyServer, `
	CopyDocFiles, `
	CopySamples, `
	CreateGem {
	
	$old = pwd
	
	cd $build_dir\Output
	
	$file = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
		
	exec { 
		& $tools_dir\zip.exe -9 -A -r `
			$file `
			EmbeddedClient\*.* `
			Client\*.* `
			Samples\*.* `
			Samples\*.* `
			Client-3.5\*.* `
			Web\*.* `
			Bundles\*.* `
			Web\bin\*.* `
			Server\*.* `
			*.*
	}
	
    cd $old
    ExecuteTask("ResetBuildArtifcats")
}

task ResetBuildArtifcats {
    git checkout "Raven.Database\RavenDB.snk"
}

task PushGem -depends CreateGem {
	exec { & "$tools_dir\IronRuby\bin\igem.bat" push "$global:gem_result" }
}

task Upload -depends DoRelease {
	Write-Host "Starting upload"
	if (Test-Path $uploader) {
		$log = $env:push_msg 
		if($log -eq $null -or $log.Length -eq 0) {
		  $log = git log -n 1 --oneline		
		}
		
		$file = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
		write-host "Executing: $uploader '$global:uploadCategory' $file '$log'"
		&$uploader "$uploadCategory" $file "$log"
			
		if ($lastExitCode -ne 0) {
			write-host "Failed to upload to S3: $lastExitCode"
			throw "Error: Failed to publish build"
		}
	}
	else {
		Write-Host "could not find upload script $uploadScript, skipping upload"
	}
	
	
}

task UploadCommercial -depends Commercial, DoRelease, Upload {
		
}	

task UploadOpenSource -depends OpenSource, DoRelease, Upload {
		
}	

task UploadUnstable -depends OpenSource, DoRelease, Upload {
		
}	