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

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Database\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "true"
		
	Generate-Assembly-Info `
		-file "$base_dir\Raven.Smuggler\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
		
	Generate-Assembly-Info `
		-file "$base_dir\Samples\Raven.Sample.SimpleClient\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
		
	Generate-Assembly-Info `
		-file "$base_dir\Samples\Raven.Sample.ComplexSharding\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
	
	Generate-Assembly-Info `
		-file "$base_dir\Samples\Raven.Sample.ShardClient\Properties\AssemblyInfo.cs" `
		-title "Raven Sample Shard Client $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Sample Shard Client $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Client\Properties\AssemblyInfo.cs" `
		-title "Raven Database Client $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
	
	Generate-Assembly-Info `
		-file "$base_dir\Raven.Client.Tests\Properties\AssemblyInfo.cs" `
		-title "Raven Database Client $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Server\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Web\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"


	Generate-Assembly-Info `
		-file "$base_dir\Raven.Scenarios\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"

	Generate-Assembly-Info `
		-file "$base_dir\Raven.Tests\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"


	Generate-Assembly-Info `
		-file "$base_dir\Raven.Tryouts\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
		
		
	Generate-Assembly-Info `
		-file "$base_dir\Bundles\Raven.Bundles.Tests\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
	
	Generate-Assembly-Info `
		-file "$base_dir\Bundles\Raven.Bundles.Versioning\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
		
	new-item $release_dir -itemType directory
	new-item $buildartifacts_dir -itemType directory
	
	copy $tools_dir\xUnit\*.* $build_dir
	 
	write-host "Commercial: ", $global:commercial
	if($global:commercial) {
		exec ".\Utilities\Binaries\Raven.ProjectRewriter.exe"
		cp "..\RavenDB_Commercial.snk" "Raven.Database\RavenDB.snk"
	}
	else {
		cp "Raven.Database\Raven.Database.csproj" "Raven.Database\Raven.Database.g.csproj"
	}
}

task Compile -depends Init {
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
    exec "C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" """$sln_file"" /p:OutDir=""$buildartifacts_dir\"""
    
    Write-Host "Merging..."
    $old = pwd
    cd $build_dir
     
    exec "..\Utilities\Binaries\Raven.Merger.exe"
    
    cd $old
    
    Write-Host "Finished merging"
    
    exec "C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" """$base_dir\Bundles\Raven.Bundles.sln"" /p:OutDir=""$buildartifacts_dir\"""
    exec "C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" """$base_dir\Samples\Raven.Samples.sln"" /p:OutDir=""$buildartifacts_dir\"""
}

task Test -depends Compile {
  $old = pwd
  cd $build_dir
  exec "$build_dir\xunit.console.exe" "$build_dir\Raven.Tests.dll"
  exec "$build_dir\xunit.console.exe" "$build_dir\Raven.Scenarios.dll"
  exec "$build_dir\xunit.console.exe" "$build_dir\Raven.Client.Tests.dll"
  exec "$build_dir\xunit.console.exe" "$build_dir\Raven.Bundles.Tests.dll"
  cd $old
}

task ReleaseNoTests -depends OpenSource,DoRelease {

}

task Commercial {
	$global:commercial = $true
	$global:uploadCategory = "RavenDB-Commercial"
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



task DoRelease -depends Compile {
	
	remove-item $build_dir\Output -Recurse -Force  -ErrorAction SilentlyContinue
	mkdir $build_dir\Output
	mkdir $build_dir\Output\Web
	mkdir $build_dir\Output\Web\bin
	mkdir $build_dir\Output\Server
	mkdir $build_dir\Output\EmbeddedClient
	mkdir $build_dir\Output\Client-3.5
	mkdir $build_dir\Output\Client
	mkdir $build_dir\Output\Bundles
	mkdir $build_dir\Output\Samples
	
	cp $build_dir\Raven.Client.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Database.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Esent.Interop.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Lucene.Net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\log4net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Storage.Esent.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Storage.Managed.dll $build_dir\Output\EmbeddedClient
	
	cp $build_dir\RavenSmuggler.exe $build_dir\Output
	
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Client
	cp $build_dir\Raven.Client.Lightweight.dll $build_dir\Output\Client
	
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Client-3.5
	cp $build_dir\Raven.Client-3.5.dll $build_dir\Output\Client-3.5
	
	
	cp $build_dir\Raven.Web.dll $build_dir\Output\Web\bin
	cp $build_dir\log4net.dll $build_dir\Output\Web\bin
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Web\bin
	cp $build_dir\Lucene.Net.dll $build_dir\Output\Web\bin
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\Web\bin
	cp $build_dir\Rhino.Licensing.dll $build_dir\Output\Web\bin
	cp $build_dir\Esent.Interop.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Database.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Storage.Managed.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Storage.Esent.dll $build_dir\Output\Web\bin
	
	cp $base_dir\DefaultConfigs\web.config $build_dir\Output\Web\web.config
	
	
	cp $build_dir\Raven.Bundles.*.dll $build_dir\Output\Bundles
	del $build_dir\Output\Bundles\Raven.Bundles.Tests.dll
	
	
	cp $build_dir\Raven.Server.exe $build_dir\Output\Server
	cp $build_dir\log4net.dll $build_dir\Output\Server
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Server
	cp $build_dir\Lucene.Net.dll $build_dir\Output\Server
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\Server
	cp $build_dir\Rhino.Licensing.dll $build_dir\Output\Server
	cp $build_dir\Esent.Interop.dll $build_dir\Output\Server
	cp $build_dir\Raven.Database.dll $build_dir\Output\Server
	cp $build_dir\Raven.Storage.Managed.dll $build_dir\Output\Server
	cp $build_dir\Raven.Storage.Esent.dll $build_dir\Output\Server
	cp $base_dir\DefaultConfigs\RavenDb.exe.config $build_dir\Output\Server\Raven.Server.exe.config
	
	cp $base_dir\license.txt $build_dir\Output\license.txt
	cp $base_dir\readme.txt $build_dir\Output\readme.txt
	cp $base_dir\acknowledgements.txt $build_dir\Output\acknowledgements.txt
	
	ExecuteTask("CopySamples")
	
	$old = pwd
	
	cd $build_dir\Output
	
	$file = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
		
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
		
	if ($lastExitCode -ne 0) {
        throw "Error: Failed to execute ZIP command"
    }
    
    cd $old
    ExecuteTask("ResetBuildArtifcats")
}

task ResetBuildArtifcats {
    git checkout "Raven.Database\RavenDB.snk"
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