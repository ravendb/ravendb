properties {
  $base_dir  = resolve-path .
  $lib_dir = "$base_dir\SharedLibs"
  $build_dir = "$base_dir\build"
  $buildartifacts_dir = "$build_dir\"
  $sln_file = "$base_dir\zzz_RavenDB_Release.sln"
  $version = "1.0.0"
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
	
	if($env:buildlabel -eq $null) {
		$env:buildlabel = "13"
	}
	
	$projectFiles = ls -path $base_dir -include *.csproj -recurse | 
					Where { $_ -notmatch [regex]::Escape($lib_dir) } | 
					Where { $_ -notmatch [regex]::Escape($tools_dir) }
	
	foreach($projectFile in $projectFiles) {
		
		$projectDir = [System.IO.Path]::GetDirectoryName($projectFile)
		$projectName = [System.IO.Path]::GetFileName($projectDir)
		$asmInfo = [System.IO.Path]::Combine($projectDir, [System.IO.Path]::Combine("Properties", "AssemblyInfo.cs"))
		
		Generate-Assembly-Info `
			-file $asmInfo `
			-title "$projectName $version.0" `
			-description "A linq enabled document database for .NET" `
			-company "Hibernating Rhinos" `
			-product "RavenDB $version.0" `
			-version "$version.0" `
			-fileversion "1.0.0.$env:buildlabel" `
			-copyright "Copyright © Hibernating Rhinos and Ayende Rahien 2004 - 2010" `
			-clsCompliant "true"
	}
		
	new-item $release_dir -itemType directory
	new-item $buildartifacts_dir -itemType directory
	
	copy $tools_dir\xUnit\*.* $build_dir
	 
	if($global:commercial) {
		exec { .\Utilities\Binaries\Raven.ProjectRewriter.exe commercial }
		cp "..\RavenDB_Commercial.snk" "Raven.Database\RavenDB.snk"
	}
	else {
		exec { .\Utilities\Binaries\Raven.ProjectRewriter.exe }
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
  exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\Raven.Munin.Tests.dll" } 
  exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\Raven.Tests.dll" } 
  exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\Raven.Scenarios.dll" }
  exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\Raven.Client.VisualBasic.Tests.dll" }
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
	$samples = @("MvcMusicStore", "Raven.Sample.ShardClient", "Raven.Sample.Failover", "Raven.Sample.Replication", `
               "Raven.Sample.EventSourcing", "Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator", `
               "Raven.Samples.IndexReplication", "Raven.Samples.Includes", "Raven.Sample.SimpleClient", `
               "Raven.Sample.ComplexSharding", "Raven.Sample.MultiTenancy", "Raven.Sample.MultiTenancy", `
               "Raven.Sample.LiveProjections")
	$exclude = @("bin", "obj", "Data", "Plugins")
	
	foreach ($sample in $samples) {
      echo $sample 
      
      Delete-Sample-Data-For-Release "$base_dir\Samples\$sample"
      
      cp "$base_dir\Samples\$sample" "$build_dir\Output\Samples" -recurse -force
      
      Delete-Sample-Data-For-Release "$build_dir\Output\Samples\$sample" 
	}
	
	cp "$base_dir\Samples\Raven.Samples.sln" "$build_dir\Output\Samples" -force
      
	exec { .\Utilities\Binaries\Raven.Samples.PrepareForRelease.exe "$build_dir\Output\Samples\Raven.Samples.sln" "$build_dir\Output" }
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
	cp $build_dir\Raven.Client.Lightweight.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Client.Lightweight.xml $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Client.Embedded.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Client.Embedded.xml $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Abstractions.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Http.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Database.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Http.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Esent.Interop.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Lucene.Net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Spatial.Net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\SpellChecker.Net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\log4net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\EmbeddedClient
  cp $build_dir\Raven.Storage.Esent.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Storage.Managed.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Munin.dll $build_dir\Output\EmbeddedClient
}

task CopySmuggler { 
	cp $build_dir\RavenSmuggler.exe $build_dir\Output
}

task CopyClient {
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Client
  cp $build_dir\Raven.Abstractions.dll $build_dir\Output\Client
	cp $build_dir\Raven.Client.Lightweight.dll $build_dir\Output\Client
	cp $build_dir\Raven.Client.Lightweight.xml $build_dir\Output\Client
}

task CopyClient35 {
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Client-3.5
	cp $build_dir\Raven.Abstractions-3.5.dll $build_dir\Output\Client-3.5
	cp $build_dir\Raven.Client.Lightweight-3.5.dll $build_dir\Output\Client-3.5
}

task CopyWeb { 
	cp $build_dir\Raven.Abstractions.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Web.dll $build_dir\Output\Web\bin
	cp $build_dir\log4net.dll $build_dir\Output\Web\bin
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Web\bin
	cp $build_dir\Lucene.Net.dll $build_dir\Output\Web\bin
	cp $build_dir\Spatial.Net.dll $build_dir\Output\Web\bin
	cp $build_dir\SpellChecker.Net.dll $build_dir\Output\Web\bin
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\Web\bin
	cp $build_dir\Rhino.Licensing.dll $build_dir\Output\Web\bin
	cp $build_dir\Esent.Interop.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Database.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Http.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Storage.Esent.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Storage.Managed.dll $build_dir\Output\Web\bin
	cp $build_dir\Raven.Munin.dll $build_dir\Output\Web\bin	
	cp $base_dir\DefaultConfigs\web.config $build_dir\Output\Web\web.config
	
}

task CopyBundles {
	cp $build_dir\Raven.Bundles.*.dll $build_dir\Output\Bundles
	cp $build_dir\Raven.Client.*.dll $build_dir\Output\Bundles
	del $build_dir\Output\Bundles\Raven.Bundles.Tests.dll
}

task CopyServer {
	cp $build_dir\Raven.Server.exe $build_dir\Output\Server
	cp $build_dir\log4net.dll $build_dir\Output\Server
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Server
	cp $build_dir\Lucene.Net.dll $build_dir\Output\Server
	cp $build_dir\Spatial.Net.dll $build_dir\Output\Server
	cp $build_dir\SpellChecker.Net.dll $build_dir\Output\Server
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\Server
	cp $build_dir\Rhino.Licensing.dll $build_dir\Output\Server
	cp $build_dir\Esent.Interop.dll $build_dir\Output\Server
	cp $build_dir\Raven.Abstractions.dll $build_dir\Output\Server
	cp $build_dir\Raven.Database.dll $build_dir\Output\Server
	cp $build_dir\Raven.Http.dll $build_dir\Output\Server
	cp $build_dir\Raven.Storage.Esent.dll $build_dir\Output\Server
	cp $build_dir\Raven.Storage.Managed.dll $build_dir\Output\Server
	cp $build_dir\Raven.Munin.dll $build_dir\Output\Server
	cp $base_dir\DefaultConfigs\RavenDb.exe.config $build_dir\Output\Server\Raven.Server.exe.config
}

task CreateNupack {
	del $base_dir\*.nupkg
	remove-item $build_dir\NuPack -force -recurse -erroraction silentlycontinue
	mkdir $build_dir\NuPack
	mkdir $build_dir\NuPack\Lib
	mkdir $build_dir\NuPack\Lib\3.5
	mkdir $build_dir\NuPack\Lib\4.0
	mkdir $build_dir\NuPack\Tools
	
	$nupack = [xml](get-content $base_dir\RavenDB.nuspec)
	
	$nupack.package.metadata.version = "$version.$env:buildlabel"

	$writerSettings = new-object System.Xml.XmlWriterSettings
	$writerSettings.OmitXmlDeclaration = $true
	$writerSettings.NewLineOnAttributes = $true
	$writerSettings.Indent = $true
	
	$writer = [System.Xml.XmlWriter]::Create("$build_dir\Nupack\RavenDB.nuspec", $writerSettings)
	
	$nupack.WriteTo($writer)
	$writer.Flush()
	$writer.Close()
	
	cp $build_dir\Newtonsoft.Json.dll $build_dir\NuPack\Lib\3.5
	cp $build_dir\Raven.Client.Lightweight-3.5.dll $build_dir\NuPack\Lib\3.5
	cp $build_dir\Raven.Client.Lightweight-3.5.xml $build_dir\NuPack\Lib\3.5
	cp $build_dir\Raven.Abstractions-3.5.dll $build_dir\NuPack\Lib\3.5
	
	cp $build_dir\Newtonsoft.Json.dll $build_dir\NuPack\Lib\4.0
	cp $build_dir\Raven.Client.Lightweight.dll $build_dir\NuPack\Lib\4.0
	cp $build_dir\Raven.Abstractions.dll $build_dir\NuPack\Lib\4.0
	cp $build_dir\Raven.Client.Lightweight.xml $build_dir\NuPack\Lib\4.0
	
	
	cp $build_dir\Raven.Server.exe $build_dir\NuPack\Tools
	cp $build_dir\log4net.dll $build_dir\NuPack\Tools
	cp $build_dir\Newtonsoft.Json.dll $build_dir\NuPack\Tools
	cp $build_dir\Lucene.Net.dll $build_dir\NuPack\Tools
	cp $build_dir\Spatial.Net.dll $build_dir\NuPack\Tools
	cp $build_dir\SpellChecker.Net.dll $build_dir\NuPack\Tools
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\NuPack\Tools
	cp $build_dir\Rhino.Licensing.dll $build_dir\NuPack\Toolsr
	cp $build_dir\Esent.Interop.dll $build_dir\NuPack\Tools
	cp $build_dir\Raven.Database.dll $build_dir\NuPack\Tools
	cp $build_dir\Raven.Http.dll $build_dir\NuPack\Tools
	cp $build_dir\Raven.Storage.Esent.dll $build_dir\NuPack\Tools
	cp $build_dir\Raven.Storage.Managed.dll $build_dir\NuPack\Tools
	cp $build_dir\Raven.Munin.dll $build_dir\NuPack\Tools
	cp $base_dir\DefaultConfigs\RavenDb.exe.config $build_dir\NuPack\Tools\Raven.Server.exe.config
	
	& $tools_dir\NuPack.exe $build_dir\NuPack\RavenDB.nuspec
}

task CreateDocs {
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	if($env:buildlabel -eq 13)
	{
      return 
	}
     
  # we expliclty allows this to fail
  & "C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\Raven.Docs.shfbproj" /p:OutDir="$buildartifacts_dir\"
}

task CopyDocFiles -depends CreateDocs {
	cp $base_dir\license.txt $build_dir\Output\license.txt
	cp $base_dir\readme.txt $build_dir\Output\readme.txt
	cp $base_dir\Help\Documentation.chm $build_dir\Output\Documentation.chm  -ErrorAction SilentlyContinue
	cp $base_dir\acknowledgements.txt $build_dir\Output\acknowledgements.txt
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
	CopyServer, `
	CopyDocFiles, `
	CopySamples, `
	CreateNupack {
	
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

task UploadUnstable -depends Unstable, DoRelease, Upload {
		
}	