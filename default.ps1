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
  
  $web_dlls = @( "Raven.Abstractions.???", "Raven.Web.???", "log4net.???", "Newtonsoft.Json.???", "Lucene.Net.???", "Spatial.Net.???", "SpellChecker.Net.???", "ICSharpCode.NRefactory.???", `
    "Rhino.Licensing.???", "Esent.Interop.???", "Raven.Database.???", "Raven.Http.???", "Raven.Storage.Esent.???", "Raven.Storage.Managed.???", "Raven.Munin.???" );
    
  $web_files = @("Raven.Studio.xap", "..\DefaultConfigs\web.config" );
    
  $server_files = @( "Raven.Server.exe", "Raven.Studio.xap", "log4net.???", "Newtonsoft.Json.???", "Lucene.Net.???", "Spatial.Net.???", "SpellChecker.Net.???", "ICSharpCode.NRefactory.???", "Rhino.Licensing.???", `
    "Esent.Interop.???", "Raven.Abstractions.???", "Raven.Database.???", "Raven.Http.???", "Raven.Storage.Esent.???", "Raven.Storage.Managed.???", "Raven.Munin.???" );
    
  $client_dlls_3_5 = @( "Newtonsoft.Json.Net35.???", "Raven.Abstractions-3.5.???", "Raven.Client.Lightweight-3.5.???", "MissingBitsFromClientProfile.???" );
     
  $client_dlls = @( "Newtonsoft.Json.???", "Raven.Abstractions.???", "Raven.Client.Lightweight.???", "MissingBitsFromClientProfile.???", "AsyncCtpLibrary.???" );
  
  $silverlight_dlls = @( "Raven.Client.Silverlight.???", "AsyncCtpLibrary_Silverlight.???", "Newtonsoft.Json.Silverlight.???");   
  
  $all_client_dlls = @( "Raven.Client.Lightweight.???", "Raven.Client.Embedded.???", "Raven.Abstractions.???", "Raven.Http.???", "Raven.Database.???", "Raven.Http.???", `
      "Esent.Interop.???", "ICSharpCode.NRefactory.???", "Lucene.Net.???", "Spatial.Net.???", "SpellChecker.Net.???", "log4net.???", "Newtonsoft.Json.???", `
      "Raven.Storage.Esent.???", "Raven.Storage.Managed.???", "Raven.Munin.???", "AsyncCtpLibrary.???" );
      
  $test_prjs = @("Raven.Munin.Tests.dll", "Raven.Tests.dll", "Raven.Scenarios.dll", "Raven.Client.VisualBasic.Tests.dll", "Raven.Bundles.Tests.dll"  );
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
		
	new-item $release_dir -itemType directory -ErrorAction SilentlyContinue
	new-item $build_dir -itemType directory -ErrorAction SilentlyContinue
	
	copy $tools_dir\xUnit\*.* $build_dir
	
	# exec { .\Utilities\Binaries\Raven.Silverlighter.exe .\Raven.Client.Silverlight\Raven.Client.Silverlight.csproj .\Raven.Abstractions\Raven.Abstractions.csproj .\Raven.Client.Lightweight\Raven.Client.Lightweight.csproj}
	
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

task Test -depends Compile{
  $old = pwd
  cd $build_dir
  Write-Host $test_prjs
  foreach($test_prj in $test_prjs) {
    Write-Host "Testing $build_dir\$test_prj"
    exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\$test_prj" } 
  }
  cd $old
}

task TestSilverlight {
	
	try{
    start "$build_dir\Raven.Server.exe" "/ram"
    exec { 
      & ".\Tools\StatLight\StatLight.exe" "-x=.\Raven.Tests.Silverlight\Bin\Debug\Raven.Tests.Silverlight.xap" "--OverrideTestProvider=MSTestWithCustomProvider" "--ReportOutputFile=.\Raven.Tests.Silverlight.Results.xml"
    }
	}
	finally{
    ps "Raven.Server" | kill
	}
	
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

task Release -depends Test,TestSilverlight,DoRelease { 
}

task CopySamples {
	$samples = @("MvcMusicStore", "Raven.Sample.ShardClient", "Raven.Sample.Failover", "Raven.Sample.Replication", `
               "Raven.Sample.EventSourcing", "Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator", `
               "Raven.Samples.IndexReplication", "Raven.Samples.Includes", "Raven.Sample.SimpleClient", `
               "Raven.Sample.ComplexSharding", "Raven.Sample.MultiTenancy", "Raven.Sample.Suggestions", `
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
	mkdir $build_dir\Output\Silverlight
	mkdir $build_dir\Output\Client-3.5
	mkdir $build_dir\Output\Client
	mkdir $build_dir\Output\Bundles
	mkdir $build_dir\Output\Samples
}

task CleanOutputDirectory { 
	remove-item $build_dir\Output -Recurse -Force  -ErrorAction SilentlyContinue
}

task CopyEmbeddedClient { 

  foreach($client_dll in $all_client_dlls) {
    cp "$build_dir\$client_dll" $build_dir\Output\EmbeddedClient
  }
}

task CopySilverlight{ 

  foreach($silverlight_dll in $silverlight_dlls) {
    cp "$build_dir\$silverlight_dll" $build_dir\Output\Silverlight
  }
}

task CopySmuggler { 
	cp $build_dir\RavenSmuggler.??? $build_dir\Output
}

task CopyClient {
  foreach($client_dll in $client_dlls) {
    cp "$build_dir\$client_dll" $build_dir\Output\Client
  }
}

task CopyClient35 {
  foreach($client_dll in $client_dlls_3_5) {
    cp "$build_dir\$client_dll" $build_dir\Output\Client-3.5
  }
}

task CopyWeb { 
  foreach($web_dll in $web_dlls) {
    cp "$build_dir\$web_dll" $build_dir\Output\Web\bin
  }
  foreach($web_file in $web_files) {
    cp "$build_dir\$web_file" $build_dir\Output\Web
  }
}

task CopyBundles {
	cp $build_dir\Raven.Bundles.*.??? $build_dir\Output\Bundles
	cp $build_dir\Raven.Client.*.??? $build_dir\Output\Bundles
	del $build_dir\Output\Bundles\Raven.Bundles.Tests.???
}

task CopyServer {
   foreach($server_file in $server_files) {
    cp "$build_dir\$server_file" $build_dir\Output\Server
  }
	
	cp $base_dir\DefaultConfigs\RavenDb.exe.config $build_dir\Output\Server\Raven.Server.exe.config
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

task CopyRootFiles -depends CreateDocs {
	cp $base_dir\license.txt $build_dir\Output\license.txt
	cp $base_dir\Scripts\*.* $build_dir\Output\*.*
	cp $base_dir\readme.txt $build_dir\Output\readme.txt
	cp $base_dir\Help\Documentation.chm $build_dir\Output\Documentation.chm  -ErrorAction SilentlyContinue
	cp $base_dir\acknowledgements.txt $build_dir\Output\acknowledgements.txt
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
			Samples\*.* `
			Client-3.5\*.* `
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
	CleanOutputDirectory,`
	CreateOutpuDirectories, `
	CopyEmbeddedClient, `
	CopySmuggler, `
	CopyClient, `
	CopySilverlight, `
	CopyClient35, `
	CopyWeb, `
	CopyBundles, `
	CopyServer, `
	CopyRootFiles, `
	CopySamples, `
	ZipOutput, `
	CreateNugetPackage, `
	ResetBuildArtifcats {	
	Write-Host "Done building RavenDB"
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

task CreateNugetPackage {
  $accessPath = "$base_dir\..\Nuget-Access-Key.txt"
  
  if( $global:uploadCategory -ne "RavenDB") # we only publish the stable version out
  {
    return
  }
  
  if ( (Test-Path $accessPath) -eq $false )
  {
    return;
  }
  
  $accessKey = Get-Content $accessPath
  $accessKey = $accessKey.Trim()
  
  del $base_dir\*.nupkg
	remove-item $build_dir\NuPack -force -recurse -erroraction silentlycontinue
	mkdir $build_dir\NuPack
	mkdir $build_dir\NuPack\content
	mkdir $build_dir\NuPack\lib
	mkdir $build_dir\NuPack\lib\3.5
	mkdir $build_dir\NuPack\lib\4.0
	mkdir $build_dir\NuPack\tools
	mkdir $build_dir\NuPack\server
	
	foreach($client_dll in $client_dlls_3_5) {
    cp "$build_dir\$client_dll" $build_dir\NuPack\lib\3.5
  }

	foreach($client_dll in $client_dlls) {
    cp "$build_dir\$client_dll" $build_dir\NuPack\lib\4.0
  }	
  
 foreach($server_file in $server_files) {
    cp "$build_dir\$server_file" $build_dir\NuPack\server
  }
	
  cp $base_dir\DefaultConfigs\RavenDb.exe.config $build_dir\NuPack\server\Raven.Server.exe.config
  
  cp $base_dir\DefaultConfigs\Nupack.Web.config $build_dir\NuPack\content\Web.config.transform
  
  cp $build_dir\RavenSmuggler.??? $build_dir\NuPack\Tools
  
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
  
  & "$tools_dir\nuget.exe" pack $build_dir\NuPack\RavenDB.nuspec
  
  & "$tools_dir\nuget.exe" push -source http://packages.nuget.org/v1/ "RavenDB.$version.$env:buildlabel.nupkg" $accessKey
  
  
  # This is prune to failure since the previous package may not exists
  
  #$prevVersion = ($env:buildlabel - 1)
  # & "$tools_dir\nuget.exe" delete RavenDB "$version.$prevVersion" $accessKey -source http://packages.nuget.org/v1/ -NoPrompt
}