properties {
  $base_dir  = resolve-path .
  $lib_dir = "$base_dir\SharedLibs"
  $build_dir = "$base_dir\build"
  $buildartifacts_dir = "$build_dir\"
  $sln_file = "$base_dir\RavenDB.sln"
  $version = "1.0.0.0"
  $tools_dir = "$base_dir\Tools"
  $release_dir = "$base_dir\Release"
}

include .\psake_ext.ps1

task default -depends Release

task Clean {
  remove-item -force -recurse $buildartifacts_dir -ErrorAction SilentlyContinue
  remove-item -force -recurse $release_dir -ErrorAction SilentlyContinue
}

task Init -depends Clean {
	Generate-Assembly-Info `
		-file "$base_dir\Raven.Database\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
		
	Generate-Assembly-Info `
		-file "$base_dir\Raven.Sample.SimpleClient\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
		
	Generate-Assembly-Info `
		-file "$base_dir\Raven.Sample.ComplexSharding\Properties\AssemblyInfo.cs" `
		-title "Raven Database $version" `
		-description "A linq enabled document database for .NET" `
		-company "Hibernating Rhinos" `
		-product "Raven Database $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2010" `
		-clsCompliant "false"
	
	Generate-Assembly-Info `
		-file "$base_dir\Raven.Sample.ShardClient\Properties\AssemblyInfo.cs" `
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
	
	.\Utilities\Binaries\Raven.DefaultDatabase.Creator .\Raven.Database\Defaults\default.json
		
	new-item $release_dir -itemType directory
	new-item $buildartifacts_dir -itemType directory
	
	copy $tools_dir\xUnit\*.* $build_dir
}

task Compile -depends Init {
	$v4_net_version = (ls "C:\Windows\Microsoft.NET\Framework\v4.0*").Name
    exec "C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" """$sln_file"" /p:OutDir=""$buildartifacts_dir\"""
}

task Test -depends Compile {
  $old = pwd
  cd $build_dir
  exec "$build_dir\xunit.console.exe" "$build_dir\Raven.Tests.dll"
  exec "$build_dir\xunit.console.exe" "$build_dir\Raven.Scenarios.dll"
  exec "$build_dir\xunit.console.exe" "$build_dir\Raven.Client.Tests.dll"
  cd $old
}

task Merge -depends Compile {
	$old = pwd
	cd $build_dir
	
	remove-item $build_dir\RavenDb.exe  -ErrorAction SilentlyContinue
	remove-item $build_dir\RavenClient.dll -ErrorAction SilentlyContinue
	remove-item $build_dir\RavenWeb.dll  -ErrorAction SilentlyContinue
	
	exec "..\Utilities\Binaries\Raven.Merger.exe"
	
	cd $old
}

task ReleaseNoTests -depends DoRelease {

}

task Release -depends Test,DoRelease { 
}

task DoRelease -depends Merge {
	
	remove-item $build_dir\Output -Recurse -Force  -ErrorAction SilentlyContinue
	mkdir $build_dir\Output
	mkdir $build_dir\Output\Web
	mkdir $build_dir\Output\Web\bin
	mkdir $build_dir\Output\Server
	mkdir $build_dir\Output\EmbeddedClient
	mkdir $build_dir\Output\Client-3.5
	mkdir $build_dir\Output\Client
	
	cp $build_dir\Raven.Client.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Raven.Database.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Esent.Interop.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\ICSharpCode.NRefactory.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Lucene.Net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\log4net.dll $build_dir\Output\EmbeddedClient
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\EmbeddedClient
	
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Client
	cp $build_dir\Raven.Client.Lightweight.dll $build_dir\Output\Client
	
	cp $build_dir\Newtonsoft.Json.dll $build_dir\Output\Client-3.5
	cp $build_dir\Raven.Client-3.5.dll $build_dir\Output\Client-3.5
	
	
	cp $build_dir\RavenWeb.dll $build_dir\Output\Web\bin
	cp $base_dir\DefaultConfigs\web.config $build_dir\Output\Web\web.config
	
	cp $build_dir\RavenDb.exe $build_dir\Output\Server
	cp $base_dir\DefaultConfigs\RavenDb.exe.config $build_dir\Output\Server\RavenDb.exe.config
	
	cp $base_dir\license.txt $build_dir\Output\license.txt
	cp $base_dir\readme.txt $build_dir\Output\readme.txt
	cp $base_dir\acknowledgements.txt $build_dir\Output\acknowledgements.txt
	
	$old = pwd
	
	cd $build_dir\Output
	
	& $tools_dir\zip.exe -9 -A `
		$release_dir\Raven.zip `
		EmbeddedClient\*.* `
		Client\*.* `
		Client-3.5\*.* `
		Web\*.* `
		Web\bin\*.* `
		Server\*.* `
		license.txt `
		acknowledgements.txt `
		readme.txt
		
	if ($lastExitCode -ne 0) {
        throw "Error: Failed to execute ZIP command"
    }
    
    cd $old
}