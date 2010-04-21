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
		-file "$base_dir\Raven.FileStorage\Properties\AssemblyInfo.cs" `
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
		-file "$base_dir\Raven.Importer\Properties\AssemblyInfo.cs" `
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
		-file "$base_dir\Raven.Importer\Properties\AssemblyInfo.cs" `
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
	
	if (([System.DateTime]::Now - (dir .\Raven.Server\Defaults\default.json).LastWriteTime).TotalHours -gt 1)
	{
			.\Utilities\Binaries\Raven.DefaultDatabase.Creator .\Raven.Server\Defaults\default.json
	}
		
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
  exec "$build_dirxunit.console.exe" "$build_dir\Raven.Client.Tests.dll"
  cd $old
}

task Merge -depends Compile {
	$old = pwd
	cd $build_dir
	
	remove-item $build_dir\RavenDb.exe  -ErrorAction SilentlyContinue
	
	exec "..\Utilities\Binaries\Raven.Merger.exe"
	
	cd $old
}

task Release -depends Test, Merge {
	& $tools_dir\zip.exe -9 -A -j `
		$release_dir\Raven.zip `
		$build_dir\RavenDb.exe `
		$build_dir\RavenDb.pdb `
		$build_dir\RavenDb.xml `
		$build_dir\Raven.Client.dll `
		$build_dir\Raven.Client.pdb `
		$build_dir\Raven.Client.xml `
		license.txt `
		acknowledgements.txt
	if ($lastExitCode -ne 0) {
        throw "Error: Failed to execute ZIP command"
    }
}