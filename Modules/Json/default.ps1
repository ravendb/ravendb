properties {
  $base_dir  = resolve-path .
  $lib_dir = "$base_dir\SharedLibs"
  $build_dir = "$base_dir\build"
  $buildartifacts_dir = "$build_dir\"
  $sln_file = "$base_dir\Raven.Json.sln"
  $version = "1.0.0"
  $tools_dir = "$base_dir\Tools"
  $release_dir = "$base_dir\Release"
  $uploader = "..\Uploader\S3Uploader.exe"
}

include .\psake_ext.ps1

task default -depends Release

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
	
	$global:uploadCategory = "Raven.Json"
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
			-description "Raven.Json - Optimized Json DOM" `
			-company "Hibernating Rhinos" `
			-product "Raven Json $version.0" `
			-version "$version.0" `
			-fileversion "1.0.0.$env:buildlabel" `
			-copyright "Copyright © Hibernating Rhinos and Ayende Rahien 2004 - 2010" `
			-clsCompliant "true"
	}
		
	new-item $release_dir -itemType directory
	new-item $buildartifacts_dir -itemType directory
	
	copy $tools_dir\xUnit\*.* $build_dir
}

task Compile -depends Init {
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
    exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$sln_file" /p:OutDir="$buildartifacts_dir\" }
}

task Test -depends Compile {
  $old = pwd
  cd $build_dir
  exec { &"$build_dir\xunit.console.clr4.exe" "$build_dir\Raven.Json.Tests.dll" } 
  cd $old
}

task ReleaseNoTests -depends OpenSource,DoRelease {

}


task Release -depends Test,DoRelease { 
}

task DoRelease -depends Compile {
	
	$old = pwd
	
	cd $build_dir
	
	$file = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
		
	exec { 
		& $tools_dir\zip.exe -9 -A -r `
			$file `
			Raven.Json.dll `
			Raven.Json.xml 
	}
	
    cd $old
}
