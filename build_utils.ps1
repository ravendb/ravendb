function Get-File-Exists-On-Path
{
	param(
		[string]$file
	)
	$results = ($Env:Path).Split(";") | Get-ChildItem -filter $file -erroraction silentlycontinue
	$found = ($results -ne $null)
	return $found
}

function Get-Git-Commit
{
	if ((Get-File-Exists-On-Path "git.exe")){
		$gitLog = git log --oneline -1
		return $gitLog.Split(' ')[0]
	}
	else {
		return "0000000"
	}
}

function Get-DependencyPackageFiles
{
	param([string]$packageName, [string]$frameworkVersion = "net45")
	
	$fullPackageName = Get-ChildItem "$base_dir\packages\$packageName.*" | 
								Sort-Object Name -Descending | 
								Select-Object -First 1
	Return "$fullPackageName\lib\$frameworkVersion\*"
}

Function Get-PackagePath {
	Param([string]$packageName)
		
	$packagePath = Get-ChildItem "$packages_dir\$packageName.*" |
						Sort-Object Name -Descending | 
						Select-Object -First 1
	Return "$packagePath"
}