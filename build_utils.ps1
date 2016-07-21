Set-Variable CUSTOM_BUILD_NUMBER -option Constant -value "13"

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

function Get-Git-Commit-Full
{
    if ((Get-File-Exists-On-Path "git.exe")){
        $gitLog = git log -1 --format="%H"
        return $gitLog;
    }
    else {
        return "0000000000000000000000000000000000000000"
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

function Get-InformationalVersion($version, $label, $category) 
{
    if (!$category) {
        throw [System.ArgumentException] "Invalid build category $type"
    }

    if ($category.EndsWith("-Unstable")) {
        $result = "$version-rc-$label"
    }
    elseif ($category.EndsWith("-Hotfix")) {
        $result = "$version-hotfix-$label"
    }
    elseif ($category.EndsWith("-Custom")) {
        $result = "$version-custom-$label"
    }
    elseif ($category -eq "RavenDB") {
        $result = "$version"
    }
    else
    {
        throw [System.ArgumentException] "Invalid build category $type"
    }
    
    $result
}

function Get-BuildLabel
{
    $result = ""
    if($env:BUILD_NUMBER -ne $null) {
        $result = $env:BUILD_NUMBER
    }
    else {
        $result = $CUSTOM_BUILD_NUMBER
    }
    
    $result
}
