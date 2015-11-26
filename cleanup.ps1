function GetGitDirectory
{
    $path = "C:\Program Files\Git"
    if (Test-Path $path) 
    {
        return $path
    }
    
    $path = "C:\Program Files (x86)\Git"
    if (Test-Path $path) 
    {
        return $path
    }
    
    $path = "$env:USERPROFILE\AppData\Local\Programs\Git"
    if (Test-Path $path) 
    {
        return $path
    }
}

$path = split-path -parent $MyInvocation.MyCommand.Definition

if (($args.Count -gt 0) -and ($args[0] -eq "force"))
{
    $result = 0
} 
else
{
    $title = "Cleanup"
    $message = "Do you want to DELETE all uncommited files in " + $path + "?"

    $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", `
        "Deletes all uncommited files in the folder."

    $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", `
        "Retains all uncommited files in the folder."

    $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)
    
    $result = $host.ui.PromptForChoice($title, $message, $options, 1) 
}

$gitDirectory = GetGitDirectory
$gitPath = "$gitDirectory\bin\git.exe"


switch ($result)
{
    0 {
        Write-Host "Performing cleanup"
        Get-ChildItem $path -Include bin,obj,build -Recurse -Force | Select -ExpandProperty FullName | Where {$_ -notlike '*Imports*' -and $_ -notlike '*pvc-packages*'} | Remove-Item -Force -Recurse
        &$gitPath clean -f -x -d
    }
    1 { return; }
}