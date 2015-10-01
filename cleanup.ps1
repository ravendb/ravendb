$path = split-path -parent $MyInvocation.MyCommand.Definition
$title = "Cleanup"
$message = "Do you want to DELETE all uncommited files in " + $path + "?"

$yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", `
    "Deletes all uncommited files in the folder."

$no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", `
    "Retains all uncommited files in the folder."

$options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)
	
$result = $host.ui.PromptForChoice($title, $message, $options, 1) 

$gitPath = "C:\Program Files\Git\bin\git.exe";
If (Test-Path $gitPath) {
} else {
	$gitPath = "C:\Program Files (x86)\Git\bin\git.exe";
}

switch ($result)
    {
        0 {
			Get-ChildItem $path -Include bin,obj,build -Recurse -Force | Select -ExpandProperty FullName | Where {$_ -notlike '*Imports*' -and $_ -notlike '*pvc-packages*'} | Remove-Item -Force -Recurse
			&$gitPath clean -f -x -d
		}
        1 { return; }
    }