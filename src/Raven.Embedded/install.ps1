param($installPath, $toolsPath, $package, $project)

function Remove($items) {
    foreach ($item in $items) {
        if ($item.Name -eq "RavenDBServer") {
            $item.Delete()
        }
    }
}

function Add($addFolder, $addPath) {   
    $pathFiles = Get-ChildItem -File -Path $addPath | ForEach-Object -Process {$_.FullName}
    $pathDirectories = Get-ChildItem -Directory -Path $addPath
    
    foreach ($item in $pathFiles) {
        $file = $addFolder.ProjectItems.AddFromFile($item)
        
        $copyToOutput = $file.Properties.Item("CopyToOutputDirectory")
        $copyToOutput.Value = 2
        
        $buildAction = $file.Properties.Item("BuildAction")
        $buildAction.Value = 0
    }
    
    foreach ($item in $pathDirectories) {   
        $itemName = $item.Name;
        $itemPath = $item.FullName;
        
        $subFolder = $addFolder.ProjectItems.AddFolder($itemName);
        
        Add $subFolder $itemPath
    }
}

Remove $project.ProjectItems

$folder = $project.ProjectItems.AddFolder("RavenDBServer");

$path = [io.path]::getfullpath([io.path]::combine($toolsPath, "..", "contentFiles", "any", "any", "RavenDBServer"))

Add $folder $path