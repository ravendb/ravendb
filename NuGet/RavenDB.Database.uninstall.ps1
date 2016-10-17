param($installPath, $toolsPath, $package, $project)

function Remove($items) {
    foreach ($item in $items) {
        if ($item.Name -eq "Raven.Studio.Html5.zip") {
            $item.Remove()
        }
        
        Remove($item.ProjectItems)
    }
}

Remove($project.ProjectItems)