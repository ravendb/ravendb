param($installPath, $toolsPath, $package, $project)

function Remove($items) {
    foreach ($item in $items) {
        if ($item.Name -eq "RavenDBServer") {
            $item.Delete()
        }
    }
}

Remove($project.ProjectItems)