param($installPath, $toolsPath, $package, $project)
$xap = "Raven.Studio.xap"
$project.ProjectItems.AddFromFile("$toolsPath\$xap")
$project.ProjectItems.Item($xap).Properties.Item("CopyToOutputDirectory").Value = 2