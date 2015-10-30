param($installPath, $toolsPath, $package, $project)

$file = $project.ProjectItems.Item("Raven.Studio.Html5.zip")

$copyToOutput1 = $file.Properties.Item("CopyToOutputDirectory")
$copyToOutput1.Value = 1
