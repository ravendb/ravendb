param([string[]]$bundles)

$appConfigFileName = "Raven.Server.exe.config"
$webConfigFileName = "web.config"
$base_dir  = resolve-path .

$global:downladUrl = ""
$global:pluginsPath = "NOT-FOUND"
$global:tempZipPath = ""
$global:tempFolder = "$env:temp\RavenDB-Plugins-Get"

function GetPluginsFolder {
  if(test-path $base_dir\$webConfigFileName){ # First, try loading from web.config
    $config = [xml](get-content $base_dir\$webConfigFileName)
    $node = $config.SelectSingleNode("/configuration/appSettings/add[@key='Raven/PluginsDirectory']")
    if ($node) {
      $global:pluginsPath = $node.value
      return
    }
   }
   
  if(test-path $base_dir\$appConfigFileName){ # Try loading from appConfig
    $config = [xml](get-content $base_dir\$appConfigFileName)
    $node = $config.SelectSingleNode("/configuration/appSettings/add[@key='Raven/PluginsDirectory']")
    if ($node) {
      $global:pluginsPath = $node.value
      return
    }
   }
   
   if(test-path $base_dir\Plugins){ # Last resort - try the default folder if exists
      $global:pluginsPath = "$base_dir\Plugins"
      return
   }
}

function FigureOutDownloadUrl {
  # TODO: Support unstable builds
  # TODO: Support specific builds
  $global:downladUrl = "http://builds.hibernatingrhinos.com/downloadLatest/ravendb"
}

function GetBuildPackage {
  FigureOutDownloadUrl
  $clnt = new-object System.Net.WebClient
  $global:tempZipPath = "$env:temp\RavenDB-Latest.zip"
  Write-Host "Downloading RavenDB binaries..."
  $clnt.DownloadFile($global:downladUrl, $global:tempZipPath)
  Write-Host -foregroundcolor Green "Download complete"
}

if (!($bundles.Length)) {
  Write-Host -foregroundcolor Red "Please specify bundles to get"
  return
}


# Locate Plugins folder
GetPluginsFolder
if ($global:pluginsPath.Equals("NOT-FOUND")){
  $global:pluginsPath = "$base_dir\Plugins"
}

# Create a Plugins folder in the default location
if (!(Test-Path -path $global:pluginsPath))
{
  New-Item $global:pluginsPath -type directory
}

Write-Host -foregroundcolor Green "Found plugins directory in $global:pluginsPath"

# Download build package
GetBuildPackage

# Create temp directory if does not exist
if (!(Test-Path -path $global:tempFolder))
{
  New-Item $global:tempFolder -type directory
}

# Unzip to temp folder
Write-Host "Unpacking..."
$sh = new-object -com shell.application
$zip_file = $sh.namespace($global:tempZipPath)
$targetfolder = $sh.namespace($global:tempFolder) # where to unzip to
$targetfolder.Copyhere($zip_file.items(), 0x14)

# Iterate over existing plugins, and copy new files from the package if exist
foreach ($bundle in $bundles) {
  if (Test-Path -path $global:tempFolder\Bundles\Raven.Bundles.$bundle.dll) {
    Write-Host "Copying Raven.Bundles.$bundle.dll"
    Copy-Item $global:tempFolder\Bundles\Raven.Bundles.$bundle.dll $global:pluginsPath\Raven.Bundles.$bundle.dll
    
    if (Test-Path -path $global:tempFolder\Bundles\Raven.Bundles.$bundle.pdb){
      Write-Host "Copying Raven.Bundles.$bundle.pdb"
      Copy-Item $global:tempFolder\Bundles\Raven.Bundles.$bundle.pdb $global:pluginsPath\Raven.Bundles.$bundle.pdb
    }
  }
  else {
    Write-Host -foregroundcolor Yellow "Warning: bundle '$bundle' was not found"
  }
}

# Cleanup
if (Test-Path -path $global:tempZipPath) {
  Remove-Item $global:tempZipPath
}
if (Test-Path -path $global:tempFolder) {
  Remove-Item $global:tempFolder -recurse -force
}

Write-Host -foregroundcolor Green "Update completed successfully"