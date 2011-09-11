function Copy-RavenDB
{
  param(
    [string]$dest
  )
  
  $raven = "..\..\Server" #release path
  if( (test-path $raven) -eq $false)
  {
    $raven = "..\..\build" #src path
  }

  $server_files = @( "Raven.Server.exe", "Raven.Json.???", "Raven.Studio.xap", "nlog.???", "Newtonsoft.Json.???", "Lucene.Net.???", `
                     "Spatial.Net.???", "SpellChecker.Net.???", "ICSharpCode.NRefactory.???", "Rhino.Licensing.???", "BouncyCastle.Crypto.???", `
                    "Esent.Interop.???", "Raven.Abstractions.???", "Raven.Database.???", "Raven.Http.???", "Raven.Storage.Esent.???", `
                    "Raven.Storage.Managed.???", "Raven.Munin.???" );
                    
   foreach($serverFile in $server_files) 
   {
      Copy-Item "$raven\$serverFile" $dest -ErrorAction SilentlyContinue -Force
   }
}

function Recreate-RavenDB
{
  param(
    [string]$dest,
    [array]$bundles
  )
  $originalDest = $dest
  $pwd = pwd
  $dest = [System.IO.Path]::Combine($pwd, $dest)
  
  rd $dest -force -recurse -erroraction silentlycontinue
  md $dest 
  
  $parent = [System.IO.Path]::GetDirectoryName($dest)
  Set-Content -Path "$parent/.gitignore" "$originalDest/"
  
  Copy-RavenDB $dest
  
  md $dest\Plugins -erroraction silentlycontinue
  
  $bundlesPath = "..\..\Bundles"
  if( (Test-Path "$bundlesPath\$bundle.???") -eq $false) 
  {
    $bundlesPath = "..\..\build"
  }
  
  foreach($bundle in $bundles) 
  {
     Copy-Item  "$bundlesPath\$bundle.???" "$dest\Plugins" -ErrorAction SilentlyContinue -Force
  }
  
}