# starts the server in debug mode

$raven = "..\..\Server" #release path
$replication = "..\..\Bundles\Raven.Bundles.IndexReplication.dll"
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build" #src path
  $replication = "..\..\build\Raven.Bundles.IndexReplication.dll"
}

rd .\Server\Data -force -recurse -erroraction silentlycontinue

cp $raven\Raven.Server.exe .\Server
cp $raven\log4net.dll .\Server
cp $raven\Newtonsoft.Json.dll .\Server
cp $raven\Lucene.Net.dll .\Server
cp $raven\ICSharpCode.NRefactory.dll .\Server
cp $raven\Rhino.Licensing.dll .\Server
cp $raven\Esent.Interop.dll .\Server
cp $raven\Raven.*.dll .\Server
cp $raven\Spatial.net.dll .\Server

mkdir .\Server\Plugins  -erroraction silentlycontinue

cp $replication .\Server\Plugins

Start .\Server\Raven.Server.exe