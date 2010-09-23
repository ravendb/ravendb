# starts the server in debug mode

$raven = "..\..\Server" #release path
$replication = "..\..\Bundles\Raven.Bundles.ReplicateToSql.dll"
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build" #src path
  $replication = "..\..\build\Raven.Bundles.ReplicateToSql.dll"
}

rd .\Server\Data -force -recurse -erroraction silentlycontinue

cp $raven\Raven.Server.exe .\Server
cp $raven\log4net.dll .\Server
cp $raven\Newtonsoft.Json.dll .\Server
cp $raven\Lucene.Net.dll .\Server
cp $raven\ICSharpCode.NRefactory.dll .\Server
cp $raven\Rhino.Licensing.dll .\Server
cp $raven\Esent.Interop.dll .\Server
cp $raven\Raven.Database.dll .\Server
cp $raven\Raven.Storage.Esent.dll .\Server


mkdir .\Server\Plugins  -erroraction silentlycontinue

cp $replication .\Server\Plugins

start .\Server\Raven.Server.exe