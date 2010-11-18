# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

$raven = "..\..\Server" #release path
$replication = "..\..\Bundles\Raven.Bundles.Replication.dll"
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build" #src path
  $replication = "..\..\build\Raven.Bundles.Replication.dll"
}

rd .\Servers\Shard1\Data -force -recurse -erroraction silentlycontinue
rd .\Servers\Shard2\Data -force -recurse -erroraction silentlycontinue


cp $raven\Raven.Server.exe .\Servers\Shard1
cp $raven\log4net.dll .\Servers\Shard1
cp $raven\Newtonsoft.Json.dll .\Servers\Shard1
cp $raven\Lucene.Net.dll .\Servers\Shard1
cp $raven\ICSharpCode.NRefactory.dll .\Servers\Shard1
cp $raven\Rhino.Licensing.dll .\Servers\Shard1
cp $raven\Esent.Interop.dll .\Servers\Shard1
cp $raven\Raven.*.dll .\Servers\Shard1
cp $raven\spatial.net.dll .\Servers\Shard1


cp $raven\Raven.Server.exe .\Servers\Shard2
cp $raven\log4net.dll .\Servers\Shard2
cp $raven\Newtonsoft.Json.dll .\Servers\Shard2
cp $raven\Lucene.Net.dll .\Servers\Shard2
cp $raven\ICSharpCode.NRefactory.dll .\Servers\Shard2
cp $raven\Rhino.Licensing.dll .\Servers\Shard2
cp $raven\Esent.Interop.dll .\Servers\Shard2
cp $raven\Raven.*.dll .\Servers\Shard2
cp $raven\spatial.net.dll .\Servers\Shard2

mkdir .\Servers\Shard1\Plugins  -erroraction silentlycontinue
mkdir .\Servers\Shard2\Plugins  -erroraction silentlycontinue

cp $replication .\Servers\Shard1\Plugins
cp $replication .\Servers\Shard2\Plugins

start .\Servers\Shard1\Raven.Server.exe
start .\Servers\Shard2\Raven.Server.exe