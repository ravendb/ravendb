# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

$raven = "..\..\Server\RavenDB.exe" #release path
$replication = "..\..\Bundles\Raven.Bundles.Replication.dll"
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build\RavenDB.exe" #src path
  $replication = "..\..\build\Raven.Bundles.Replication.dll"
}

rd .\Servers\Shard1\Data -force -recurse -erroraction silentlycontinue
rd .\Servers\Shard2\Data -force -recurse -erroraction silentlycontinue

cp $raven .\Servers\Shard1
cp $raven  .\Servers\Shard2

mkdir .\Servers\Shard1\Plugins  -erroraction silentlycontinue
mkdir .\Servers\Shard2\Plugins  -erroraction silentlycontinue

cp $replication \Servers\Shard1\Plugins
cp $replication \Servers\Shard2\Plugins

start .\Servers\Shard1\RavenDB.exe
start .\Servers\Shard2\RavenDB.exe