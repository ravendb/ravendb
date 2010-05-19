# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

$raven = "..\..\Server\RavenDB.exe" #release path
$aggregator = "..\..\Bundles\Raven.Sample.EventSourcing.ShoppingCartAggregator.dll"
$aggregator_pdb = "..\..\Bundles\Raven.Sample.EventSourcing.ShoppingCartAggregator.pdb"
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build\RavenDB.exe" #src path
  $aggregator = "..\..\build\Raven.Sample.EventSourcing.ShoppingCartAggregator.dll"
  $aggregator_pdb = "..\..\build\Raven.Sample.EventSourcing.ShoppingCartAggregator.pdb"
}

rd .\Server\Data -force -recurse -erroraction silentlycontinue

cp $raven .\Server

mkdir .\Server\Plugins  -erroraction silentlycontinue

cp $aggregator .\Server\Plugins
cp $aggregator_pdb .\Server\Plugins

start .\Server\RavenDB.exe
