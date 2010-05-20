# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

$raven = "..\..\Server\RavenDB.exe" #release path
$aggregator = "..\..\Bundles\Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator.dll"
$aggregator_pdb = "..\..\Bundles\Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator.pdb"
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build\RavenDB.exe" #src path
  $aggregator = "..\..\build\Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator.dll"
  $aggregator_pdb = "..\..\build\Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator.pdb"
}

rd .\Server\Data -force -recurse -erroraction silentlycontinue

cp $raven .\Server

mkdir .\Server\Plugins  -erroraction silentlycontinue

cp $aggregator .\Server\Plugins

if( (test-path $aggregator_pdb) -eq $true) #release hasn't any debug information
{
	cp $aggregator_pdb .\Server\Plugins
}

start .\Server\RavenDB.exe
