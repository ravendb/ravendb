# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

$raven = "..\..\Server" #release path
$aggregator = "..\..\Bundles\Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator.dll"
$aggregator_pdb = "..\..\Bundles\Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator.pdb"
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build" #src path
  $aggregator = "..\..\build\Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator.dll"
  $aggregator_pdb = "..\..\build\Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator.pdb"
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

cp $aggregator .\Server\Plugins

if( (test-path $aggregator_pdb) -eq $true) #release hasn't any debug information
{
	cp $aggregator_pdb .\Server\Plugins
}

start .\Server\Raven.Server.exe
