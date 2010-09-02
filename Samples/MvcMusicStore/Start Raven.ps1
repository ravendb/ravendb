# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

$raven = "..\..\Server" #release path
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build" #src path
}

rd Data -force -recurse -erroraction silentlycontinue

cp $raven\Raven.Server.exe .\
cp $raven\log4net.dll .\
cp $raven\Newtonsoft.Json.dll .\
cp $raven\Lucene.Net.dll .\
cp $raven\ICSharpCode.NRefactory.dll .\
cp $raven\Rhino.Licensing.dll .\
cp $raven\Esent.Interop.dll .\
cp $raven\Raven.Database.dll .\
cp $raven\Raven.Storage.Esent.dll .\

start .\Raven.Server.exe