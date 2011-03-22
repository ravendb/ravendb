# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

$raven = "..\..\Server" #release path
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build" #src path
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
cp $raven\Raven.Http.dll .\Server
cp $raven\Raven.Storage.Managed.dll .\Server
cp $raven\Raven.Abstractions.dll .\Server
cp $raven\Spatial.Net.dll .\Server
cp $raven\SpellChecker.Net.dll .\Server
cp $raven\Raven.Munin.dll .\Server

start .\Server\Raven.Server.exe

