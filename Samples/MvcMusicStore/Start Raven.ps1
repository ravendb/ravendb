# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

$raven = "..\..\Server\RavenDB.exe" #release path
if( (test-path $raven) -eq $false)
{
  $raven = "..\..\build\RavenDB.exe" #src path
}

rd Data -force -recurse -erroraction silentlycontinue
cp  $raven .\
start .\RavenDB.exe