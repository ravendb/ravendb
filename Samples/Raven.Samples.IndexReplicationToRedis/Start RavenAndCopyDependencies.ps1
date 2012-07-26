# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration


. ..\Samples.ps1

Recreate-RavenDB Server @("Raven.Bundles.IndexReplicationToRedis")

#copy Poco type dll dependency
Copy-Item "..\..\SharedLibs\*" -filter "ServiceStack*.dll" -Destination ".\Server\Plugins" -ErrorAction SilentlyContinue -Force

#copy ServiceStack.Redis*.dll dependencies
Copy-Item "..\Raven.Samples.IndexReplicationToRedis.PocoTypes\bin\Debug\*" -filter "*.dll" -Destination ".\Server\Plugins" -ErrorAction SilentlyContinue -Force
Copy-Item "..\Raven.Samples.IndexReplicationToRedis.PocoTypes\bin\Release\*" -filter "*.dll" -Destination ".\Server\Plugins" -ErrorAction SilentlyContinue -Force

cp App.config .\Server\Raven.Server.exe.config -force

Start .\Server\Raven.Server.exe --set=Raven/AnonymousAccess==All