# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration


. ..\Samples.ps1

Recreate-RavenDB Servers/Shard1 @("Raven.Bundles.Replication")
Recreate-RavenDB Servers/Shard2 @("Raven.Bundles.Replication")


start .\Servers\Shard1\Raven.Server.exe --set=Raven/Port==8080,--set=Raven/AnonymousAccess==All
start .\Servers\Shard2\Raven.Server.exe --set=Raven/Port==8081,--set=Raven/AnonymousAccess==All
