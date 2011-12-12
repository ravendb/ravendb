# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration


. ..\Samples.ps1

Recreate-RavenDB Server @("Raven.Bundles.IndexReplication")


Start .\Server\Raven.Server.exe --set=Raven/AnonymousAccess==All
