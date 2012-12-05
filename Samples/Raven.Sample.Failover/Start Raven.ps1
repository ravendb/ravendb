# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

start .\..\..\Server\Raven.Server.exe --set=Raven/Port==8080,--set=Raven/AnonymousAccess==All, --ram
start .\..\..\Server\Raven.Server.exe --set=Raven/Port==8081,--set=Raven/AnonymousAccess==All, --ram