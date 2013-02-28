. ..\Samples.ps1

Recreate-RavenDB "Server" @("Raven.Bundles.Sample.EventSourcing.ShoppingCartAggregator")

#start .\Server\Raven.Server.exe
