# RavenDB .NET Client - Newtonsoft.Json serialization
This package plugs [Newtonsoft.Json](https://www.newtonsoft.com/json) into the RavenDB .NET Client.

The RavenDB .NET Client serializes entities with System.Text.Json by default. Install this package to keep serializing them with Newtonsoft.Json, for example when your entities rely on Newtonsoft.Json attributes, converters or `JObject` values.

## Installation
Get the latest stable version from [NuGet](https://www.nuget.org/packages/RavenDB.Client.NewtonsoftJson).

## Usage
```csharp
using Raven.Client.Json.Serialization.NewtonsoftJson;

using (IDocumentStore store = new DocumentStore
{
    Urls = new[] { "http://localhost:8080" },
    Database = "Northwind",
    Conventions =
    {
        Serialization = new NewtonsoftJsonSerializationConventions
        {
            CustomizeJsonSerializer = serializer => { }
        }
    }
})
{
    store.Initialize();
}
```

## Learning resources
- **Documentation** - https://ravendb.net/docs
- **RavenDB's Discord** - https://discord.gg/ravendb
