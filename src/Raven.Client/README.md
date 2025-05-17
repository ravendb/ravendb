# RavenDB .NET Client
You're looking at RavenDB .NET Client (SDK) NuGet release.  
It makes it easy for you to communicate with your RavenDB instance, letting you perform any database operations with friendly API.

RavenDB is a NoSQL database that fuses extreme performance with ease-of-use, offering above the roof developer experience.  
Learn more at https://ravendb.net or visit our [GitHub repository](https://github.com/ravendb/ravendb).


## Installation
Get the latest stable version from [NuGet](https://www.nuget.org/packages/RavenDB.Client).

## Learning resources
- **Documentation** - https://ravendb.net/docs 🎒
- Code Samples - https://demo.ravendb.net/ 🧑‍💻

## Community
- **RavenDB's Discord** - https://discord.gg/ravendb 🍻
- Articles & guides - https://ravendb.net/articles 📰
- YouTube - https://www.youtube.com/@ravendb_net 🍿
- Blog - https://ayende.com/blog/ 🧑‍💻


## Getting started

### Initialize
```csharp
using (IDocumentStore store = new DocumentStore
{
    Urls = new[]                        // URL to the Server,
    {                                   // or list of URLs 
        "http://live-test.ravendb.net"  // to all Cluster Servers (Nodes)
    },
    Database = "Northwind",             // Default database that DocumentStore will interact with
    Conventions = { }                   // DocumentStore customizations
})
{
    store.Initialize();                 // Each DocumentStore needs to be initialized before use.
                                        // This process establishes the connection with the Server
                                        // and downloads various configurations
                                        // e.g. cluster topology or client configuration
}
```

### Store documents
```csharp
using (IDocumentSession session = store.OpenSession())  // Open a session for a default 'Database'
{
    Category category = new Category
    {
        Name = "Database Category"
    };

    session.Store(category);                            // Assign an 'Id' and collection (Categories)
                                                        // and start tracking an entity

    Product product = new Product
    {
        Name = "RavenDB Database",
        Category = category.Id,
        UnitsInStock = 10
    };

    session.Store(product);                             // Assign an 'Id' and collection (Products)
                                                        // and start tracking an entity

    session.SaveChanges();                              // Send to the Server
                                                        // one request processed in one transaction
}
```

### Query
```csharp
using (IDocumentSession session = store.OpenSession())  // Open a session for a default 'Database'
{
    List<string> productNames = session
        .Query<Product>()                               // Query for Products
        .Where(x => x.UnitsInStock > 5)                 // Filter
        .Skip(0).Take(10)                               // Page
        .Select(x => x.Name)                            // Project
        .ToList();                                      // Materialize query
}
```

## Contributing
- Submit an issue - https://github.com/ravendb/ravendb/issues
- GitHub Discussions - https://github.com/ravendb/ravendb/discussions
- Contributing rules - https://github.com/ravendb/ravendb/blob/v7.1/CONTRIBUTING.md
