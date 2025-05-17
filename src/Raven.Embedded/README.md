# RavenDB .NET Embedded Server
You're looking at RavenDB .NET Embedded Server NuGet release.  
It lets you to spin-up RavenDB server locally in no-time using friendly API.

RavenDB is a NoSQL database that fuses extreme performance with ease-of-use, offering above the roof developer experience.  
Learn more at https://ravendb.net or visit our [GitHub repository](https://github.com/ravendb/ravendb).


## Installation
Check the runtime [prerequisites](https://ravendb.net/docs/article-page/server/embedded#prerequisite) in our docs and get the latest stable version from [NuGet](https://www.nuget.org/packages/RavenDB.Embedded).

## Learning resources
- **Embedded Server Guide** - https://ravendb.net/articles/building-a-beer-vending-machine-program-with-ravendb-embedded-server 🛣️
- **Documentation** - https://ravendb.net/docs/article-page/server/embedded 🎒
- Code Samples - https://demo.ravendb.net/ 🧑‍💻

## Community
- **RavenDB's Discord** - https://discord.gg/ravendb 🍻
- Articles & guides - https://ravendb.net/articles 📰
- YouTube - https://www.youtube.com/@ravendb_net 🍿
- Blog - https://ayende.com/blog/ 🧑‍💻


## Getting started

### Initialize
```csharp
EmbeddedServer.Instance.StartServer();
using (var store = EmbeddedServer.Instance.GetDocumentStore("Embedded"))
{
    using (var session = store.OpenSession())
    {
        // Your code here
    }
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
