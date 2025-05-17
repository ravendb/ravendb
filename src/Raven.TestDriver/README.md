# RavenDB .NET Test Driver 
You're looking at RavenDB Test Driver Client (SDK) NuGet release.  
It makes it easy to write tests that require RavenDB instance, leveraging embedded server and providing you friendly API.

RavenDB is a NoSQL database that fuses extreme performance with ease-of-use, offering above the roof developer experience.  
Learn more at https://ravendb.net or visit our [GitHub repository](https://github.com/ravendb/ravendb).


## Installation
Get the latest stable version from [NuGet](https://www.nuget.org/packages/RavenDB.TestDriver).

## Learning resources
- **Test Driver Guide** - https://ravendb.net/articles/writing-unit-tests-with-ravendb-net-test-driver 🛣️
- **Documentation** - https://ravendb.net/docs/article-page/start/test-driver 🎒
- Code Samples - https://demo.ravendb.net/ 🧑‍💻

## Community
- **RavenDB's Discord** - https://discord.gg/ravendb 🍻
- Articles & guides - https://ravendb.net/articles 📰
- YouTube - https://www.youtube.com/@ravendb_net 🍿
- Blog - https://ayende.com/blog/ 🧑‍💻


## Getting started

### Configure
```csharp
var testServerOptions = new TestServerOptions
{
    // Looks for the newest version on your machine including 3.1.15 and any newer patches
    // but not major new releases (default is .NET version at time of server release).
    FrameworkVersion = "3.1.15+",

    // Specifies where ravendb server binaries are located (Optional)
    ServerDirectory = "PATH_TO_RAVENDB_SERVER",

    // Specifies where ravendb data will be placed/located (Optional)
    DataDirectory = "PATH_TO_RAVENDB_DATADIR", 
};

ConfigureServer(testServerOptions);
```

### Write a test
```csharp
using Raven.Client.Documents;
using Raven.TestDriver;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace RavenDBTestDriverFullExample
{

    public class RavenDBTestDriver : RavenTestDriver
    {
        static RavenDBTestDriver()
        {
            // ConfigureServer() must be set before calling GetDocumentStore()
            // and can only be set once per test run.
            ConfigureServer(new TestServerOptions
            {
                DataDirectory = "C:\\RavenDBTestDir"
            });
        }
        // This allows us to modify the conventions of the store we get from 'GetDocumentStore'
        protected override void PreInitialize(IDocumentStore documentStore)
        {
            documentStore.Conventions.MaxNumberOfRequestsPerSession = 50;
        }

        [Fact]
        public void MyFirstTest()
        {
            // GetDocumentStore() evokes the Document Store, which establishes and manages communication
            // between your client application and a RavenDB cluster via HTTP requests.
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TestDocumentByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument { Name = "Hello world!" });
                    session.Store(new TestDocument { Name = "Goodbye..." });
                    session.SaveChanges();
                }
                // If we want to query documents, sometimes we need to wait for the indexes to catch up  
                // to prevent using stale indexes.
                WaitForIndexing(store);

                // Sometimes we want to debug the test itself. This method redirects us to the studio
                // so that we can see if the code worked as expected (in this case, created two documents).
                WaitForUserToContinueTheTest(store);

                // Queries are defined in the session scope.
                // If there is no relevant index to quickly answer the query, RavenDB creates an auto-index
                // based on the query parameters.
                // This query will use the static index defined in lines 63-70 and filter the results by name.
                using (var session = store.OpenSession())
                {
                    var query = session.Query<TestDocument, TestDocumentByName>()
                        .Where(x => x.Name == "hello").ToList();
                    Assert.Single(query);
                }
            }
        }
    }
    // AbstractIndexCreationTask allows you to create and manually define a static index. 
    public class TestDocumentByName : AbstractIndexCreationTask<TestDocument>
    {
        public TestDocumentByName()
        {
            Map = docs => from doc in docs select new { doc.Name };
            Indexes.Add(x => x.Name, FieldIndexing.Search);
        }
    }

    public class TestDocument
    {
        public string Name { get; set; }
    }
}
```


## Contributing
- Submit an issue - https://github.com/ravendb/ravendb/issues
- GitHub Discussions - https://github.com/ravendb/ravendb/discussions
- Contributing rules - https://github.com/ravendb/ravendb/blob/v7.1/CONTRIBUTING.md
