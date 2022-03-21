using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class WithStringReverse : RavenTestBase
    {
        public WithStringReverse(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }

        private class ReversedResult
        {
            public string ReverseName { get; set; }
        }

        [Fact]
        public async Task GivenSomeUsers_QueryWithAnIndex_ReturnsUsersWithNamesReversed()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs select new { doc.Name, ReverseName = doc.Name.Reverse()}" },
                        Name = "StringReverseIndex"
                    }}));

                using (IDocumentSession documentSession = store.OpenSession())
                {
                    documentSession.Store(new User { Name = "Ayende" });
                    documentSession.Store(new User { Name = "Itamar" });
                    documentSession.Store(new User { Name = "Pure Krome" });
                    documentSession.Store(new User { Name = "John Skeet" });
                    documentSession.Store(new User { Name = "StackOverflow" });
                    documentSession.Store(new User { Name = "Wow" });
                    documentSession.SaveChanges();
                }

                using (IDocumentSession documentSession = store.OpenSession())
                {
                    var users = documentSession
                        .Query<User>("StringReverseIndex")
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                    var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                    Assert.Equal(errorsCount, 0);

                    Assert.NotNull(users);
                    Assert.True(users.Count > 0);
                }
            }
        }

        [Fact]
        public async Task CanQueryInReverse()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs select new { doc.Name, ReverseName = doc.Name.Reverse()}" },
                        Name = "StringReverseIndex"
                    }}));

                using (IDocumentSession documentSession = store.OpenSession())
                {
                    documentSession.Store(new User { Name = "Ayende" });
                    documentSession.Store(new User { Name = "Itamar" });
                    documentSession.Store(new User { Name = "Pure Krome" });
                    documentSession.Store(new User { Name = "John Skeet" });
                    documentSession.Store(new User { Name = "StackOverflow" });
                    documentSession.Store(new User { Name = "Wow" });
                    documentSession.SaveChanges();
                }

                using (IDocumentSession documentSession = store.OpenSession())
                {
                    var users = documentSession
                        .Query<ReversedResult>("StringReverseIndex")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.ReverseName.StartsWith("edn"))
                        .As<User>()
                        .ToList();

                    var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                    var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                    Assert.Equal(errorsCount, 0);

                    Assert.True(users.Count > 0);
                }
            }
        }
    }
}
