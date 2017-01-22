using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Indexing;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class WithStringReverse : RavenTestBase
    {
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
        public void GivenSomeUsers_QueryWithAnIndex_ReturnsUsersWithNamesReversed()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("StringReverseIndex",
                                                new IndexDefinition
                                                    {
                                                        Maps = { "from doc in docs select new { doc.Name, ReverseName = doc.Name.Reverse()}" }
                                                });

                using (IDocumentSession documentSession = store.OpenSession())
                {
                    documentSession.Store(new User {Name = "Ayende"});
                    documentSession.Store(new User {Name = "Itamar"});
                    documentSession.Store(new User {Name = "Pure Krome"});
                    documentSession.Store(new User {Name = "John Skeet"});
                    documentSession.Store(new User {Name = "StackOverflow"});
                    documentSession.Store(new User {Name = "Wow"});
                    documentSession.SaveChanges();
                }

                using (IDocumentSession documentSession = store.OpenSession())
                {
                    var users = documentSession
                        .Query<User>("StringReverseIndex")
                        .Customize(x=>x.WaitForNonStaleResults())
                        .ToList();

                    var db = GetDocumentDatabaseInstanceFor(store).Result;
                    var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrors().Count);

                    Assert.Equal(errorsCount, 0);

                    Assert.NotNull(users);
                    Assert.True(users.Count > 0);
                }
            }
        }

        [Fact]
        public void CanQueryInReverse()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("StringReverseIndex",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Name, ReverseName = doc.Name.Reverse()}" }
                                                });

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
                        .Where(x=>x.ReverseName.StartsWith("edn"))
                        .As<User>()
                        .ToList();

                    var db = GetDocumentDatabaseInstanceFor(store).Result;
                    var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrors().Count);

                    Assert.Equal(errorsCount, 0);

                    Assert.True(users.Count > 0);
                }
            }
        }
    }
}
