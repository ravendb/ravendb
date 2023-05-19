using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_422 : RavenTestBase
    {
        public RavenDB_422(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void UsingStoreAllFields(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new UserIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "aye",
                        Email = "de"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var r = session.Advanced.DocumentQuery<dynamic>("UserIndex")
                        .WaitForNonStaleResults()
                        .SelectFields<dynamic>("UN", "UE")
                        .Single();

                    Assert.Equal("aye", r.UN.ToString());
                    Assert.Equal("de", r.UE.ToString());
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
            public string Email { get; set; }
        }

        private class UserIndex : AbstractIndexCreationTask<User>
        {
            public UserIndex()
            {
                Map = users =>
                      from user in users
                      select new { UN = user.Name, UE = user.Email };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
