using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5422 : RavenTestBase
    {
        public RavenDB_5422(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldBeAbleToQueryLuceneTokens(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(new Users_ByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "OR" });
                    session.Store(new User() { Name = "AND" });
                    session.Store(new User() { Name = "NOT" });
                    session.Store(new User() { Name = "TO" });
                    session.Store(new User() { Name = "INTERSECT" });
                    session.Store(new User() { Name = "NULL" });

                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);
                    session.Query<User, Users_ByName>().Search(user => user.Name, "OR").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "AND").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "NOT").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "TO").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "INTERSECT").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "NULL").Single();
                }
            }
        }

        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from user in users select new { user.Name };
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
