using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16146 : RavenTestBase
    {
        public RavenDB_16146(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void Query_With_Where_Clause(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "John" }, "users/1");
                    session.Store(new User { Name = "Jane" }, "users/2");
                    session.Store(new User { Name = "Tarzan" }, "users/3");
                    session.SaveChanges();

                    var q1 = session.Query<User>()
                        .Where(x => (x.Name == "John" || x.Name == "Jane") == false);

                    var q1AsString = q1.ToString();
                    var queryResult = q1.ToList();

                    var q2 = session.Query<User>()
                        .Where(x => !(x.Name == "John" || x.Name == "Jane"));

                    var q2AsString = q2.ToString();
                    var queryResult2 = q2.ToList();

                    Assert.Equal(q1AsString, q2AsString);

                    Assert.Equal(queryResult.Count, 1);
                    Assert.Equal(queryResult2.Count, 1);
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
