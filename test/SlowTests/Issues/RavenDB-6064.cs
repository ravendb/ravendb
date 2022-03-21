using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6064_2 : RavenTestBase
    {
        public RavenDB_6064_2(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string A, B, C;
            public string D;
        }

        private class User_Index : AbstractIndexCreationTask<User, User>
        {
            public User_Index()
            {
                Map = users =>
                    from user in users
                    select new
                    {
                        user.A,
                        user.B,
                        user.C,
                        user.D
                    };
                Reduce = results =>
                    from result in results
                    group result by result.D
                    into g
                    select new
                    {
                        D = g.Key,
                        g.First().A,
                        g.First().B,
                        g.First().C
                    };
            }
        }

        [Fact]
        public void CanIndexWithThreeCompressedProperties()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        A = new string('a', 129),
                        B = new string('b', 257),
                        C = new string('c', 513),
                        D = "u"
                    });
                    s.SaveChanges();
                }
                new User_Index().Execute(store);

                Indexes.WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));
                    var collection = s.Query<User, User_Index>().ToList();
                    Assert.NotEmpty(collection);
                }
            }
        }
    }
}
