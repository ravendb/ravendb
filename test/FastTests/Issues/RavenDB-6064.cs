using Raven.Client.Indexes;
using Xunit;
using System.Linq;

namespace FastTests.Issues
{
    public class RavenDB_6064 : RavenTestBase
    {
        public class User
        {
            public string A, B, C;
            public string D;
        }

        public class User_Index : AbstractIndexCreationTask<User, User>
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

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var errors = store.DatabaseCommands.GetIndexErrors()[0];
                    Assert.Empty(errors.Errors);
                    var collection = s.Query<User, User_Index>().ToList();
                    Assert.NotEmpty(collection);
                }
            }
        }
    }
}