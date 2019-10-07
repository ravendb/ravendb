using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13432 : RavenTestBase
    {
        [Fact]
        public void CountersInSelectClauseShouldAffectQueryEtag()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var counterValue = session.Query<User>().Select(u => RavenQuery.Counter(u, "Downloads")).First();
                    Assert.Equal(100, counterValue);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("Downloads", 50);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should not be served from cache 
                    var counterValue = session.Query<User>().Select(u => RavenQuery.Counter(u, "Downloads")).First();
                    Assert.Equal(150, counterValue);
                }
            }
        }

        [Fact]
        public void CountersInSelectClauseShouldAffectQueryEtag_JsProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>().Select(u => new
                    {
                        Name = u.Name + " "+ u.LastName, // creates JS projection
                        Counter = RavenQuery.Counter(u, "Downloads")
                    }).First();

                    Assert.Equal(100, query.Counter);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("Downloads", 50);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should not be served from cache 
                    var query = session.Query<User>().Select(u => new
                    {
                        Name = u.Name + " " + u.LastName, // creates JS projection
                        Counter = RavenQuery.Counter(u, "Downloads")
                    }).First();

                    Assert.Equal(150, query.Counter);
                }
            }
        }
    }
}
