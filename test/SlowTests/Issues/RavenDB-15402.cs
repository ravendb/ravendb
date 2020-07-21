using FastTests.Server.Replication;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15402 : ReplicationTestBase
    {
        public RavenDB_15402(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GetCountersShouldBeCaseInsensitive()
        {
            using (var store = GetDocumentStore())
            {
                var id = "companies/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), id);
                    session.CountersFor(id).Increment("Likes", 999);
                    session.CountersFor(id).Increment("DisLikes", 999);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(id);
                    var counters = session.CountersFor(company)
                        .Get(new []{"likes", "dislikes"});

                    Assert.Equal(2, counters.Count);
                    Assert.Equal(999, counters["likes"]);
                    Assert.Equal(999, counters["dislikes"]);
                }
            }
        }
    }
}
