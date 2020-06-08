using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15109 : RavenTestBase
    {
        public RavenDB_15109(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void BulkIncrementNewCounterShouldAddCounterNameToMetadata()
        {
            using (var store = GetDocumentStore())
            {
                string id;
                using (var bulkInsert = store.BulkInsert())
                {
                    var user = new User { Name = "Aviv1" };
                    bulkInsert.Store(user);
                    id = user.Id;

                    var counter = bulkInsert.CountersFor(id);
                    for (var i = 1; i <= 10; i++)
                    {
                        counter.Increment(i.ToString(), i);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var all = session.CountersFor(id).GetAll();
                    Assert.Equal(10, all.Count);
                }

                using (var session = store.OpenSession())
                {
                    var u = session.Load<User>(id);
                    var counters = session.Advanced.GetCountersFor(u);
                    Assert.NotNull(counters); // fails here
                    Assert.Equal(10, counters.Count);
                }
            }
        }
    }
}
