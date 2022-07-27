using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Counters;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15134 : RavenTestBase
    {
        public RavenDB_15134(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GetCountersOperationShouldReturnNullForNonExistingCounter()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "users/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new object(), docId);

                    var c = session.CountersFor(docId);

                    c.Increment("likes");
                    c.Increment("dislikes", 2);

                    session.SaveChanges();
                }

                var vals = store.Operations.Send(new GetCountersOperation(docId, new[] { "likes", "downloads", "dislikes" }));
                Assert.Equal(3, vals.Counters.Count);

                Assert.Equal(1, vals.Counters[0].TotalValue);
                Assert.Null(vals.Counters[1]);
                Assert.Equal(2, vals.Counters[2].TotalValue);


                vals = store.Operations.Send(new GetCountersOperation(docId, new[] { "likes", "downloads", "dislikes" }, returnFullResults: true));
                Assert.Equal(3, vals.Counters.Count);

                Assert.Equal(1, vals.Counters[0].CounterValues.Count);
                Assert.Null(vals.Counters[1]);
                Assert.Equal(1, vals.Counters[2].CounterValues.Count);
            }
        }
    }
}
