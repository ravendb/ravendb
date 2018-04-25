using FastTests;
using Raven.Client.Documents.Operations.Counters;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersCrud : RavenTestBase
    {
        [Fact]
        public void IncrementCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1");
                    session.SaveChanges();
                }

                store.Operations.Send(new IncrementCounterOperation("users/1", "likes"));
                var val = store.Operations.Send(new GetCounterValueOperation("users/1", "likes"));
                Assert.Equal(0, val);

                store.Operations.Send(new IncrementCounterOperation("users/1", "likes", 10));
                val = store.Operations.Send(new GetCounterValueOperation("users/1", "likes"));
                Assert.Equal(10, val);

                store.Operations.Send(new IncrementCounterOperation("users/1", "likes", -3));
                val = store.Operations.Send(new GetCounterValueOperation("users/1", "likes"));
                Assert.Equal(7, val);
            }
        }
    }
}
