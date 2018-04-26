using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Counters;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersCrudSingleNode : RavenTestBase
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

        [Fact]
        public void GetCountersForDoc()
        {           
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1");
                    session.SaveChanges();
                }

                for (var i = 0; i < 5; i++)
                {
                    store.Operations.Send(new IncrementCounterOperation("users/1", $"ctr{i}"));
                }

                var counters = store.Operations.Send(new GetCountersForDocumentOperation("users/1")).ToList();
                Assert.Equal(5, counters.Count);

                for (var i = 0; i < 5; i++)
                {
                    Assert.Equal($"ctr{i}", counters[i]);
                }
            }
        }

        [Fact]
        public void GetCounterValues()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1");
                    session.SaveChanges();
                }

                store.Operations.Send(new IncrementCounterOperation("users/1", "likes", 5));
                store.Operations.Send(new IncrementCounterOperation("users/1", "likes", 10));

                var dic = store.Operations.Send(new GetCounterValuesOperation("users/1", "likes"));
                Assert.Equal(dic.Values.Single(), 15L);
            }
        }
    }
}
