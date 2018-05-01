using System.Linq;
using System.Threading.Tasks;
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
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                store.Operations.Send(new IncrementCounterOperation("users/1-A", "likes", 0));
                var val = store.Operations.Send(new GetCounterValueOperation("users/1-A", "likes"));
                Assert.Equal(0, val);

                store.Operations.Send(new IncrementCounterOperation("users/1-A", "likes", 10));
                val = store.Operations.Send(new GetCounterValueOperation("users/1-A", "likes"));
                Assert.Equal(10, val);

                store.Operations.Send(new IncrementCounterOperation("users/1-A", "likes", -3));
                val = store.Operations.Send(new GetCounterValueOperation("users/1-A", "likes"));
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
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                for (var i = 0; i < 5; i++)
                {
                    store.Operations.Send(new IncrementCounterOperation("users/1-A", $"ctr{i}"));
                }

                var counters = store.Operations.Send(new GetCountersForDocumentOperation("users/1-A")).ToList();
                Assert.Equal(5, counters.Count);

                for (var i = 0; i < 5; i++)
                {
                    Assert.Equal($"ctr{i}", counters[i]);
                }
            }
        }

        [Fact]
        public void GetCounterValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                var a = store.Operations.SendAsync(new IncrementCounterOperation("users/1-A", "likes", 5));
                var b = store.Operations.SendAsync(new IncrementCounterOperation("users/1-A", "likes", 10));
                Task.WaitAll(a, b); // run them in parallel and see that they are good

                var val = store.Operations.Send(new GetCounterValueOperation("users/1-A", "likes"));
                Assert.Equal(15, val);
            }
        }

        [Fact]
        public void DeleteCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");

                    session.SaveChanges();
                }

                store.Operations.Send(new IncrementCounterOperation("users/1-A", "likes", 10));
                store.Operations.Send(new IncrementCounterOperation("users/2-A", "likes", 20));

                store.Operations.Send(new DeleteCounterOperation("users/1-A", "likes"));
                var val = store.Operations.Send(new GetCounterValueOperation("users/1-A", "likes"));
                Assert.Null(val);

                store.Operations.Send(new DeleteCounterOperation("users/2-A", "likes"));
                val = store.Operations.Send(new GetCounterValueOperation("users/2-A", "likes"));
                Assert.Null(val);
            }
        }

    }
}
