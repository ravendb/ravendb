using System.Collections.Generic;
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
				
				

                store.Counters.Increment("users/1-A", "likes", 0);
                var val = store.Operations.Send(new GetCounterValueOperation("users/1-A", "likes"));
                Assert.Equal(0, val);

                store.Counters.Increment("users/1-A", "likes", 10);
                val = store.Operations.Send(new GetCounterValueOperation("users/1-A", "likes"));
                Assert.Equal(10, val);

                store.Counters.Increment("users/1-A", "likes", -3);
                val = store.Operations.Send(new GetCounterValueOperation("users/1-A", "likes"));
                Assert.Equal(7, val);
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
                var a = store.Counters.IncrementAsync("users/1-A", "likes", 5);
                var b = store.Counters.IncrementAsync("users/1-A", "likes", 10);
                Task.WaitAll(a, b); // run them in parallel and see that they are good

                var val = store.Operations.Send(new GetCounterValueOperation("users/1-A", "likes"));
                Assert.Equal(15, val);
            }
        }

        [Fact]
        public void MultiGetCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");
                    session.SaveChanges();
                }
                var a = store.Counters.IncrementAsync("users/1-A", "likes", 5);
                var b = store.Counters.IncrementAsync("users/1-A", "dislikes", 10);
                var c = store.Counters.IncrementAsync("users/2-A", "rank", 20);
                Task.WaitAll(a, b, c); // run them in parallel and see that they are good

                var countersDetail = store.Operations.Send(new GetCounterValuesOperation(new GetOrDeleteCounters
                {
                    Counters = new List<CountersOperation>
                    {
                        new CountersOperation
                        {
                            DocumentId = "users/1-A",
                            Counters = new []{"likes", "dislikes"}
                        },
                        new CountersOperation
                        {
                            DocumentId = "users/2-A",
                            Counters = new []{"rank"}
                        }


                    }
                }));

                Assert.Equal(3, countersDetail.Counters.Count);

                Assert.Equal("likes", countersDetail.Counters[0].CounterName);
                Assert.Equal("users/1-A", countersDetail.Counters[0].DocumentId);
                Assert.Equal(5, countersDetail.Counters[0].TotalValue);

                Assert.Equal("dislikes", countersDetail.Counters[1].CounterName);
                Assert.Equal("users/1-A", countersDetail.Counters[1].DocumentId);
                Assert.Equal(10, countersDetail.Counters[1].TotalValue);

                Assert.Equal("rank", countersDetail.Counters[2].CounterName);
                Assert.Equal("users/2-A", countersDetail.Counters[2].DocumentId);
                Assert.Equal(20, countersDetail.Counters[2].TotalValue);

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

                store.Counters.Increment("users/1-A", "likes", 10);
                store.Counters.Increment("users/2-A", "likes", 20);

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
