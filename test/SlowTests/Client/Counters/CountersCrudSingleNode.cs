using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Counters;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersCrudSingleNode : RavenTestBase
    {
        private static readonly Guid TombstoneMarker = new Guid("DEAD0000-5A81-4CB1-9E4D-E60B8EBDCE64");
        private const string DocId = "users/1-A";

        [Fact]
        public void IncrementCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" });
                    session.SaveChanges();
                }

                store.Operations.Send(new IncrementCounterOperation(DocId, "likes"));
                var val = store.Operations.Send(new GetCounterValueOperation(DocId, "likes"));
                Assert.Equal(0, val);

                store.Operations.Send(new IncrementCounterOperation(DocId, "likes", 10));
                val = store.Operations.Send(new GetCounterValueOperation(DocId, "likes"));
                Assert.Equal(10, val);

                store.Operations.Send(new IncrementCounterOperation(DocId, "likes", -3));
                val = store.Operations.Send(new GetCounterValueOperation(DocId, "likes"));
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
                    session.Store(new User { Name = "Aviv" });
                    session.SaveChanges();
                }

                for (var i = 0; i < 5; i++)
                {
                    store.Operations.Send(new IncrementCounterOperation(DocId, $"ctr{i}"));
                }

                var counters = store.Operations.Send(new GetCountersForDocumentOperation(DocId)).ToList();
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
                    session.Store(new User { Name = "Aviv" });
                    session.SaveChanges();
                }

                store.Operations.Send(new IncrementCounterOperation(DocId, "likes", 5));
                store.Operations.Send(new IncrementCounterOperation(DocId, "likes", 10));

                var dic = store.Operations.Send(new GetCounterValuesOperation(DocId, "likes"));
                Assert.Equal(15, dic.Values.Single());
            }
        }

        [Fact]
        public void ResetCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" });
                    session.SaveChanges();
                }

                store.Operations.Send(new IncrementCounterOperation(DocId, "likes", 10));
                var val = store.Operations.Send(new GetCounterValueOperation(DocId, "likes"));
                Assert.Equal(10, val);

                store.Operations.Send(new ResetCounterOperation(DocId, "likes"));
                var dic = store.Operations.Send(new GetCounterValuesOperation(DocId, "likes"));

                Assert.Equal(0, dic.Values.Single());
                Assert.Equal(TombstoneMarker, dic.Keys.Single());

            }
        }
    }
}
