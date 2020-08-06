using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.Counters;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
     public class RavenDB_15440 : RavenTestBase
    {
        public RavenDB_15440(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGetCounterWhichDoesntExist()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Raven"}, "users/1-A");
                    session.SaveChanges();
                }

                var longCounterName = new string('a', 5);

                var documentCountersOperation = new DocumentCountersOperation
                {
                    DocumentId = "users/1-A",
                    Operations = new List<CounterOperation>()
                    {
                        new CounterOperation() {CounterName = longCounterName, Type = CounterOperationType.Increment, Delta = 5}
                    }
                };

                var counterBatch = new CounterBatch {Documents = new List<DocumentCountersOperation>() {documentCountersOperation}};

                store.Operations.Send(new CounterBatchOperation(counterBatch));

                using (var session = store.OpenSession())
                {
                    var dic = session.CountersFor("users/1-A")
                        .Get(new string[] {longCounterName, "no_such"});

                    Assert.Equal(dic.Count, 1);
                }

            }
        }
    }
}
