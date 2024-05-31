using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.Counters;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22150 : RavenTestBase
{
    public RavenDB_22150(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Counters | RavenTestCategory.ClientApi)]
    public void IncrementByDefaultValue()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Danielle" }, "users/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var documentCounters = session.CountersFor("users/1");
                documentCounters.Increment("Likes", 10);
                documentCounters.Increment("Dislikes", 20);
                documentCounters.Increment("Downloads", 30);
                session.SaveChanges();
            }

            var operationResult = store.Operations.Send(new CounterBatchOperation(new CounterBatch
            {
                Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    // Increment by 5
                                    Type = CounterOperationType.Increment,
                                    CounterName = "likes",
                                    Delta = 5
                                },
                                new CounterOperation
                                {
                                    // Should increment by 1 (default value)
                                    Type = CounterOperationType.Increment,
                                    CounterName = "dislikes"
                                }
                            }
                        }
                    }
            }));

            // This fails, value doesn't increment, stays 20 
            Assert.Equal(21, operationResult.Counters[1].TotalValue);
        }
    }
}
