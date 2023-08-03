using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20678 : RavenTestBase
{
    public RavenDB_20678(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void CanBreakSingleTxOfCounters(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var company = new Company();
                session.Store(company, "companies/1");
                var c = session.CountersFor("companies/1");
                for (int i = 0; i < 1000; i++)
                {
                    c.Increment($"Day{i + 1}", Random.Shared.Next(1, 100));
                }
                session.SaveChanges();
            }

            store.Maintenance.Send(new StopIndexingOperation());

            var result = store.Maintenance.Send(new PutIndexesOperation(new CountersIndexDefinition
            {
                Name = "MyCounterIndex",
                Maps = {
                    "from counter in counters.Companies " +
                    "select new { " +
                    "   HeartBeat = counter.Value, " +
                    "   Name = counter.Name," +
                    "   User = counter.DocumentId " +
                    "}" },
                Configuration = new IndexConfiguration
                {
                    [RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = "128"
                }
            }));

            var staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
            Assert.True(staleness.IsStale);
            Assert.Equal(1, staleness.StalenessReasons.Count);
            Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

            store.Maintenance.Send(new StartIndexingOperation());

            Indexes.WaitForIndexing(store);

            staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
            Assert.False(staleness.IsStale);

            store.Maintenance.Send(new StopIndexingOperation());

            var terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "Name", null));
            Assert.Equal(1000, terms.Length);
        }
    }
}
