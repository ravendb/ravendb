using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using SlowTests.Server.Documents.Indexing.MapReduce;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_19056 : RavenTestBase
{
    public RavenDB_19056(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Querying)]
    public void ShouldThrowOnAttemptToCreateIndexWithOutputReduce()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var e = Assert.Throws<NotSupportedInShardingException>(() => new DailyInvoicesIndex().Execute(store));
            Assert.Contains("Index with output reduce to collection is not supported in sharding.", e.Message);
        }
    }

    private class DailyInvoicesIndex : AbstractIndexCreationTask<OutputReduceToCollectionTests.Invoice, OutputReduceToCollectionTests.DailyInvoice>
    {
        public DailyInvoicesIndex()
        {
            Map = invoices =>
                from invoice in invoices
                select new OutputReduceToCollectionTests.DailyInvoice
                {
                    Date = invoice.IssuedAt.Date,
                    Amount = invoice.Amount
                };

            Reduce = results =>
                from r in results
                group r by r.Date
                into g
                select new OutputReduceToCollectionTests.DailyInvoice
                {
                    Date = g.Key,
                    Amount = g.Sum(x => x.Amount)
                };

            OutputReduceToCollection = "DailyInvoices";
        }
    }
}
