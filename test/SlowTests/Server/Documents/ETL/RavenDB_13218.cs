using System;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_13218 : EtlTestBase
    {
        public RavenDB_13218(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("Orders", null)]
        [InlineData(null, null)]
        [InlineData("Orders", @"
    loadToOrders(this);

function loadCountersOfOrdersBehavior(doc, counter)
{
    return true;
}
")]
        public void MustNotIterateCountersTooFar(string collection, string script)
        {
            using (var src = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = x =>
                {
                    x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "20";
                    x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedItems)] = "20";
                }
            }))
            using (var dest = GetDocumentStore())
            {
                const int numberOfDocs = 50;

                using (var session = src.OpenSession())
                {
                    for (int i = 0; i < numberOfDocs; i++)
                    {
                        session.Store(new Order(), $"orders/{i}");
                    }

                    session.SaveChanges();
                }

                src.Operations.Send(new PatchByQueryOperation(@"from Orders update{
    incrementCounter(this, 'Foo', 1);
}")).WaitForCompletion();

                if (collection == null)
                {
                    AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);
                }
                else
                {
                    AddEtl(src, dest, collection, script: script);
                }

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses >= numberOfDocs * 2);

                etlDone.Wait(TimeSpan.FromSeconds(60));

                var destStats = dest.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(numberOfDocs, destStats.CountOfDocuments);
                Assert.Equal(numberOfDocs, destStats.CountOfCounterEntries);

                using (var session = dest.OpenSession())
                {
                    var order = session.Load<Order>("orders/48");

                    Assert.NotNull(order);

                    var counters = session.Advanced.GetCountersFor(order);

                    Assert.Equal(1, counters.Count);
                }
            }
        }
    }
}
