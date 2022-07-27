using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15046 : RavenTestBase
    {
        public RavenDB_15046(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanResetTimeSeriesAndCountersIndex()
        {
            using (var store = GetDocumentStore())
            {
                new MyCountersIndex().Execute(store);
                new MyTimeSeriesIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);

                    session.CountersFor(company).Increment("Likes", 7);
                    session.TimeSeriesFor(company, "Likes").Append(DateTime.UtcNow, 5);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(new MyCountersIndex().IndexName));
                Assert.True(indexStats.EntriesCount > 0);
                indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(new MyTimeSeriesIndex().IndexName));
                Assert.True(indexStats.EntriesCount > 0);

                store.Maintenance.Send(new StopIndexingOperation());

                store.Maintenance.Send(new ResetIndexOperation(new MyCountersIndex().IndexName));
                store.Maintenance.Send(new ResetIndexOperation(new MyTimeSeriesIndex().IndexName));

                indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(new MyCountersIndex().IndexName));
                Assert.Equal(0, indexStats.EntriesCount);
                indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(new MyTimeSeriesIndex().IndexName));
                Assert.Equal(0, indexStats.EntriesCount);
            }
        }

        private class MyCountersIndex : AbstractCountersIndexCreationTask<Company>
        {
            public MyCountersIndex()
            {
                AddMap("Likes", counters => from counter in counters
                                            select new
                                            {
                                                Value = counter.Value
                                            });
            }
        }

        private class MyTimeSeriesIndex : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public MyTimeSeriesIndex()
            {
                AddMap("Likes", timeSeries => from ts in timeSeries
                                              from entry in ts.Entries
                                              select new
                                              {
                                                  Value = entry.Value
                                              });
            }
        }
    }
}
