using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15240 : RavenTestBase
    {
        public RavenDB_15240(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanCalculateTombstoneCleanerStateCorrectly(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new MyTsIndex().Execute(store);
                new MyCounterIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company, "companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.UtcNow, new double[] { 3 }, "tag");
                    session.CountersFor(company).Increment("HeartRate", 6);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                var tombstoneCleaner = database.TombstoneCleaner;
                var state = tombstoneCleaner.GetState().Tombstones;

                Assert.Equal(0, state["Companies"].Documents.Etag);
                Assert.Equal(2, state["Companies"].TimeSeries.Etag);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Delete();

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                state = tombstoneCleaner.GetState().Tombstones;

                Assert.Equal(0, state["Companies"].Documents.Etag);
                Assert.Equal(2, state["Companies"].TimeSeries.Etag);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.Delete(company);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                state = tombstoneCleaner.GetState().Tombstones;

                Assert.Equal(9, state["Companies"].Documents.Etag);
                Assert.Equal(2, state["Companies"].TimeSeries.Etag);
            }
        }

        private class MyTsIndex : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public MyTsIndex()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });
            }
        }

        private class MyCounterIndex : AbstractCountersIndexCreationTask<Company>
        {
            public MyCounterIndex()
            {
                AddMap("HeartRate", counters => from counter in counters
                                                select new
                                                {
                                                    HeartBeat = counter.Value,
                                                    Name = counter.Name,
                                                    User = counter.DocumentId
                                                });
            }
        }
    }
}
