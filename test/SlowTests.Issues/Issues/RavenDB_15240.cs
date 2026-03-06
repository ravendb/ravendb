using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Tests.Infrastructure;
using Xunit;

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
                await new MyTsIndex().ExecuteAsync(store);
                await new MyCounterIndex().ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company, "companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.UtcNow, new double[] { 3 }, "tag");
                    session.CountersFor(company).Increment("HeartRate", 6);

                    session.SaveChanges();
                }

                await Indexes.WaitForIndexingAsync(store);

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

                await Indexes.WaitForIndexingAsync(store);

                state = tombstoneCleaner.GetState().Tombstones;

                Assert.Equal(0, state["Companies"].Documents.Etag);
                Assert.Equal(2, state["Companies"].TimeSeries.Etag);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.Delete(company);

                    session.SaveChanges();
                }

                await Indexes.WaitForIndexingAsync(store);

                WaitForValue(() =>
                {
                    state = tombstoneCleaner.GetState().Tombstones;
                    var companyState = state["Companies"];
                    return companyState.Documents.Etag == 9 && companyState.TimeSeries.Etag == 2;
                }, true);
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
