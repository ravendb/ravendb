using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15240 : RavenTestBase
    {
        public RavenDB_15240(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanCalculateTombstoneCleanerStateCorrectly()
        {
            using (var store = GetDocumentStore())
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

                WaitForIndexing(store);

                var database = await GetDocumentDatabaseInstanceFor(store);

                var tombstoneCleaner = database.TombstoneCleaner;
                var state = tombstoneCleaner.GetState();

                Assert.Equal(0, state["Companies"].Documents.Etag);
                Assert.Equal(3, state["Companies"].TimeSeries.Etag);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Delete();

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                state = tombstoneCleaner.GetState();

                Assert.Equal(0, state["Companies"].Documents.Etag);
                Assert.Equal(10, state["Companies"].TimeSeries.Etag);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.Delete(company);

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                state = tombstoneCleaner.GetState();

                Assert.Equal(12, state["Companies"].Documents.Etag);
                Assert.Equal(10, state["Companies"].TimeSeries.Etag);
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
