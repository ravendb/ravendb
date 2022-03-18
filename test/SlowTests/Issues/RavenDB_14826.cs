using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14826 : RavenTestBase
    {
        public RavenDB_14826(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task SmugglerShouldTriggerTimeSeriesNotificationsForIndexing()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Employees_HeartBeat();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    var employee = new Employee();
                    session.Store(employee);
                    var baseline = DateTime.Now;
                    session.TimeSeriesFor(employee, "HeartBeat").Append(baseline, 10);
                    session.TimeSeriesFor(employee, "HeartBeat").Append(baseline.AddSeconds(1), 15);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.False(staleness.IsStale);

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfDocuments);
                Assert.Equal(1, stats.CountOfTimeSeriesSegments);
                Assert.Equal(1, stats.CountOfIndexes);

                var databaseName = $"{store}_smuggler";
                using (Databases.EnsureDatabaseDeletion(databaseName, store))
                {
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName)));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), store.Smuggler.ForDatabase(databaseName));
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    Indexes.WaitForIndexing(store, databaseName);
                    RavenTestHelper.AssertNoIndexErrors(store, databaseName);

                    staleness = store.Maintenance.ForDatabase(databaseName).Send(new GetIndexStalenessOperation(index.IndexName));
                    Assert.False(staleness.IsStale);

                    stats = store.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfTimeSeriesSegments);
                    Assert.Equal(1, stats.CountOfIndexes);
                }
            }
        }

        private class Employees_HeartBeat : AbstractTimeSeriesIndexCreationTask<Employee>
        {
            public Employees_HeartBeat()
            {
                AddMap("HeartBeat", timeseries => from ts in timeseries
                                                  from entry in ts.Entries
                                                  select new
                                                  {
                                                      Name = ts.Name,
                                                      Value = entry.Value
                                                  });
            }
        }
    }
}
