using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.Replication;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14702 : ReplicationTestBase
    {
        public RavenDB_14702(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanImportTimeSeries()
        {

            var file = GetTempFileName();

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "aviv" }, "zzz/1");
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenSession())
                    {
                        var d = new DateTime(1980, 1, 1);

                        var r = new Random();
                        var previous = 0.0;
                        var tsf = session.TimeSeriesFor("zzz/1", "small");

                        for (var i = 0; i < 4000; i++)
                        {
                            var nextDouble = previous * 0.9 + 0.1 * r.NextDouble();

                            tsf.Append(d, nextDouble);
                            d = d.AddMinutes(1);

                            previous = nextDouble;
                        }

                        session.SaveChanges();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        var doc = await session.LoadAsync<User>("zzz/1");
                        doc.Name = "Karmel";
                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var session = store2.OpenSession())
                    {
                        var doc = session.Load<User>("zzz/1");
                        Assert.NotNull(doc);

                        var ts = session.TimeSeriesFor(doc, "small").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                        Assert.Equal(4000, ts.Count);
                    }

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(16, stats.CountOfTimeSeriesSegments);
                    Assert.Equal(1, stats.DatabaseChangeVector.ToChangeVector().Length);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}
