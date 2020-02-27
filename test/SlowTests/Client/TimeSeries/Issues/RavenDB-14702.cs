using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
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
                        await session.StoreAsync(new { Name = "aviv" }, "zzz/1");
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenSession())
                    {
                        var d = new DateTime(1980, 1, 1);

                        var r = new Random();
                        var previous = 0.0;
                        var tsf = session.TimeSeriesFor("zzz/1");

                        for (var i = 0; i < 100_000; i++)
                        {
                            var nextDouble = previous * 0.9 + 0.1 * r.NextDouble();

                            tsf.Append("small", d, null, new double[] { nextDouble });
                            d = d.AddMinutes(1);

                            previous = nextDouble;
                        }

                        session.SaveChanges();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(376, stats.CountOfTimeSeriesSegments);

                    using (var session = store2.OpenSession())
                    {
                        var doc = session.Load<object>("zzz/1");
                        Assert.NotNull(doc);

                        var ts = session.TimeSeriesFor(doc).Get("small", DateTime.MinValue, DateTime.MaxValue).ToList();
                        Assert.Equal(100_000, ts.Count);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}
