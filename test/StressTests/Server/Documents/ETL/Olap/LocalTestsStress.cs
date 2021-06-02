using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.OLAP;
using SlowTests.Server.Documents.ETL;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Documents.ETL.Olap
{
    public class LocalTestsStress : EtlTestBase
    {
        internal const string DefaultFrequency = "* * * * *"; // every minute
        private const string AllFilesPattern = "*.*";


        public LocalTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldRespectEtlRunFrequency()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            Company = $"companies/{i}"
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    Company : this.Company
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                var frequency = DateTime.UtcNow.Minute % 2 == 1
                    ? "1-59/2 * * * *" // every uneven minute
                    : "*/2 * * * *"; // every 2nd minute (even minutes)

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path, frequency: frequency);
                etlDone.Wait(TimeSpan.FromMinutes(1));
                var sw = new Stopwatch();
                sw.Start();

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));

                        var data = rowGroupReader.ReadColumn((DataField)field).Data;
                        Assert.True(data.Length == 10);

                        if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                            continue;

                        var count = 1;
                        foreach (var val in data)
                        {
                            switch (field.Name)
                            {
                                case ParquetTransformedItems.DefaultIdColumn:
                                    Assert.Equal($"orders/{count}", val);
                                    break;
                                case "Company":
                                    Assert.Equal($"companies/{count}", val);
                                    break;
                            }

                            count++;

                        }
                    }
                }

                await Task.Delay(1000);

                baseline = new DateTime(2021, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 20; i <= 30; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            Company = $"companies/{i}"
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                etlDone.Wait(TimeSpan.FromSeconds(120));
                var timeWaited = sw.Elapsed.TotalMilliseconds;
                sw.Stop();

                Assert.True(timeWaited > TimeSpan.FromSeconds(60).TotalMilliseconds);

                files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(2, files.Length);
            }
        }

        [Fact]
        public async Task AfterDatabaseRestartEtlShouldRespectRunFrequency()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            Company = $"companies/{i}"
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    Company : this.Company
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                var path = NewDataPath(forceCreateDir: true);
                var frequency = DateTime.UtcNow.Minute % 2 == 1
                    ? "1-59/2 * * * *" // every uneven minute
                    : "*/2 * * * *"; // every 2nd minute (even minutes)

                SetupLocalOlapEtl(store, script, path, frequency: frequency);
                var sw = new Stopwatch();
                sw.Start();

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                // disable and re enable the database

                var result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
                Assert.True(result.Success);
                Assert.True(result.Disabled);

                result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
                Assert.True(result.Success);
                Assert.False(result.Disabled);

                Assert.True(WaitForDatabaseToUnlock(store, timeout: TimeSpan.FromMilliseconds(1000), out var database));

                etlDone = WaitForEtl(database, (n, statistics) => statistics.LoadSuccesses != 0);

                baseline = new DateTime(2021, 1, 1);

                // add more data
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 20; i <= 30; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            Company = $"companies/{i}"
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                Assert.False(etlDone.Wait(TimeSpan.FromSeconds(50)));
                files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(120)));

                var timeWaited = sw.Elapsed.TotalMilliseconds;
                sw.Stop();

                Assert.True(timeWaited > TimeSpan.FromSeconds(60).TotalMilliseconds);

                files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(2, files.Length);
            }
        }

        private static ManualResetEventSlim WaitForEtl(DocumentDatabase database, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var mre = new ManualResetEventSlim();

            database.EtlLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    mre.Set();
            };

            return mre;
        }

        private bool WaitForDatabaseToUnlock(IDocumentStore store, TimeSpan timeout, out DocumentDatabase database)
        {
            database = null;
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            while (stopwatch.Elapsed < timeout)
            {
                try
                {
                    database = GetDocumentDatabaseInstanceFor(store).Result;
                    return true;
                }
                catch (AggregateException e)
                {
                    if (e.Message.Contains($"The database '{store.Database}' has been unloaded and locked"))
                    {
                        Thread.Sleep(10);
                        continue;
                    }

                    throw;
                }
            }

            return false;
        }

        private void SetupLocalOlapEtl(DocumentStore store, string script, string path, string name = "olap-test", string frequency = null, string transformationName = null)
        {
            var connectionStringName = $"{store.Database} to local";
            var configuration = new OlapEtlConfiguration
            {
                Name = name,
                ConnectionStringName = connectionStringName,
                RunFrequency = frequency ?? DefaultFrequency,
                Transforms =
                {
                    new Transformation
                    {
                        Name = transformationName ?? "MonthlyOrders",
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                }
            };

            SetupLocalOlapEtl(store, configuration, path, connectionStringName);
        }

        private void SetupLocalOlapEtl(DocumentStore store, OlapEtlConfiguration configuration, string path, string connectionStringName)
        {
            var connectionString = new OlapConnectionString
            {
                Name = connectionStringName,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            };

            AddEtl(store, configuration, connectionString);
        }

    }
}
