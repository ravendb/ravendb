using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Parquet;
using Parquet.Schema;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Server.Documents.ETL.Olap
{
    public class LocalTestsStress : RavenTestBase
    {
        internal const string DefaultFrequency = "* * * * *"; // every minute
        private const string AllFilesPattern = "*.*";


        public LocalTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task ShouldRespectEtlRunFrequency()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var o = new Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            Company = $"companies/{i}"
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

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
                SetupLocalOlapEtl(store, script, path, frequency: DefaultFrequency);

                Assert.True(await etlDone.WaitAsync(TimeSpan.FromSeconds(10)));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories).OrderBy(x => x).ToArray();
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = await ParquetReader.CreateAsync(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));

                        var data = (await rowGroupReader.ReadColumnAsync((DataField)field)).Data;
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

                await Task.Delay(100);

                baseline = new DateTime(2021, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 20; i <= 30; i++)
                    {
                        var o = new Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            Company = $"companies/{i}"
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var secondBatchCompleted = WaitForValue(() =>
                {
                    files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories).OrderBy(x => x).ToArray();
                    return files.Length == 2;
                }, expectedVal: true, timeout: 70_000);

                Assert.True(secondBatchCompleted, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(70)));

                var firstBatchTime = File.GetLastWriteTimeUtc(files[0]);
                var secondBatchTime = File.GetLastWriteTimeUtc(files[1]);

                // compute next minute boundary
                var expectedSecondBatchAt = new DateTime(firstBatchTime.Year, firstBatchTime.Month, firstBatchTime.Day, firstBatchTime.Hour, firstBatchTime.Minute, 0, DateTimeKind.Utc)
                    .AddMinutes(1);

                Assert.True(secondBatchTime >= expectedSecondBatchAt.AddMilliseconds(-250),
                    $"First batch time : {firstBatchTime}, second batch time : {secondBatchTime}. Files : {string.Join(", ", Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories))}");

            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task AfterDatabaseRestartEtlShouldRespectRunFrequency()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var o = new Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            Company = $"companies/{i}"
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

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

                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                // disable and re enable the database

                var result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
                Assert.True(result.Success);
                Assert.True(result.Disabled);

                result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
                Assert.True(result.Success);
                Assert.False(result.Disabled);

                var database = await WaitForDatabaseToUnlockAsync(store, timeout: TimeSpan.FromSeconds(10));
                Assert.NotNull(database);

                var etlDone2 = WaitForEtl(database, (n, statistics) => statistics.LoadSuccesses != 0);

                baseline = new DateTime(2021, 1, 1);

                // add more data
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 20; i <= 30; i++)
                    {
                        var o = new Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            Company = $"companies/{i}"
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                Assert.False(await etlDone2.WaitAsync(TimeSpan.FromSeconds(50)));
                files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                Assert.True(await etlDone2.WaitAsync(TimeSpan.FromSeconds(120)));

                var timeWaited = sw.Elapsed.TotalMilliseconds;
                sw.Stop();

                Assert.True(timeWaited > TimeSpan.FromSeconds(60).TotalMilliseconds);

                files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(2, files.Length);
            }
        }

        private static AsyncManualResetEvent WaitForEtl(DocumentDatabase database, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var amre = new AsyncManualResetEvent();

            database.EtlLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    amre.Set();
            };

            return amre;
        }

        private async Task<DocumentDatabase> WaitForDatabaseToUnlockAsync(IDocumentStore store, TimeSpan timeout)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            while (stopwatch.Elapsed < timeout)
            {
                try
                {
                    return await Databases.GetDocumentDatabaseInstanceFor(store);
                }
                catch (DatabaseDisabledException)
                {
                    await Task.Delay(10);
                }
            }

            return null;
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

            Etl.AddEtl(store, configuration, connectionString);
        }

    }
}
