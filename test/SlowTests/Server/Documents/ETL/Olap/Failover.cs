using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server;
using Raven.Server.Documents.ETL;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class Failover : ClusterTestBase
    {
        public Failover(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task OlapTaskShouldBeHighlyAvailable()
        {
            var nodes = await CreateRaftCluster(3, watcherCluster: true);
            var leader = nodes.Leader;
            var dbName = GetDatabaseName();
            var db = await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);

            var stores = db.Servers.Select(s => new DocumentStore
                {
                    Database = dbName,
                    Urls = new[] { s.WebUrl },
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                .ToList();

            var path = GetTempPath("Orders");
            try
            {
                var server = db.Servers.First(s => s != leader);
                var store = stores.Single(s => s.Urls[0] == server.WebUrl);

                Assert.Equal(store.Database, dbName);

                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 31; i++)
                    {
                        await session.StoreAsync(new Query.Order
                        {
                            Id = $"orders/{i}", OrderedAt = baseline.AddDays(i), ShipVia = $"shippers/{i}", Company = $"companies/{i}"
                        });
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(server, dbName, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key),
    {
        OrderId: id(this),
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";

                var connectionStringName = $"{store.Database} to S3";
                var configName = "olap-s3";
                var transformationName = "MonthlyOrders";
                
                var configuration = new OlapEtlConfiguration
                {
                    Name = configName,
                    ConnectionStringName = connectionStringName,
                    RunFrequency = LocalTests.DefaultFrequency,
                    Transforms = {new Transformation
                    {
                        Name = transformationName, 
                        Collections = new List<string> {"Orders"}, 
                        Script = script
                    }},
                    MentorNode = server.ServerStore.NodeTag
                };
                AddEtl(store,
                    configuration,
                    new OlapConnectionString
                    {
                        Name = connectionStringName, 
                        LocalSettings = new LocalSettings
                        {
                            FolderPath = path
                        }
                    });

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                var files = Directory.GetFiles(path);
                Assert.Equal(1, files.Length);

                DisposeServerAndWaitForFinishOfDisposal(server);

                var store2 = stores.First(s => s != store);
                using (var session = store2.OpenAsyncSession())
                {
                    for (int i = 0; i < 28; i++)
                    {
                        await session.StoreAsync(new Query.Order
                        {
                            Id = $"orders/{i + 31}",
                            OrderedAt = baseline.AddMonths(1).AddDays(i),
                            ShipVia = $"shippers/{i + 31}",
                            Company = $"companies/{i + 31}"
                        });
                    }

                    await session.SaveChangesAsync();
                }

                etlDone.Reset();
                etlDone.Wait(TimeSpan.FromMinutes(1));
                files = Directory.GetFiles(path);
                Assert.Equal(2, files.Length);
            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();

                foreach (var item in stores)
                {
                    item.Dispose();
                }
            }
        }
        private static string GetTempPath(string collection, [CallerMemberName] string caller = null)
        {
            var tmpPath = Path.GetTempPath();
            return Directory.CreateDirectory(Path.Combine(tmpPath, caller, collection)).FullName;
        }

        private ManualResetEventSlim WaitForEtl(RavenServer server, string databaseName, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var database = server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;

            var mre = new ManualResetEventSlim();

            database.EtlLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    mre.Set();
            };


            return mre;
        }

        private static AddEtlOperationResult AddEtl(IDocumentStore src, OlapEtlConfiguration configuration, OlapConnectionString connectionString)
        {
            var putResult = src.Maintenance.Send(new PutConnectionStringOperation<OlapConnectionString>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var addResult = src.Maintenance.Send(new AddEtlOperation<OlapConnectionString>(configuration));
            return addResult;
        }

    }
}
