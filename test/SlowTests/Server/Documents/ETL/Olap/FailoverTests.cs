using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Documents.ETL;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Issues.RavenDB_17096;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class FailoverTests : ClusterTestBase
    {
        public FailoverTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task OlapTaskShouldBeHighlyAvailable()
        {
            var cluster = await CreateRaftCluster(3);
            var leader = cluster.Leader;
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
            var mentorNode = db.Servers.First(s => s != leader);
            var mentorTag = mentorNode.ServerStore.NodeTag;
            var store = stores.Single(s => s.Urls[0] == mentorNode.WebUrl);

            Assert.Equal(store.Database, dbName);

            var baseline = new DateTime(2020, 1, 1);

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 31; i++)
                {
                    await session.StoreAsync(new Order
                    {
                        Id = $"orders/{i}",
                        OrderedAt = baseline.AddDays(i),
                        ShipVia = $"shippers/{i}",
                        Company = $"companies/{i}"
                    });
                }

                await session.SaveChangesAsync();
            }

            var etlDone = await WaitForEtlAsync(mentorNode, dbName, (n, statistics) => statistics.LoadSuccesses != 0);

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
            var path = NewDataPath(forceCreateDir: true);

            var configuration = new OlapEtlConfiguration
            {
                Name = configName,
                ConnectionStringName = connectionStringName,
                RunFrequency = LocalTests.DefaultFrequency,
                Transforms =
                {
                    new Transformation
                    {
                        Name = transformationName,
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                },
                MentorNode = mentorTag
            };
            var task = Etl.AddEtl(store,
                configuration,
                new OlapConnectionString
                {
                    Name = connectionStringName,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = path
                    }
                });

            var timeout = TimeSpan.FromSeconds(65);
            Assert.True(await etlDone.WaitAsync(timeout), await GetPerformanceStats(mentorNode, dbName, timeout));

            var files = Directory.GetFiles(path, "*.*", SearchOption.AllDirectories);
            Assert.Equal(1, files.Length);

            await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(mentorTag));
            Assert.True(await mentorNode.ServerStore.WaitForState(RachisState.Passive, CancellationToken.None).WaitWithoutExceptionAsync(TimeSpan.FromSeconds(30)),
                $"Removed node {mentorTag} wasn't move to passive state ({mentorNode.ServerStore.Engine.CurrentCommittedState.State})");

            var store2 = stores.First(s => s != store);

            var newResponsible = WaitForNewResponsibleNode(store2, task.TaskId, OngoingTaskType.OlapEtl, mentorTag);
            var newResponsibleNode = cluster.Nodes.Single(s => s.ServerStore.NodeTag == newResponsible);
            etlDone = await WaitForEtlAsync(newResponsibleNode, dbName, (n, statistics) => statistics.LoadSuccesses != 0);

            using (var session = store2.OpenAsyncSession())
            {
                for (int i = 0; i < 28; i++)
                {
                    await session.StoreAsync(new Order
                    {
                        Id = $"orders/{i + 31}",
                        OrderedAt = baseline.AddMonths(1).AddDays(i),
                        ShipVia = $"shippers/{i + 31}",
                        Company = $"companies/{i + 31}"
                    });
                }

                await session.SaveChangesAsync();
            }

            Assert.True(await etlDone.WaitAsync(timeout), await GetPerformanceStats(newResponsibleNode, dbName, timeout));

            files = Directory.GetFiles(path, "*.*", SearchOption.AllDirectories);
            Assert.True(files.Length == 2, $"Expected 2 output files but got {files.Length}. " +
                                           $"files : '{string.Join(", ", files)}'. " +
                                           $"Mentor node : '{mentorTag}', new Responsible node : '{newResponsibleNode.ServerStore.NodeTag}'.");
        }


        private static async Task<AsyncManualResetEvent> WaitForEtlAsync(RavenServer server, string databaseName, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

            var mre = new AsyncManualResetEvent();

            database.EtlLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    mre.Set();
            };


            return mre;
        }


        private async Task<string> GetPerformanceStats(RavenServer server, string database, TimeSpan timeout)
        {
            var documentDatabase = await GetDatabase(server, database);
            var performanceStats = Etl.GetEtlPerformanceStatsForDatabase(documentDatabase);
            return $"olap etl to local machine did not finish in {timeout.TotalSeconds} seconds. stats : {performanceStats}";
        }
    }
}
