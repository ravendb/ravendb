using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21427 : ReplicationTestBase
    {
        public RavenDB_21427(ITestOutputHelper output) : base(output)
        {
        }

        private const string RL_COMM = "RAVEN_LICENSE_COMMUNITY";
        private const string RL_DEV = "RAVEN_LICENSE_DEVELOPER";
        private const string RL_PRO = "RAVEN_LICENSE_PROFESSIONAL";

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Sharding)]
        public async Task Prevent_License_Downgrade_Multi_Node_Sharding()
        {
            DoNotReuseServer();

            var (_, leader) = await CreateRaftCluster(3, false, watcherCluster: true);
            var options = Options.ForMode(RavenDatabaseMode.Sharded);
            options.Server = leader;
            options.ReplicationFactor = 3;
            options.ModifyDatabaseRecord = r =>
            {
                r.Sharding ??= new ShardingConfiguration();
                r.Sharding.Shards = Enumerable.Range(0, 5)
                    .Select((shardNumber) => new KeyValuePair<int, DatabaseTopology>(shardNumber, new DatabaseTopology())).ToDictionary(x => x.Key, x => x.Value);
            };
            using (var store = GetDocumentStore(options))
            {
                await TryToChangeLicense(leader, RL_COMM, LimitType.Sharding);
                await TryToChangeLicense(leader, RL_DEV, LimitType.Sharding);
                await TryToChangeLicense(leader, RL_PRO, LimitType.Sharding);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_License_Downgrade_QueueSink()
        {
            DoNotReuseServer();
            using var store = GetDocumentStore();
            var res = store.Maintenance.Send(
                new PutConnectionStringOperation<QueueConnectionString>(
                    new QueueConnectionString
                    {
                        Name = "KafkaConStr",
                        BrokerType = QueueBrokerType.Kafka,
                        KafkaConnectionSettings = new KafkaConnectionSettings()
                            { BootstrapServers = "localhost:9092" }
                    }));

            // Define a Sink script
            QueueSinkScript queueSinkScript = new QueueSinkScript
            {
                // Script name
                Name = "orders",
                // A list of Kafka topics to connect
                Queues = new List<string>() { "orders" },
                // Apply this script
                Script = @"this['@metadata']['@collection'] = 'Orders';
               put(this.Id.toString(), this)"
            };

            // Define a Kafka configuration
            var config = new QueueSinkConfiguration()
            {
                // Sink name
                Name = "KafkaSinkTaskName",
                // The connection string to connect the broker with
                ConnectionStringName = "KafkaConStr",
                // What queue broker is this task using
                BrokerType = QueueBrokerType.Kafka,
                // The list of scripts to run
                Scripts = { queueSinkScript }
            };

            AddQueueSinkOperationResult addQueueSinkOperationResult =
                store.Maintenance.Send(new AddQueueSinkOperation<QueueConnectionString>(config));

            await TryToChangeLicense(Server, RL_COMM, LimitType.QueueSink);
            await TryToChangeLicense(Server, RL_PRO, LimitType.QueueSink);

            config = new QueueSinkConfiguration()
            {
                // Sink name
                Name = "KafkaSinkTaskName2",
                // The connection string to connect the broker with
                ConnectionStringName = "KafkaConStr",
                // What queue broker is this task using
                BrokerType = QueueBrokerType.RabbitMq,
                // The list of scripts to run
                Scripts = { queueSinkScript }
            };

            addQueueSinkOperationResult =
                store.Maintenance.Send(new AddQueueSinkOperation<QueueConnectionString>(config));

            await TryToChangeLicense(Server, RL_COMM, LimitType.QueueSink);
            await TryToChangeLicense(Server, RL_PRO, LimitType.QueueSink);
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ExpirationRefresh)]
        public async Task Prevent_License_Downgrade_DataArchival()
        {
            DoNotReuseServer();

            using var store = GetDocumentStore();
            var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100 };

            await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
            await TryToChangeLicense(Server, RL_COMM, LimitType.DataArchival);
            await TryToChangeLicense(Server, RL_PRO, LimitType.DataArchival);
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes)]
        public async Task Prevent_License_Downgrade_Static_Index_Count_Per_Database()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                for (int i = 0; i < 15; i++)
                {
                    store.Maintenance.Send(new PutIndexesOperation(new[]
                    {
                        new IndexDefinition { Maps = { "from doc in docs.Images select new { doc.Tags }" }, Name = "test" + i }
                    }));
                }

                await TryToChangeLicense(Server, RL_COMM, LimitType.Indexes);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes)]
        public async Task Prevent_License_Downgrade_Static_Index_Count_Per_Cluster()
        {
            DoNotReuseServer();
            var (_, leader) = await CreateRaftCluster(3, false, watcherCluster: true);
            var storeList = new List<DocumentStore>();
            for (int i = 0; i < 7; i++)
            {
                var store = GetDocumentStore();
                storeList.Add(store);
                for (int j = 0; j < 10; j++)
                {
                    store.Maintenance.Send(new PutIndexesOperation(new[]
                    {
                        new IndexDefinition { Maps = { "from doc in docs.Images select new { doc.Tags }" }, Name = "test" + j }
                    }));
                }
            }

            await TryToChangeLicense(Server, RL_COMM, LimitType.Indexes);
            foreach (var store in storeList)
            {
                store.Dispose();
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes)]
        public async Task Prevent_License_Downgrade_Auto_Index_Count_Per_Database()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                for (int j = 0; j < 28; j++)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    var index = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name" + j } }),
                        Guid.NewGuid().ToString());

                }

                await TryToChangeLicense(Server, RL_COMM, LimitType.Indexes);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes)]
        public async Task Prevent_License_Downgrade_Auto_Index_Count_Per_Cluster()
        {
            DoNotReuseServer();
            var (_, leader) = await CreateRaftCluster(3, false, watcherCluster: true);
            var storeList = new List<DocumentStore>();
            for (int i = 0; i < 7; i++)
            {
                var store = GetDocumentStore();
                storeList.Add(store);
                for (int j = 0; j < 20; j++)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    var index = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name" + j } }),
                        Guid.NewGuid().ToString());

                }
            }

            await TryToChangeLicense(Server, RL_COMM, LimitType.Indexes);
            foreach (var store in storeList)
            {
                store.Dispose();
            }

        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Revisions)]
        public async Task Prevent_License_Downgrade_Revision_Default_Configuration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 0 } };
                var result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                await TryToChangeLicense(Server, RL_COMM, LimitType.RevisionsConfiguration);

                configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        {
                            "Companies", new RevisionsCollectionConfiguration()
                            {
                                Disabled = false,
                                MinimumRevisionsToKeep = 5
                            }
                        }
                    }
                };

                result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                await TryToChangeLicense(Server, RL_COMM, LimitType.RevisionsConfiguration);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Prevent_License_Downgrade_PeriodicBackup()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *");
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                await TryToChangeLicense(Server, RL_COMM, LimitType.PeriodicBackup);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_Sorters_Per_Database()
        {
            DoNotReuseServer();
            var sorterName = GetDatabaseName();

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => sorterName,
                ModifyDatabaseRecord = record => record.Sorters = new Dictionary<string, SorterDefinition>
                       {
                           {
                               "MySorter",
                               new SorterDefinition { Name = sorterName + "1", Code = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName + "1") }
                           },
                           {
                               "MySorter2",
                               new SorterDefinition { Name = sorterName + "2", Code = GetSorter("RavenDB_8355.MySorter.cs", "MySorter2", sorterName + "2") }
                           }
                       }
            }))
            {
                await TryToChangeLicense(Server, RL_COMM, LimitType.CustomSorters);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_Sorters_Per_Cluster()
        {
            DoNotReuseServer();
            var (_, leader) = await CreateRaftCluster(3, false, watcherCluster: true);
            var sorterName = GetDatabaseName();

            var storeList = new List<DocumentStore>();
            for (int i = 0; i < 6; i++)
            {
                var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseRecord = record => record.Sorters = new Dictionary<string, SorterDefinition>
                    {
                        {
                            "MySorter", new SorterDefinition { Name = sorterName + "1", Code = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName + "1") }
                        }
                    }
                });

                storeList.Add(store);
            }

            await TryToChangeLicense(Server, RL_COMM, LimitType.CustomSorters);
            foreach (var store in storeList)
            {
                store.Dispose();
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_Analyzer_Per_Database()
        {
            DoNotReuseServer();
            var sorterName = GetDatabaseName();

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => sorterName,
                ModifyDatabaseRecord = record => record.Sorters = new Dictionary<string, SorterDefinition>
                       {
                           {
                               "MySorter",
                               new SorterDefinition { Name = sorterName + "1", Code = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName + "1") }
                           },
                           {
                               "MySorter2",
                               new SorterDefinition { Name = sorterName + "2", Code = GetSorter("RavenDB_8355.MySorter.cs", "MySorter2", sorterName + "2") }
                           }
                       }
            }))
            {
                await TryToChangeLicense(Server, RL_COMM, LimitType.CustomSorters);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_Analyzer_Per_Cluster()
        {
            DoNotReuseServer();
            var (_, leader) = await CreateRaftCluster(3, false, watcherCluster: true);
            var sorterName = GetDatabaseName();

            var storeList = new List<DocumentStore>();
            for (int i = 0; i < 6; i++)
            {
                var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseRecord = record => record.Sorters = new Dictionary<string, SorterDefinition>
                    {
                        {
                            "MySorter", new SorterDefinition { Name = sorterName + "1", Code = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName + "1") }
                        }
                    }
                });

                storeList.Add(store);
            }

            await TryToChangeLicense(Server, RL_COMM, LimitType.CustomSorters);
            foreach (var store in storeList)
            {
                store.Dispose();
            }

        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ClientApi)]
        public async Task Prevent_License_Downgrade_ClientConfiguration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = r => r.Client = new ClientConfiguration { MaxNumberOfRequestsPerSession = 50 } }))
            {
                await TryToChangeLicense(Server, RL_COMM, LimitType.ClientConfiguration);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_StudioConfiguration()
        {
            DoNotReuseServer();

            using var store = GetDocumentStore();
            var command = new PutDatabaseStudioConfigurationCommand(new ServerWideStudioConfiguration() { DisableAutoIndexCreation = true, }, store.Database,
                RaftIdGenerator.NewId());
            await Server.ServerStore.SendToLeaderAsync(command);

            await TryToChangeLicense(Server, RL_COMM, LimitType.StudioConfiguration);
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ExpirationRefresh)]
        public async Task Prevent_License_Downgrade_Expiration_Configuration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var config = new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 100, };

                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);
                await TryToChangeLicense(Server, RL_COMM, LimitType.Expiration);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ExpirationRefresh)]
        public async Task Prevent_License_Downgrade_Refresh_Configuration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var refConfig = new RefreshConfiguration { RefreshFrequencyInSec = 33, Disabled = false };
                await store.Maintenance.SendAsync(new ConfigureRefreshOperation(refConfig));
                await TryToChangeLicense(Server, RL_COMM, LimitType.Refresh);
            }
        }

        private static async Task TryToChangeLicense(RavenServer leader, string licenseType, LimitType limitType)
        {
            var license = Environment.GetEnvironmentVariable(licenseType);
            LicenseHelper.TryDeserializeLicense(license, out License li);

            var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await leader.ServerStore.PutLicenseAsync(li, RaftIdGenerator.NewId()));

            Assert.Equal(limitType, exception.Type);
        }


        private static string GetSorter(string resourceName, string originalSorterName, string sorterName)
        {
            using (var stream = GetDump(resourceName))
            using (var reader = new StreamReader(stream))
            {
                var analyzerCode = reader.ReadToEnd();
                analyzerCode = analyzerCode.Replace(originalSorterName, sorterName);

                return analyzerCode;
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_8355).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
