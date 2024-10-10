using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Issues;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Backup
{
    public class ShardedSmugglerTests : RavenTestBase
    {
        public ShardedSmugglerTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task ManyDocsRegularToShardToRegular()
        {
            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();

            try
            {
                using (var store1 = GetDocumentStore())
                {
                    using (var session = store1.BulkInsert())
                    {
                        for (int i = 0; i < 12345; i++)
                        {
                            await session.StoreAsync(new User()
                            {
                                Name = "user/" + i
                            }, "users/" + i);
                        }
                    }
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store2 = Sharding.GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        operation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                        }, file2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        using (var store3 = GetDocumentStore())
                        {
                            operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                            {
                                OperateOnTypes = DatabaseItemType.Documents
                            }, file2);
                            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                            var detailedStats = store3.Maintenance.Send(new GetDetailedStatisticsOperation());
                            //doc
                            Assert.Equal(12345, detailedStats.CountOfDocuments);
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
                File.Delete(file2);
            }
        }
        
        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task RegularToShardToRegular()
        {
            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                {
                    await Sharding.Backup.InsertData(store1);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.CompareExchangeTombstones
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.Indexes
                                         | DatabaseItemType.LegacyAttachments
                                         | DatabaseItemType.LegacyAttachmentDeletions
                                         | DatabaseItemType.LegacyDocumentDeletions
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(20));

                    using (var store2 = Sharding.GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.CompareExchangeTombstones
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.Indexes
                                             | DatabaseItemType.LegacyAttachments
                                             | DatabaseItemType.LegacyAttachmentDeletions
                                             | DatabaseItemType.LegacyDocumentDeletions
                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        operation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.CompareExchangeTombstones
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.Indexes
                                             | DatabaseItemType.LegacyAttachments
                                             | DatabaseItemType.LegacyAttachmentDeletions
                                             | DatabaseItemType.LegacyDocumentDeletions
                        }, file2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        using (var store3 = GetDocumentStore())
                        {
                            operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                            {
                                OperateOnTypes = DatabaseItemType.Documents
                                                 | DatabaseItemType.TimeSeries
                                                 | DatabaseItemType.CounterGroups
                                                 | DatabaseItemType.Attachments
                                                 | DatabaseItemType.Tombstones
                                                 | DatabaseItemType.DatabaseRecord
                                                 | DatabaseItemType.Subscriptions
                                                 | DatabaseItemType.Identities
                                                 | DatabaseItemType.CompareExchange
                                                 | DatabaseItemType.CompareExchangeTombstones // todo check test after fix
                                                 | DatabaseItemType.RevisionDocuments
                                                 | DatabaseItemType.Indexes
                                                 | DatabaseItemType.LegacyAttachments // todo test
                                                 | DatabaseItemType.LegacyAttachmentDeletions // todo test
                                                 | DatabaseItemType.LegacyDocumentDeletions //todo test


                            }, file2);
                            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(20));

                            await Sharding.Backup.CheckData(store3);
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
                File.Delete(file2);
            }
        }

        [Fact(Skip = "For testing")]
        public async Task RegularToRegular()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                {
                    await Sharding.Backup.InsertData(store1);
                    WaitForUserToContinueTheTest(store1);
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.Indexes
                        // | DatabaseItemType.CompareExchangeTombstones

                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store2 = GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.Indexes
                            // | DatabaseItemType.CompareExchangeTombstones

                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                        WaitForUserToContinueTheTest(store2);
                        await Sharding.Backup.CheckData(store2);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanExportAndImportDatabaseRecordToShardDatabase()
        {
            var file = Path.GetTempFileName();
            var file2 = Path.GetTempFileName();
            var dummy = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            string privateKey;
            using (var pullReplicationCertificate =
                   CertificateHelper.CreateCertificate(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable))
            {
                privateKey = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Pfx));
            }
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_1",

                    ModifyDatabaseRecord = record =>
                    {
                        record.ConflictSolverConfig = new ConflictSolver
                        {
                            ResolveToLatest = false,
                            ResolveByCollection = new Dictionary<string, ScriptResolver>
                                {
                                    {
                                        "ConflictSolver", new ScriptResolver()
                                        {
                                            Script = "Script"
                                        }
                                    }
                                }
                        };
                        record.Sorters = new Dictionary<string, SorterDefinition>
                        {
                            {
                                "MySorter", new SorterDefinition
                                {
                                    Name = "MySorter",
                                    Code = GetCode("RavenDB_8355.MySorter.cs")
                                }
                            }
                        };
                        record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                        {
                            {
                                "MyAnalyzer", new AnalyzerDefinition
                                {
                                    Name = "MyAnalyzer",
                                    Code = GetCode("RavenDB_14939.MyAnalyzer.cs")
                                }
                            }
                        };
                    }
                }))
                using (var store2 = GetDocumentStore())
                using (var store3 = Sharding.GetDocumentStore())
                {
                    var config = Backup.CreateBackupConfiguration(backupPath: "FolderPath", fullBackupFrequency: "0 */1 * * *", incrementalBackupFrequency: "0 */6 * * *", mentorNode: "A", name: "Backup");

                    store1.Maintenance.Send(new UpdateExternalReplicationOperation(new ExternalReplication("tempDatabase", "ExternalReplication")
                    {
                        TaskId = 1,
                        Name = "External",
                        DelayReplicationFor = new TimeSpan(4),
                        Url = "http://127.0.0.1/",
                        Disabled = false
                    }));
                    store1.Maintenance.Send(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink()
                    {
                        Database = "sinkDatabase",
                        CertificatePassword = (string)null,
                        CertificateWithPrivateKey = privateKey,
                        TaskId = 2,
                        Name = "Sink",
                        HubName = "hub",
                        ConnectionStringName = "ConnectionName"
                    }));
                    store1.Maintenance.Send(new PutPullReplicationAsHubOperation(new PullReplicationDefinition()
                    {
                        TaskId = 3,
                        Name = "hub",
                        MentorNode = "A",
                        DelayReplicationFor = new TimeSpan(3),
                    }));

                    var result1 = store1.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                    {
                        Name = "ConnectionName",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                        Database = "Northwind",
                    }));
                    Assert.NotNull(result1.RaftCommandIndex);

                    var sqlConnectionString = new SqlConnectionString
                    {
                        Name = "connection",
                        ConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3" + $";Initial Catalog=SqlReplication-{store1.Database};",
                        FactoryName = "System.Data.SqlClient"
                    };

                    var result2 = store1.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                    Assert.NotNull(result2.RaftCommandIndex);

                    store1.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(new RavenEtlConfiguration()
                    {
                        AllowEtlOnNonEncryptedChannel = true,
                        ConnectionStringName = "ConnectionName",
                        MentorNode = "A",
                        Name = "Etl",
                        TaskId = 4,
                        TestMode = true,
                        Transforms = {
                             new Transformation
                             {
                                 Name = $"ETL : 1",
                                 Collections = new List<string>(new[] {"Users"}),
                                 Script = null,
                                 ApplyToAllDocuments = false,
                                 Disabled = false
                             }
                         }
                    }));

                    store1.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(new SqlEtlConfiguration()
                    {
                        AllowEtlOnNonEncryptedChannel = true,
                        ForceQueryRecompile = false,
                        ConnectionStringName = "connection",
                        SqlTables =
                             {
                                 new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id", InsertOnlyMode = false},
                                 new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId", InsertOnlyMode = false},
                             },
                        Name = "sql",
                        ParameterizeDeletes = false,
                        MentorNode = "A",
                        Transforms = {
                             new Transformation
                             {
                                 Name = $"ETL : 2",
                                 Collections = new List<string>(new[] {"Users"}),
                                 Script = "loadToOrders(this)",
                                 ApplyToAllDocuments = false,
                                 Disabled = false
                             }
                         }
                    }));
                    await store1.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.DatabaseRecord
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    , file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store3.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    , file2);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    , file2);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store2)).PeriodicBackupRunner;
                    var backups = periodicBackupRunner.PeriodicBackups;

                    Assert.Equal("Backup", backups.First().Configuration.Name);
                    Assert.Equal(true, backups.First().Configuration.IncrementalBackupFrequency.Equals("0 */6 * * *"));
                    Assert.Equal(true, backups.First().Configuration.FullBackupFrequency.Equals("0 */1 * * *"));
                    Assert.Equal(BackupType.Backup, backups.First().Configuration.BackupType);
                    Assert.Equal(true, backups.First().Configuration.Disabled);

                    var record = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database));

                    record.Settings.TryGetValue("Patching.MaxNumberOfCachedScripts", out string value);
                    Assert.Null(value);

                    Assert.NotNull(record.ConflictSolverConfig);
                    Assert.Equal(false, record.ConflictSolverConfig.ResolveToLatest);
                    Assert.Equal(1, record.ConflictSolverConfig.ResolveByCollection.Count);
                    Assert.Equal(true, record.ConflictSolverConfig.ResolveByCollection.TryGetValue("ConflictSolver", out ScriptResolver sr));
                    Assert.Equal("Script", sr.Script);

                    Assert.Equal(1, record.Sorters.Count);
                    Assert.Equal(true, record.Sorters.TryGetValue("MySorter", out SorterDefinition sd));
                    Assert.Equal("MySorter", sd.Name);
                    Assert.NotEmpty(sd.Code);

                    Assert.Equal(1, record.Analyzers.Count);
                    Assert.Equal(true, record.Analyzers.TryGetValue("MyAnalyzer", out AnalyzerDefinition ad));
                    Assert.Equal("MyAnalyzer", ad.Name);
                    Assert.NotEmpty(ad.Code);

                    Assert.Equal(1, record.ExternalReplications.Count);
                    Assert.Equal("tempDatabase", record.ExternalReplications[0].Database);
                    Assert.Equal(true, record.ExternalReplications[0].Disabled);

                    Assert.Equal(0, record.SinkPullReplications.Count);
                    Assert.Equal(0, record.HubPullReplications.Count);
              
                    Assert.Equal(1, record.RavenEtls.Count);
                    Assert.Equal("Etl", record.RavenEtls.First().Name);
                    Assert.Equal("ConnectionName", record.RavenEtls.First().ConnectionStringName);
                    Assert.Equal(true, record.RavenEtls.First().AllowEtlOnNonEncryptedChannel);
                    Assert.Equal(true, record.RavenEtls.First().Disabled);

                    Assert.Equal(1, record.SqlEtls.Count);
                    Assert.Equal("sql", record.SqlEtls.First().Name);
                    Assert.Equal(false, record.SqlEtls.First().ParameterizeDeletes);
                    Assert.Equal(false, record.SqlEtls.First().ForceQueryRecompile);
                    Assert.Equal("connection", record.SqlEtls.First().ConnectionStringName);
                    Assert.Equal(true, record.SqlEtls.First().AllowEtlOnNonEncryptedChannel);
                    Assert.Equal(true, record.SqlEtls.First().Disabled);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task PullReplicationCertificateExportAndImport()
        {
            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();
            var hubSettings = new ConcurrentDictionary<string, string>();
            var hubCertificates = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var hubCerts = Certificates.SetupServerAuthentication(hubSettings, certificates: hubCertificates);
            var hubDB = GetDatabaseName();
            var pullReplicationName = $"{hubDB}-pull";

            var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = hubSettings, RegisterForDisposal = true });

            var dummy = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var pullReplicationCertificate = CertificateHelper.CreateCertificate(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable);
            Assert.True(pullReplicationCertificate.HasPrivateKey);

            using (var hubStore = GetDocumentStore(new Options
            {
                ClientCertificate = hubCerts.ServerCertificate.Value,
                Server = hubServer,
                ModifyDatabaseName = _ => hubDB
            }))
            {
                await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(pullReplicationName)));
                await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(pullReplicationName,
                    new ReplicationHubAccess
                    {
                        Name = pullReplicationName,
                        CertificateBase64 = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Cert))
                    }));

                var operation = await hubStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                {
                    OperateOnTypes = DatabaseItemType.ReplicationHubCertificates
                                     | DatabaseItemType.DatabaseRecord

                }, file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));


                using (var shardStore = Sharding.GetDocumentStore(new Options
                {
                    ClientCertificate = hubCerts.ServerCertificate.Value,
                    Server = hubServer,
                    ModifyDatabaseName = _ => hubDB + "shard"
                }))
                {
                    operation = await shardStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.ReplicationHubCertificates | DatabaseItemType.DatabaseRecord

                    }, file);

                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await shardStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.ReplicationHubCertificates | DatabaseItemType.DatabaseRecord
                    }, file2);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store2 = GetDocumentStore(new Options
                    {
                        ClientCertificate = hubCerts.ServerCertificate.Value,
                        Server = hubServer,
                        ModifyDatabaseName = _ => hubDB + "shard2"
                    }))
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.ReplicationHubCertificates | DatabaseItemType.DatabaseRecord

                        }, file2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        var accesses = await store2.Maintenance.SendAsync(new GetReplicationHubAccessOperation(pullReplicationName));
                        Assert.NotEmpty(accesses[0].Certificate);
                    }
                }
            }
        }

        private static string GetCode(string name)
        {
            using (var stream = GetDump(name))
            using (var reader = new StreamReader(stream))
                return reader.ReadToEnd();
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_9912).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task RegularToShardToRegularEncrypted()
        {
            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();
            try
            {
                var operateOnTypes = DatabaseItemType.Documents
                                     | DatabaseItemType.TimeSeries
                                     | DatabaseItemType.CounterGroups
                                     | DatabaseItemType.Attachments
                                     | DatabaseItemType.Tombstones
                                     | DatabaseItemType.DatabaseRecord
                                     | DatabaseItemType.Subscriptions
                                     | DatabaseItemType.Identities
                                     | DatabaseItemType.CompareExchange
                                     | DatabaseItemType.CompareExchangeTombstones
                                     | DatabaseItemType.RevisionDocuments
                                     | DatabaseItemType.Indexes;

                using (var store1 = GetDocumentStore())
                {
                    await Sharding.Backup.InsertData(store1);
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                        OperateOnTypes = operateOnTypes
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store2 = Sharding.GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                            OperateOnTypes = operateOnTypes

                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        operation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                        {
                            EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                            OperateOnTypes = operateOnTypes
                        }, file2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        using (var store3 = GetDocumentStore())
                        {
                            operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                            {
                                EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                                OperateOnTypes = operateOnTypes

                            }, file2);
                            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                            await Sharding.Backup.CheckData(store3);
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
                File.Delete(file2);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task ShardExportReturnState()
        {
            var file = GetTempFileName();
            var file2 = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                {
                    await Sharding.Backup.InsertData(store1);
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.CompareExchangeTombstones
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.Indexes
                                         | DatabaseItemType.LegacyAttachments
                                         | DatabaseItemType.LegacyAttachmentDeletions
                                         | DatabaseItemType.LegacyDocumentDeletions


                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(20));

                    using (var store2 = Sharding.GetDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.CompareExchangeTombstones
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.Indexes
                                             | DatabaseItemType.LegacyAttachments
                                             | DatabaseItemType.LegacyAttachmentDeletions
                                             | DatabaseItemType.LegacyDocumentDeletions


                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                        operation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents
                                             | DatabaseItemType.TimeSeries
                                             | DatabaseItemType.CounterGroups
                                             | DatabaseItemType.Attachments
                                             | DatabaseItemType.Tombstones
                                             | DatabaseItemType.DatabaseRecord
                                             | DatabaseItemType.Subscriptions
                                             | DatabaseItemType.Identities
                                             | DatabaseItemType.CompareExchange
                                             | DatabaseItemType.CompareExchangeTombstones
                                             | DatabaseItemType.RevisionDocuments
                                             | DatabaseItemType.Indexes
                                             | DatabaseItemType.LegacyAttachments
                                             | DatabaseItemType.LegacyAttachmentDeletions
                                             | DatabaseItemType.LegacyDocumentDeletions
                        }, file2);

                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        OperationState op = null;
                        WaitForValue(() =>
                        {
                            op = store2.Maintenance.Send(new GetOperationStateOperation(operation.Id));
                            return op.Status == OperationStatus.Completed;
                        }, true);

                        var res = (ShardedSmugglerResult)op.Result;

                        var smugglerRes = new SmugglerResult();
                        foreach (var shardResult in res.Results)
                        {
                            smugglerRes.Documents.ReadCount += shardResult.Result.Documents.ReadCount;
                            smugglerRes.Tombstones.ReadCount += shardResult.Result.Tombstones.ReadCount;
                            smugglerRes.TimeSeries.ReadCount += shardResult.Result.TimeSeries.ReadCount;
                            smugglerRes.Counters.ReadCount += shardResult.Result.Counters.ReadCount;
                            smugglerRes.Documents.Attachments.ReadCount += shardResult.Result.Documents.Attachments.ReadCount;
                            smugglerRes.RevisionDocuments.ReadCount += shardResult.Result.RevisionDocuments.ReadCount;
                            smugglerRes.Subscriptions.ReadCount += shardResult.Result.Subscriptions.ReadCount;
                            smugglerRes.Identities.ReadCount += shardResult.Result.Identities.ReadCount;
                            smugglerRes.CompareExchange.ReadCount += shardResult.Result.CompareExchange.ReadCount;
                            smugglerRes.CompareExchangeTombstones.ReadCount += shardResult.Result.CompareExchangeTombstones.ReadCount;
                            smugglerRes.Indexes.ReadCount += shardResult.Result.Indexes.ReadCount;
                        }

                        Assert.Equal(5, smugglerRes.Documents.ReadCount);
                        Assert.Equal(1, smugglerRes.Tombstones.ReadCount);
                        Assert.Equal(2, smugglerRes.TimeSeries.ReadCount);
                        Assert.Equal(1, smugglerRes.Counters.ReadCount);
                        Assert.Equal(3, smugglerRes.Documents.Attachments.ReadCount);
                        Assert.Equal(21, smugglerRes.RevisionDocuments.ReadCount);
                        Assert.Equal(1, smugglerRes.Subscriptions.ReadCount);
                        Assert.Equal(1, smugglerRes.Identities.ReadCount);
                        Assert.Equal(3, smugglerRes.CompareExchange.ReadCount);
                        //Assert.Equal(0, smugglerRes.CompareExchangeTombstones.ReadCount);
                        Assert.Equal(1, smugglerRes.Indexes.ReadCount);
                    }
                }
            }
            finally
            {
                File.Delete(file);
                File.Delete(file2);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanImportAtomicGuardTombstonesToShardedDatabase()
        {
            //RavenDB-19201

            var file = GetTempFileName();
            using var store = GetDocumentStore();
            using var shardedStore = Sharding.GetDocumentStore();

            Cluster.WaitForFirstCompareExchangeTombstonesClean(Server);

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    await session.StoreAsync(new User(), $"users/{i}");
                }

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user = new User { Name = "Ayende" };
                await session.StoreAsync(user, "users/ayende");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                session.Delete("users/ayende");
                await session.SaveChangesAsync();
            }

            using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
#pragma warning disable CS0618
                var compareExchangeTombs = Server.ServerStore.Cluster.GetCompareExchangeTombstonesByKey(context, store.Database).ToList();
#pragma warning restore CS0618
                Assert.Equal(1, compareExchangeTombs.Count);
                Assert.Equal("rvn-atomic/users/ayende", compareExchangeTombs[0].Key.Key);
            }
            
            var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
            {
                OperateOnTypes = DatabaseItemType.Documents
                                 | DatabaseItemType.CompareExchange
                                 | DatabaseItemType.CompareExchangeTombstones

            }, file);

            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            operation = await shardedStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
            {
                OperateOnTypes = DatabaseItemType.Documents
                                 | DatabaseItemType.CompareExchange
                                 | DatabaseItemType.CompareExchangeTombstones
            }, file);

            var result = await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(10)) as SmugglerResult;

            Assert.NotNull(result);
            Assert.Equal(1, result.CompareExchangeTombstones.ReadCount);
            Assert.Equal(10, result.Documents.ReadCount);
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding | RavenTestCategory.Attachments)]
        public async Task CanImportUniqueAttachments()
        {
            await using var stream = typeof(ShardedSmugglerTests).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_19723.RavenDB_19723.ravendbdump");
            var operateOnTypes = DatabaseItemType.Documents
                                 | DatabaseItemType.Attachments;

            using (var store1 = Sharding.GetDocumentStore())
            {
                var operation = await store1.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                {
                    OperateOnTypes = operateOnTypes
                }, stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }
        }
    }
}
