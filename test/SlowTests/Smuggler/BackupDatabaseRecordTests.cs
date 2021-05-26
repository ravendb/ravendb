using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Smuggler.Migration;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Issues;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Smuggler
{
    public class BackupDatabaseRecordTests : RavenTestBase
    {
        public BackupDatabaseRecordTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportAndImportDatabaseRecord()
        {
            var file = Path.GetTempFileName();
            var dummy = GenerateAndSaveSelfSignedCertificate(createNew: true);
            string privateKey;
            using (var pullReplicationCertificate =
                new X509Certificate2(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable))
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
                                    Code = GetSorter("RavenDB_8355.MySorter.cs")
                                }
                            }
                        };
                    }

                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2"
                }))
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
                        TestMode = true
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
                        MentorNode = "A"
                    }));
                    await store1.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
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

                    Assert.Equal(1, record.ExternalReplications.Count);
                    Assert.Equal("tempDatabase", record.ExternalReplications[0].Database);
                    Assert.Equal(true, record.ExternalReplications[0].Disabled);

                    Assert.Equal(1, record.SinkPullReplications.Count);
                    Assert.Equal("sinkDatabase", record.SinkPullReplications[0].Database);
                    Assert.Equal("hub", record.SinkPullReplications[0].HubName);
                    Assert.Equal((string)null, record.SinkPullReplications[0].CertificatePassword);
                    Assert.Equal(privateKey, record.SinkPullReplications[0].CertificateWithPrivateKey);
                    Assert.Equal(true, record.SinkPullReplications[0].Disabled);

                    Assert.Equal(1, record.HubPullReplications.Count);
                    Assert.Equal(new TimeSpan(3), record.HubPullReplications.First().DelayReplicationFor);
                    Assert.Equal("hub", record.HubPullReplications.First().Name);
                    Assert.Equal(true, record.HubPullReplications.First().Disabled);

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

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanMigrateDatabaseRecord()
        {
            var file = Path.GetTempFileName();
            var dummy = GenerateAndSaveSelfSignedCertificate(createNew: true);
            string privateKey;
            using (var pullReplicationCertificate =
                new X509Certificate2(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable))
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
                                    Code = GetSorter("RavenDB_8355.MySorter.cs")
                                }
                            }
                        };
                    }

                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2"
                }))
                {
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
                        TestMode = true
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
                        MentorNode = "A"
                    }));
                    var migrate = new Migrator(new DatabasesMigrationConfiguration
                    {
                        ServerUrl = Server.WebUrl,
                        Databases = new List<DatabaseMigrationSettings>()
                        {
                            new DatabaseMigrationSettings
                            {
                                DatabaseName = store1.Database,
                                OperateOnTypes = DatabaseItemType.DatabaseRecord,
                                OperateOnDatabaseRecordTypes = DatabaseRecordItemType.Expiration |
                                                               DatabaseRecordItemType.ConflictSolverConfig |
                                                               DatabaseRecordItemType.Client |
                                                               DatabaseRecordItemType.ExternalReplications |
                                                               DatabaseRecordItemType.HubPullReplications |
                                                               DatabaseRecordItemType.SinkPullReplications |
                                                               DatabaseRecordItemType.Sorters |
                                                               DatabaseRecordItemType.RavenEtls |
                                                               DatabaseRecordItemType.SqlConnectionStrings |
                                                               DatabaseRecordItemType.SqlEtls |
                                                               DatabaseRecordItemType.RavenConnectionStrings
                            }
                        }
                    }, Server.ServerStore);
                    await migrate.UpdateBuildInfoIfNeeded();
                    var operationId = migrate.StartMigratingSingleDatabase(new DatabaseMigrationSettings
                    {
                        DatabaseName = store1.Database,
                        OperateOnTypes = DatabaseItemType.DatabaseRecord,
                        OperateOnDatabaseRecordTypes = DatabaseRecordItemType.Expiration | 
                                                       DatabaseRecordItemType.ConflictSolverConfig |
                                                       DatabaseRecordItemType.Client |
                                                       DatabaseRecordItemType.ExternalReplications |
                                                       DatabaseRecordItemType.HubPullReplications |
                                                       DatabaseRecordItemType.SinkPullReplications |
                                                       DatabaseRecordItemType.Sorters |
                                                       DatabaseRecordItemType.RavenEtls |
                                                       DatabaseRecordItemType.SqlConnectionStrings |
                                                       DatabaseRecordItemType.SqlEtls |
                                                       DatabaseRecordItemType.RavenConnectionStrings 
                    }, GetDocumentDatabaseInstanceFor(store2).Result);

                    WaitForValue(() =>
                    {
                        var Operation = store2.Maintenance.Send(new GetOperationStateOperation(operationId));
                        return Operation.Status == OperationStatus.Completed;
                    }, true);
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

                    Assert.Equal(1, record.ExternalReplications.Count);
                    Assert.Equal("tempDatabase", record.ExternalReplications[0].Database);
                    Assert.Equal(true, record.ExternalReplications[0].Disabled);

                    Assert.Equal(1, record.SinkPullReplications.Count);
                    Assert.Equal("sinkDatabase", record.SinkPullReplications[0].Database);
                    Assert.Equal("hub", record.SinkPullReplications[0].HubName);
                    Assert.Equal((string)null, record.SinkPullReplications[0].CertificatePassword);
                    Assert.Equal(privateKey, record.SinkPullReplications[0].CertificateWithPrivateKey);
                    Assert.Equal(true, record.SinkPullReplications[0].Disabled);

                    Assert.Equal(1, record.HubPullReplications.Count);
                    Assert.Equal(new TimeSpan(3), record.HubPullReplications.First().DelayReplicationFor);
                    Assert.Equal("hub", record.HubPullReplications.First().Name);
                    Assert.Equal(true, record.HubPullReplications.First().Disabled);

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

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportAndImportMergedDatabaseRecord()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_1",
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2",
                }))
                {
                    var collection = new Dictionary<string, ScriptResolver>
                    {
                        {
                            "ConflictSolver1", new ScriptResolver()
                            {
                                Script = "Script1"
                            }
                        },
                        {
                            "ConflictSolver2", new ScriptResolver()
                            {
                                Script = "Script2"
                            }
                        }
                    };
                    var collection2 = new Dictionary<string, ScriptResolver>
                    {
                        {
                            "ConflictSolver1", new ScriptResolver()
                            {
                                Script = "Script4"
                            }
                        },
                        {
                            "ConflictSolver3", new ScriptResolver()
                            {
                                Script = "Script3"
                            }
                        }
                    };
                    store1.Maintenance.Server.Send(new ModifyConflictSolverOperation(store1.Database, collection, false));
                    store2.Maintenance.Server.Send(new ModifyConflictSolverOperation(store2.Database, collection2, false));

                    var configuration = new ClientConfiguration
                    {
                        Etag = 10,
                        Disabled = false,
                        MaxNumberOfRequestsPerSession = 1024,
                        ReadBalanceBehavior = ReadBalanceBehavior.FastestNode
                    };
                    var configuration2 = new ClientConfiguration
                    {
                        Etag = 10,
                        Disabled = false,
                        MaxNumberOfRequestsPerSession = 512,
                        ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin
                    };
                    await store1.Maintenance.SendAsync(new PutClientConfigurationOperation(configuration));
                    await store2.Maintenance.SendAsync(new PutClientConfigurationOperation(configuration2));

                    var revisionConfig = new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            PurgeOnDelete = true
                        },
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            {"rev1", new RevisionsCollectionConfiguration
                                {
                                    Disabled = true,
                                    PurgeOnDelete = false,
                                    MinimumRevisionsToKeep = 10
                                }
                            },
                            {"rev2", new RevisionsCollectionConfiguration
                                {
                                    Disabled = true,
                                    PurgeOnDelete = false,
                                    MinimumRevisionsToKeep = 20
                                }

                            }
                        }

                    };
                    var revisionConfig2 = new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Disabled = true,
                            PurgeOnDelete = false
                        },
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            {"rev1", new RevisionsCollectionConfiguration
                                {
                                    Disabled = true,
                                    PurgeOnDelete = false,
                                    MinimumRevisionsToKeep = 20
                                }
                            },
                            {"rev3", new RevisionsCollectionConfiguration
                                {
                                    Disabled = true,
                                    PurgeOnDelete = false,
                                    MinimumRevisionsToKeep = 20
                                }

                            }
                        }

                    };
                    await store1.Maintenance.SendAsync(new ConfigureRevisionsOperation(revisionConfig));
                    await store2.Maintenance.SendAsync(new ConfigureRevisionsOperation(revisionConfig2));

                    var exConfig = new ExpirationConfiguration
                    {
                        DeleteFrequencyInSec = 60,
                        Disabled = false
                    };
                    var exConfig2 = new ExpirationConfiguration
                    {
                        DeleteFrequencyInSec = 30,
                        Disabled = true
                    };
                    await store1.Maintenance.SendAsync(new ConfigureExpirationOperation(exConfig));
                    await store2.Maintenance.SendAsync(new ConfigureExpirationOperation(exConfig2));

                    var hub1 = new PullReplicationDefinition
                    {
                        Name = "hub1",
                        DelayReplicationFor = new TimeSpan(3),
                    };
                    var hub2 = new PullReplicationDefinition
                    {
                        Name = "hub2",
                        DelayReplicationFor = new TimeSpan(3),
                    };
                    var hub3 = new PullReplicationDefinition
                    {
                        Name = "hub1",
                        DelayReplicationFor = new TimeSpan(5),
                    };
                    var hub4 = new PullReplicationDefinition
                    {
                        Name = "hub4",
                        DelayReplicationFor = new TimeSpan(3),
                    };
                    await store1.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(hub1));
                    await store1.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(hub2));
                    await store2.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(hub3));
                    await store2.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(hub4));

                    var con1 = new RavenConnectionString
                    {
                        Database = "db1",
                        Name = "con1",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8081" }
                    };
                    var con2 = new RavenConnectionString
                    {
                        Database = "db2",
                        Name = "con2",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8082" }
                    };
                    var con3 = new RavenConnectionString
                    {
                        Database = "db3",
                        Name = "con3",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8083" }
                    };
                    var con4 = new RavenConnectionString
                    {
                        Database = "db4",
                        Name = "con4",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8084" }
                    };
                    var result1 = await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(con1));
                    var result2 = await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(con2));
                    var result3 = await store2.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(con3));
                    var result4 = await store2.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(con4));
                    Assert.NotNull(result1.RaftCommandIndex);
                    Assert.NotNull(result2.RaftCommandIndex);
                    Assert.NotNull(result3.RaftCommandIndex);
                    Assert.NotNull(result4.RaftCommandIndex);

                    var sink1 = new PullReplicationAsSink()
                    {
                        Name = "sink1",
                        ConnectionString = con1,
                        ConnectionStringName = "con1",
                        Database = "db1",
                        HubName = "hub1"
                    };
                    var sink2 = new PullReplicationAsSink()
                    {
                        Name = "sink2",
                        ConnectionString = con2,
                        ConnectionStringName = "con2",
                        Database = "db2",
                        HubName = "hub2"
                    };
                    var sink3 = new PullReplicationAsSink()
                    {
                        Name = "sink1",
                        ConnectionString = con3,
                        ConnectionStringName = "con3",
                        Database = "db3",
                        HubName = "hub3"
                    };
                    var sink4 = new PullReplicationAsSink()
                    {
                        Name = "sink4",
                        ConnectionString = con4,
                        ConnectionStringName = "con4",
                        Database = "db4",
                        HubName = "hub4"
                    };
                    await store1.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sink1));
                    await store1.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sink2));
                    await store2.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sink3));
                    await store2.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sink4));

                    var external = new ExternalReplication
                    {
                        ConnectionString = con1,
                        ConnectionStringName = "con1",
                        Database = "db1",
                        DelayReplicationFor = new TimeSpan(1),
                        Name = "external1",
                        Url = "http://127.0.0.1:8081"
                    };
                    var external2 = new ExternalReplication
                    {
                        ConnectionString = con2,
                        ConnectionStringName = "con2",
                        Database = "db2",
                        DelayReplicationFor = new TimeSpan(2),
                        Name = "external2",
                        Url = "http://127.0.0.1:8081"
                    };
                    var external3 = new ExternalReplication
                    {
                        ConnectionString = con3,
                        ConnectionStringName = "con3",
                        Database = "db3",
                        DelayReplicationFor = new TimeSpan(3),
                        Name = "external1",
                        Url = "http://127.0.0.1:8083"
                    };
                    var external4 = new ExternalReplication
                    {
                        ConnectionString = con4,
                        ConnectionStringName = "con4",
                        Database = "db4",
                        DelayReplicationFor = new TimeSpan(4),
                        Name = "external4",
                        Url = "http://127.0.0.1:8084"
                    };
                    await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));
                    await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external2));
                    await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external3));
                    await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external4));

                    var etlConfiguration = new RavenEtlConfiguration
                    {
                        ConnectionStringName = "con1",
                        Name = "etl1",
                        AllowEtlOnNonEncryptedChannel = true
                    };
                    var etlConfiguration2 = new RavenEtlConfiguration
                    {
                        ConnectionStringName = "con2",
                        Name = "etl2",
                        AllowEtlOnNonEncryptedChannel = true
                    };
                    var etlConfiguration3 = new RavenEtlConfiguration
                    {
                        ConnectionStringName = "con3",
                        Name = "etl1",
                        AllowEtlOnNonEncryptedChannel = false
                    };
                    var etlConfiguration4 = new RavenEtlConfiguration
                    {
                        ConnectionStringName = "con4",
                        Name = "etl4",
                        AllowEtlOnNonEncryptedChannel = true
                    };
                    WaitForUserToContinueTheTest(store1);
                    await store1.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfiguration));
                    await store1.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfiguration2));
                    await store2.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfiguration3));
                    await store2.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfiguration4));

                    var scon1 = new SqlConnectionString()
                    {
                        Name = "scon1",
                        ConnectionString = "http://127.0.0.1:8081",
                        FactoryName = "System.Data.SqlClient"
                    };
                    var scon2 = new SqlConnectionString()
                    {
                        Name = "scon2",
                        ConnectionString = "http://127.0.0.1:8082",
                        FactoryName = "System.Data.SqlClient"
                    };
                    var scon3 = new SqlConnectionString()
                    {
                        Name = "scon3",
                        ConnectionString = "http://127.0.0.1:8083",
                        FactoryName = "System.Data.SqlClient"
                    };
                    var scon4 = new SqlConnectionString()
                    {
                        Name = "scon4",
                        ConnectionString = "http://127.0.0.1:8084",
                        FactoryName = "System.Data.SqlClient"
                    };
                    var putResult1 = await store1.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(scon1));
                    var putResult2 = await store1.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(scon2));
                    var putResult3 = await store2.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(scon3));
                    var putResult4 = await store2.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(scon4));
                    Assert.NotNull(putResult1.RaftCommandIndex);
                    Assert.NotNull(putResult2.RaftCommandIndex);
                    Assert.NotNull(putResult3.RaftCommandIndex);
                    Assert.NotNull(putResult4.RaftCommandIndex);

                    var sqlEtl =new SqlEtlConfiguration
                    {
                        ConnectionStringName = "scon1",
                        Name = "setl1",
                        AllowEtlOnNonEncryptedChannel = true,
                        SqlTables =
                        {
                            new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id"},
                            new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId"},
                            new SqlEtlTable {TableName = "NotUsedInScript", DocumentIdColumn = "OrderId"},
                        }
                    };
                    var sqlEtl2 = new SqlEtlConfiguration
                    {
                        ConnectionStringName = "scon2",
                        Name = "setl2",
                        AllowEtlOnNonEncryptedChannel = true,
                        SqlTables =
                        {
                            new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id"},
                            new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId"},
                            new SqlEtlTable {TableName = "NotUsedInScript", DocumentIdColumn = "OrderId"},
                        }
                    };
                    var sqlEtl3 = new SqlEtlConfiguration
                    {
                        ConnectionStringName = "scon3",
                        Name = "setl1",
                        AllowEtlOnNonEncryptedChannel = true,
                        SqlTables =
                        {
                            new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id"},
                            new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId"},
                            new SqlEtlTable {TableName = "NotUsedInScript", DocumentIdColumn = "OrderId"},
                        }
                    };
                    var sqlEtl4 = new SqlEtlConfiguration
                    {
                        ConnectionStringName = "scon4",
                        Name = "setl4",
                        AllowEtlOnNonEncryptedChannel = true,
                        SqlTables =
                        {
                            new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id"},
                            new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId"},
                            new SqlEtlTable {TableName = "NotUsedInScript", DocumentIdColumn = "OrderId"},
                        }
                    };
                    await store1.Maintenance.SendAsync(new AddEtlOperation<SqlConnectionString>(sqlEtl));
                    await store1.Maintenance.SendAsync(new AddEtlOperation<SqlConnectionString>(sqlEtl2));
                    await store2.Maintenance.SendAsync(new AddEtlOperation<SqlConnectionString>(sqlEtl3));
                    await store2.Maintenance.SendAsync(new AddEtlOperation<SqlConnectionString>(sqlEtl4));

                    var config = Backup.CreateBackupConfiguration(backupPath: "FolderPath", fullBackupFrequency: "0 1 * * *", incrementalBackupFrequency: "0 6 * * *", mentorNode: "A", name: "Backup");
                    var config2 = Backup.CreateBackupConfiguration(backupPath: "FolderPath", fullBackupFrequency: "0 1 * * *", incrementalBackupFrequency: "0 6 * * *", mentorNode: "A", name: "Backup2");
                    var config3 = Backup.CreateBackupConfiguration(backupPath: "FolderPath", backupType: BackupType.Snapshot, fullBackupFrequency: "0 8 * * *", incrementalBackupFrequency: "0 6 * * *", mentorNode: "A", name: "Backup");
                    var config4 = Backup.CreateBackupConfiguration(backupPath: "FolderPath", fullBackupFrequency: "0 1 * * *", incrementalBackupFrequency: "0 6 * * *", mentorNode: "A", name: "Backup4");
                  
                    await store1.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                    await store1.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config2));
                    await store2.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config3));
                    await store2.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config4));

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    WaitForUserToContinueTheTest(store2);

                    int disabled = 0;

                    var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store2)).PeriodicBackupRunner;
                    var backups = periodicBackupRunner.PeriodicBackups;

                    disabled = 0;
                    Assert.Equal(3, backups.Count);
                    Assert.Equal(true, backups.Any(x => x.Configuration.Name.Equals("Backup")));
                    foreach (var backup in backups)
                    {
                        if (backup.Configuration.Disabled)
                            disabled++;
                        if (!backup.Configuration.Name.Equals("Backup")) continue;
                        Assert.Equal(true, backup.Configuration.IncrementalBackupFrequency.Equals("0 6 * * *"));
                        Assert.Equal(true, backup.Configuration.FullBackupFrequency.Equals("0 1 * * *"));
                        Assert.Equal(BackupType.Backup, backup.Configuration.BackupType);
                    }
                    Assert.Equal(2, disabled);

                    

                    var record = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database));

                    Assert.Equal(3, record.ConflictSolverConfig.ResolveByCollection.Count);
                    Assert.Equal(false, record.ConflictSolverConfig.ResolveToLatest);
                    Assert.Equal(true, record.ConflictSolverConfig.ResolveByCollection.TryGetValue("ConflictSolver1", out ScriptResolver sr));
                    Assert.Equal("Script1", sr.Script);

                    Assert.Equal(1024, record.Client.MaxNumberOfRequestsPerSession);
                    Assert.Equal(ReadBalanceBehavior.FastestNode, record.Client.ReadBalanceBehavior);

                    Assert.Equal(true, record.Revisions.Default.PurgeOnDelete);
                    Assert.Equal(true, record.Revisions.Collections.TryGetValue("rev1", out RevisionsCollectionConfiguration rcc));
                    Assert.Equal(10, rcc.MinimumRevisionsToKeep);

                    Assert.Equal(60, record.Expiration.DeleteFrequencyInSec);

                    disabled = 0;
                    Assert.Equal(3, record.HubPullReplications.Count);
                    Assert.Equal(true, record.HubPullReplications.Any(x => x.Name.Equals("hub1")));
                    record.HubPullReplications.ForEach(x =>
                    {
                        if (x.Disabled)
                            disabled++;
                        if (!x.Name.Equals("hub1"))
                            return;
                        Assert.Equal(new TimeSpan(3), x.DelayReplicationFor);
                    });
                    Assert.Equal(2, disabled);

                    disabled = 0;
                    Assert.Equal(3, record.SinkPullReplications.Count);
                    Assert.Equal(true, record.SinkPullReplications.Any(x => x.Name.Equals("sink1")));
                    record.SinkPullReplications.ForEach(x =>
                    {
                        if (x.Disabled)
                            disabled++;
                        if (!x.Name.Equals("sink1"))
                            return;
                        Assert.Equal("hub1", x.HubName);
                        Assert.Equal("con1", x.ConnectionStringName);
                    });
                    Assert.Equal(2, disabled);

                    disabled = 0;
                    Assert.Equal(3, record.ExternalReplications.Count);
                    Assert.Equal(true, record.ExternalReplications.Any(x => x.Name.Equals("external1")));
                    record.ExternalReplications.ForEach(x =>
                    {
                        if (x.Disabled)
                            disabled++;
                        if (!x.Name.Equals("external1"))
                            return;
                        Assert.Equal("db1", x.Database);
                        Assert.Equal("con1", x.ConnectionStringName);
                    });
                    Assert.Equal(2, disabled);

                    disabled = 0;
                    Assert.Equal(3, record.RavenEtls.Count);
                    Assert.Equal(true, record.RavenEtls.Any(x => x.Name.Equals("etl1")));
                    record.RavenEtls.ForEach(x =>
                    {
                        if (x.Disabled)
                            disabled++;
                        if (!x.Name.Equals("etl1"))
                            return;
                        Assert.Equal("con1", x.ConnectionStringName);
                        Assert.Equal(true, x.AllowEtlOnNonEncryptedChannel);
                    });
                    Assert.Equal(2, disabled);

                    disabled = 0;
                    Assert.Equal(3, record.SqlEtls.Count);
                    Assert.Equal(true, record.SqlEtls.Any(x => x.Name.Equals("setl1")));
                    record.SqlEtls.ForEach(x =>
                    {
                        if (x.Disabled)
                            disabled++;
                        if (!x.Name.Equals("setl1"))
                            return;
                        Assert.Equal("scon1", x.ConnectionStringName);
                        Assert.Equal(true, x.AllowEtlOnNonEncryptedChannel);
                    });
                    Assert.Equal(2, disabled);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanBackupAndRestoreDatabaseRecord()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var dummy = GenerateAndSaveSelfSignedCertificate(createNew: true);
            string privateKey;
            using (var pullReplicationCertificate =
                new X509Certificate2(dummy.ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable))
            {
                privateKey = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Pfx));
            }

                using (var store = GetDocumentStore(new Options
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
                                    Code = GetSorter("RavenDB_8355.MySorter.cs")
                                }
                            }
                        };
                    }
                }))
                {
                    store.Maintenance.Send(new UpdateExternalReplicationOperation(new ExternalReplication("tempDatabase", "ExternalReplication")
                    {
                        TaskId = 1,
                        Name = "External",
                        DelayReplicationFor = new TimeSpan(4),
                        Url = "http://127.0.0.1/",
                        Disabled = false
                    }));
                    store.Maintenance.Send(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink()
                    {
                        Database = "sinkDatabase",
                        CertificatePassword = (string)null,
                        CertificateWithPrivateKey = privateKey,
                        TaskId = 2,
                        Name = "Sink",
                        HubName = "hub",
                        ConnectionStringName = "ConnectionName"
                    }));
                    store.Maintenance.Send(new PutPullReplicationAsHubOperation(new PullReplicationDefinition()
                    {
                        TaskId = 3,
                        Name = "hub",
                        MentorNode = "A",
                        DelayReplicationFor = new TimeSpan(3),
                    }));

                    var result1 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                    {
                        Name = "ConnectionName",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                        Database = "Northwind",
                    }));
                    Assert.NotNull(result1.RaftCommandIndex);

                    var sqlConnectionString = new SqlConnectionString
                    {
                        Name = "connection",
                        ConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3" + $";Initial Catalog=SqlReplication-{store.Database};",
                        FactoryName = "System.Data.SqlClient"
                    };

                    var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                    Assert.NotNull(result2.RaftCommandIndex);
                    store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(new RavenEtlConfiguration()
                    {
                        AllowEtlOnNonEncryptedChannel = true,
                        ConnectionStringName = "ConnectionName",
                        MentorNode = "A",
                        Name = "Etl",
                        TaskId = 4,
                        TestMode = true
                    }));

                    store.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(new SqlEtlConfiguration()
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
                        MentorNode = "A"
                    }));

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "oren"
                        }, "users/1");
                        await session.SaveChangesAsync();
                    }
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 */5 * * *", name: "Real");
                var config2 = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 */1 * * *", incrementalBackupFrequency: "0 */6 * * *", mentorNode: "A", name: "Backup");

                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config2));
                Backup.UpdateConfigAndRunBackup(Server, config, store);
                    
                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = databaseName,
                    }))
                    {
                        var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;
                        var backups = periodicBackupRunner.PeriodicBackups;

                        Assert.Equal(2, backups.Count);
                        Assert.Equal(true, backups.Any(x => x.Configuration.Name.Equals("Backup")));
                        foreach (var backup in backups)
                        {
                            if (!backup.Configuration.Name.Equals("Backup"))
                                continue;
                            Assert.Equal(true, backup.Configuration.IncrementalBackupFrequency.Equals("0 */6 * * *"));
                            Assert.Equal(true, backup.Configuration.FullBackupFrequency.Equals("0 */1 * * *"));
                            Assert.Equal(BackupType.Backup, backup.Configuration.BackupType);
                            Assert.Equal(false, backup.Configuration.Disabled);
                        }

                        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                        Assert.NotNull(record.ConflictSolverConfig);
                        Assert.Equal(false, record.ConflictSolverConfig.ResolveToLatest);
                        Assert.Equal(1, record.ConflictSolverConfig.ResolveByCollection.Count);
                        Assert.Equal(true, record.ConflictSolverConfig.ResolveByCollection.TryGetValue("ConflictSolver", out ScriptResolver sr));
                        Assert.Equal("Script", sr.Script);

                        Assert.Equal(1, record.Sorters.Count);
                        Assert.Equal(true, record.Sorters.TryGetValue("MySorter", out SorterDefinition sd));
                        Assert.Equal("MySorter", sd.Name);
                        Assert.NotEmpty(sd.Code);

                        Assert.Equal(1, record.ExternalReplications.Count);
                        Assert.Equal("tempDatabase", record.ExternalReplications[0].Database);
                        Assert.Equal(false, record.ExternalReplications[0].Disabled);

                        Assert.Equal(1, record.SinkPullReplications.Count);
                        Assert.Equal("sinkDatabase", record.SinkPullReplications[0].Database);
                        Assert.Equal("hub", record.SinkPullReplications[0].HubName);
                        Assert.Equal((string)null, record.SinkPullReplications[0].CertificatePassword);
                        Assert.Equal(privateKey, record.SinkPullReplications[0].CertificateWithPrivateKey);
                        Assert.Equal(false, record.SinkPullReplications[0].Disabled);

                        Assert.Equal(1, record.HubPullReplications.Count);
                        Assert.Equal(new TimeSpan(3), record.HubPullReplications.First().DelayReplicationFor);
                        Assert.Equal("hub", record.HubPullReplications.First().Name);
                        Assert.Equal(false, record.HubPullReplications.First().Disabled);

                        Assert.Equal(1, record.RavenEtls.Count);
                        Assert.Equal("Etl", record.RavenEtls.First().Name);
                        Assert.Equal("ConnectionName", record.RavenEtls.First().ConnectionStringName);
                        Assert.Equal(true, record.RavenEtls.First().AllowEtlOnNonEncryptedChannel);
                        Assert.Equal(false, record.RavenEtls.First().Disabled);

                        Assert.Equal(1, record.SqlEtls.Count);
                        Assert.Equal("sql", record.SqlEtls.First().Name);
                        Assert.Equal(false, record.SqlEtls.First().ParameterizeDeletes);
                        Assert.Equal(false, record.SqlEtls.First().ForceQueryRecompile);
                        Assert.Equal("connection", record.SqlEtls.First().ConnectionStringName);
                        Assert.Equal(true, record.SqlEtls.First().AllowEtlOnNonEncryptedChannel);
                        Assert.Equal(false, record.SqlEtls.First().Disabled);
                    }
                }
            }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanRestoreSubscriptionsFromBackup()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                store.Subscriptions.Create<User>(x => x.Name == "Marcin");
                store.Subscriptions.Create<User>();

                var config = Backup.CreateBackupConfiguration(backupPath);
                Backup.UpdateConfigAndRunBackup(Server, config, store);

                await ValidateSubscriptions(store);

                // restore the database with a different name
                var restoredDatabaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = restoredDatabaseName
                }))
                {
                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    })
                    {
                        restoredStore.Initialize();
                        var subscriptions = restoredStore.Subscriptions.GetSubscriptions(0, 10);

                        Assert.Equal(2, subscriptions.Count);

                        var taskIds = subscriptions
                            .Select(x => x.SubscriptionId)
                            .ToHashSet();

                        Assert.Equal(2, taskIds.Count);

                        foreach (var subscription in subscriptions)
                        {
                            Assert.NotNull(subscription.SubscriptionName);
                            Assert.NotNull(subscription.Query);
                        }

                        await ValidateSubscriptions(restoredStore);
                    }
                }
            }

            using (var store = GetDocumentStore())
            {
                var dir = Directory.GetDirectories(backupPath).First();
                var file = Directory.GetFiles(dir).First();

                var op = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                op.WaitForCompletion(TimeSpan.FromSeconds(30));

                var subscriptions = store.Subscriptions.GetSubscriptions(0, 10);

                Assert.Equal(2, subscriptions.Count);

                var taskIds = subscriptions
                    .Select(x => x.SubscriptionId)
                    .ToHashSet();

                Assert.Equal(2, taskIds.Count);

                foreach (var subscription in subscriptions)
                {
                    Assert.NotNull(subscription.SubscriptionName);
                    Assert.NotNull(subscription.Query);
                }

                await ValidateSubscriptions(store);
            }
        }

        [Theory, Trait("Category", "Smuggler")]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanDisableTasksAfterRestore(bool disableOngoingTasks)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                // etl
                store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = store.Database, TopologyDiscoveryUrls = new[] {"http://127.0.0.1:8080"}, Database = "Northwind",
                }));

                var etlConfiguration = new RavenEtlConfiguration
                {
                    ConnectionStringName = store.Database,
                    Transforms = {new Transformation() {Name = "loadAll", Collections = {"Users"}, Script = "loadToUsers(this)"}}
                };
                await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfiguration));

                // external replication
                var connectionString = new RavenConnectionString
                {
                    Name = store.Database, Database = store.Database, TopologyDiscoveryUrls = new[] {"http://127.0.0.1:12345"}
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication(store.Database, store.Database)));

                // pull replication sink
                var sink = new PullReplicationAsSink {HubName = "aa", ConnectionString = connectionString, ConnectionStringName = connectionString.Name};
                await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sink));

                // pull replication hub
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("test"));

                // backup
                var config = Backup.CreateBackupConfiguration(backupPath);
                Backup.UpdateConfigAndRunBackup(Server, config, store);

                // restore the database with a different name
                var restoredDatabaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store,
                    new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName,
                        DisableOngoingTasks = disableOngoingTasks
                    }))
                {
                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        var databaseRecord = await restoredStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredStore.Database));

                        var tasksCount = 0;
                        foreach (var task in databaseRecord.ExternalReplications)
                        {
                            Assert.Equal(disableOngoingTasks, task.Disabled);
                            tasksCount++;
                        }

                        foreach (var task in databaseRecord.RavenEtls)
                        {
                            Assert.Equal(disableOngoingTasks, task.Disabled);
                            tasksCount++;
                        }

                        foreach (var task in databaseRecord.PeriodicBackups)
                        {
                            Assert.Equal(disableOngoingTasks, task.Disabled);
                            tasksCount++;
                        }

                        foreach (var task in databaseRecord.ExternalReplications)
                        {
                            Assert.Equal(disableOngoingTasks, task.Disabled);
                            tasksCount++;
                        }

                        foreach (var task in databaseRecord.HubPullReplications)
                        {
                            Assert.Equal(disableOngoingTasks, task.Disabled);
                            tasksCount++;
                        }

                        foreach (var task in databaseRecord.SinkPullReplications)
                        {
                            Assert.Equal(disableOngoingTasks, task.Disabled);
                            tasksCount++;
                        }

                        Assert.Equal(6, tasksCount);
                    }
                }
            }
        }

        private async Task ValidateSubscriptions(DocumentStore restoredStore)
        {
            var subscriptions = restoredStore.Subscriptions.GetSubscriptions(0, 10);

            int count = 0;

            foreach (var sub in subscriptions)
            {
                var worker = restoredStore.Subscriptions.GetSubscriptionWorker<User>(sub.SubscriptionName);
                var t = worker.Run((batch) =>
                {
                    Interlocked.Add(ref count, batch.NumberOfItemsInBatch);
                });
                GC.KeepAlive(t);
            }

            using (var session = restoredStore.OpenSession())
            {
                session.Store(new User { Name = "Marcin" }, "users/1");
                session.Store(new User { Name = "Karmel" }, "users/2");
                session.SaveChanges();
            }

            var actual = await WaitForValueAsync(() => count, 3);
            Assert.Equal(3, actual);

            foreach (var subscription in subscriptions)
            {
                Assert.NotNull(subscription.SubscriptionName);
                Assert.NotNull(subscription.Query);
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_9912).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }

        private static string GetSorter(string name)
        {
            using (var stream = GetDump(name))
            using (var reader = new StreamReader(stream))
                return reader.ReadToEnd();
        }
    }
}
