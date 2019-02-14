using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Issues;
using Xunit;

namespace SlowTests.Smuggler
{
    public class BackupDatabaseRecordTests : RavenTestBase
    {
        [Fact]
        public async Task CanExportAndImportDatabaseRecord()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_1",

                    ModifyDatabaseRecord = record =>
                    {
                        record.Settings[RavenConfiguration.GetKey(x => x.Patching.MaxNumberOfCachedScripts)] = "1024";
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
                        record.ExternalReplications = new List<ExternalReplication>
                        {
                            new ExternalReplication("tempDatabase", "ExternalReplication")
                            {
                                TaskId = 1,
                                Name = "External",
                                MentorNode = "B",
                                DelayReplicationFor = new TimeSpan(4),
                                Url = "http://127.0.0.1/",
                                Disabled = false
                            }
                        };
                        record.SinkPullReplications = new List<PullReplicationAsSink>
                        {
                            new PullReplicationAsSink()
                            {
                                Database = "sinkDatabase",
                                CertificatePassword = "CertificatePassword",
                                CertificateWithPrivateKey = "CertificateWithPrivateKey",
                                TaskId = 2,
                                Name = "Sink",
                                MentorNode = "A",
                                DelayReplicationFor = new TimeSpan(3),
                                HubDefinitionName = "hub"

                            }
                        };
                        record.HubPullReplications = new List<PullReplicationDefinition>
                        {
                            new PullReplicationDefinition()
                            {
                                TaskId = 3,
                                Name = "hub",
                                MentorNode = "A",
                                DelayReplicationFor = new TimeSpan(3),
                            }
                        };
                        record.RavenEtls = new List<RavenEtlConfiguration>
                        {
                            new RavenEtlConfiguration()
                            {
                                AllowEtlOnNonEncryptedChannel = true,
                                ConnectionStringName = "ConnectionName",
                                MentorNode = "A",
                                Name = "Etl",
                                TaskId = 4,
                                TestMode = true
                            }
                        };
                        record.SqlEtls = new List<SqlEtlConfiguration>
                        {
                            new SqlEtlConfiguration()
                            {
                                AllowEtlOnNonEncryptedChannel = true,
                                ConnectionStringName = "connection",
                                ForceQueryRecompile = false,
                                Name = "sql",
                                ParameterizeDeletes = false,
                                TaskId = 5
                            }
                        };
                    }

                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2"
                }))
                {
                    var config = new PeriodicBackupConfiguration
                    {
                        Disabled = false,
                        MentorNode = "A",
                        Name = "Backup",
                        BackupType = BackupType.Backup,
                        FullBackupFrequency = "0 */1 * * *",
                        IncrementalBackupFrequency = "0 */6 * * *",
                        LocalSettings = new LocalSettings()
                        {
                            FolderPath = "FolderPath"
                        }
                    };

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
                    Assert.Equal("hub", record.SinkPullReplications[0].HubDefinitionName);
                    Assert.Equal("CertificatePassword", record.SinkPullReplications[0].CertificatePassword);
                    Assert.Equal("CertificateWithPrivateKey", record.SinkPullReplications[0].CertificateWithPrivateKey);
                    Assert.Equal(new TimeSpan(3), record.SinkPullReplications[0].DelayReplicationFor);
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
                    Assert.Equal(false, record.SqlEtls.First().Disabled);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanBackupAndRestoreDatabaseRecord()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var file = Path.GetTempFileName();
            try
            {
                using (var store = GetDocumentStore(new Options
                {
                    
                    ModifyDatabaseName = s => $"{s}_1",

                    ModifyDatabaseRecord = record =>
                    {
                        record.Settings[RavenConfiguration.GetKey(x => x.Patching.MaxNumberOfCachedScripts)] = "1024";
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
                        record.ExternalReplications = new List<ExternalReplication>
                        {
                            new ExternalReplication("tempDatabase", "ExternalReplication")
                            {
                                TaskId = 1,
                                Name = "External",
                                DelayReplicationFor = new TimeSpan(4),
                                Url = "http://127.0.0.1/",
                                Disabled = false
                            }
                        };
                        record.SinkPullReplications = new List<PullReplicationAsSink>
                        {
                            new PullReplicationAsSink()
                            {
                                Database = "sinkDatabase",
                                CertificatePassword = "CertificatePassword",
                                CertificateWithPrivateKey = "CertificateWithPrivateKey",
                                TaskId = 2,
                                Name = "Sink",
                                DelayReplicationFor = new TimeSpan(3),
                                HubDefinitionName = "hub"

                            }
                        };
                        record.HubPullReplications = new List<PullReplicationDefinition>
                        {
                            new PullReplicationDefinition()
                            {
                                TaskId = 3,
                                Name = "hub",
                                MentorNode = "A",
                                DelayReplicationFor = new TimeSpan(3),
                            }
                        };
                        record.RavenEtls = new List<RavenEtlConfiguration>
                        {
                            new RavenEtlConfiguration()
                            {
                                AllowEtlOnNonEncryptedChannel = true,
                                ConnectionStringName = "ConnectionName",
                                MentorNode = "A",
                                Name = "Etl",
                                TaskId = 4,
                                TestMode = true
                            }
                        };
                        record.SqlEtls = new List<SqlEtlConfiguration>
                        {
                            new SqlEtlConfiguration()
                            {
                                AllowEtlOnNonEncryptedChannel = true,
                                ConnectionStringName = "connection",
                                ForceQueryRecompile = false,
                                Name = "sql",
                                ParameterizeDeletes = false,
                                TaskId = 5
                            }
                        };
                    }
                }))
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "oren"
                        }, "users/1");
                        await session.SaveChangesAsync();
                    }

                    var config = new PeriodicBackupConfiguration
                    {
                        Name = "Real",
                        BackupType = BackupType.Backup,
                        LocalSettings = new LocalSettings
                        {
                            FolderPath = backupPath
                        },
                        IncrementalBackupFrequency = "0 */5 * * *"
                    };
                    var config2 = new PeriodicBackupConfiguration
                    {
                        Disabled = false,
                        MentorNode = "A",
                        Name = "Backup",
                        BackupType = BackupType.Backup,
                        FullBackupFrequency = "0 */1 * * *",
                        IncrementalBackupFrequency = "0 */6 * * *",
                        LocalSettings = new LocalSettings()
                        {
                            FolderPath = backupPath
                        }
                    };

                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config2));
                    var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                    await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                    var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                    
                    var value = WaitForValue(() =>
                    {
                        var getPeriodicBackupResult = store.Maintenance.Send(operation);
                        return getPeriodicBackupResult.Status?.LastEtag;
                    }, 1);
                    Assert.Equal(1, value);
                    var databaseName = $"restored_database-{Guid.NewGuid()}";

                    using (RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = databaseName,
                    }))
                    {
                        var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;
                        var backups = periodicBackupRunner.PeriodicBackups;
                        
                        Assert.Equal(2, backups.Count);
                        Assert.Equal("Backup", backups.First().Configuration.Name);
                        Assert.Equal(true, backups.First().Configuration.IncrementalBackupFrequency.Equals("0 */6 * * *"));
                        Assert.Equal(true, backups.First().Configuration.FullBackupFrequency.Equals("0 */1 * * *"));
                        Assert.Equal(BackupType.Backup, backups.First().Configuration.BackupType);
                        Assert.Equal(false, backups.Any(x => x.Configuration.Disabled));

                        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                        Assert.Equal("1024", record.Settings["Patching.MaxNumberOfCachedScripts"]);

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
                        Assert.Equal("hub", record.SinkPullReplications[0].HubDefinitionName);
                        Assert.Equal("CertificatePassword", record.SinkPullReplications[0].CertificatePassword);
                        Assert.Equal("CertificateWithPrivateKey", record.SinkPullReplications[0].CertificateWithPrivateKey);
                        Assert.Equal(new TimeSpan(3), record.SinkPullReplications[0].DelayReplicationFor);
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
            finally
            {
                File.Delete(file);
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
