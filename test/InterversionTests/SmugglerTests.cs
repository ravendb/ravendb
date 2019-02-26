using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Operations;
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
using Tests.Infrastructure;
using Xunit;

namespace InterversionTests
{
    public class SmugglerTests : InterversionTestBase
    {
        [Fact]
        public async Task CanExportFrom40AndImportTo41()
        {
            var file = GetTempFileName();
            long countOfDocuments;
            long countOfAttachments;
            long countOfIndexes;
            long countOfRevisions;

            try
            {
                using (var store40 = await GetDocumentStoreAsync("4.0.6-patch-40047"))
                {
                    store40.Maintenance.Send(new CreateSampleDataOperation());

                    var options = new DatabaseSmugglerExportOptions();
                    options.OperateOnTypes &= ~DatabaseItemType.CounterGroups;

                    var operation = await store40.Smuggler.ExportAsync(options, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store40.Maintenance.SendAsync(new GetStatisticsOperation());

                    countOfDocuments = stats.CountOfDocuments;
                    countOfAttachments = stats.CountOfAttachments;
                    countOfIndexes = stats.CountOfIndexes;
                    countOfRevisions = stats.CountOfRevisionDocuments;

                }

                using (var store41 = GetDocumentStore())
                {
                    var options = new DatabaseSmugglerImportOptions
                    {
                        SkipRevisionCreation = true
                    };

                    var operation = await store41.Smuggler.ImportAsync(options, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store41.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(countOfDocuments, stats.CountOfDocuments);
                    Assert.Equal(countOfAttachments, stats.CountOfAttachments);
                    Assert.Equal(countOfIndexes, stats.CountOfIndexes);
                    Assert.Equal(countOfRevisions, stats.CountOfRevisionDocuments);
                }
            }
            finally
            {
                File.Delete(file);
            }

        }

        [Fact]
        public async Task CanExportFrom41AndImportTo40()
        {
            var file = GetTempFileName();
            long countOfDocuments;
            long countOfAttachments;
            long countOfIndexes;
            long countOfRevisions;

            try
            {
                using (var store41 = GetDocumentStore())
                {
                    store41.Maintenance.Send(new CreateSampleDataOperation());

                    using (var session = store41.OpenSession())
                    {
                        var o = session.Load<Order>("orders/1-A");
                        Assert.NotNull(o);
                        session.CountersFor(o).Increment("downloads", 100);
                        session.SaveChanges();
                    }

                    var operation = await store41.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store41.Maintenance.SendAsync(new GetStatisticsOperation());

                    countOfDocuments = stats.CountOfDocuments;
                    countOfAttachments = stats.CountOfAttachments;
                    countOfIndexes = stats.CountOfIndexes;
                    countOfRevisions = stats.CountOfRevisionDocuments;

                    Assert.Equal(1, stats.CountOfCounterEntries);
                }

                using (var store40 = await GetDocumentStoreAsync("4.0.6-patch-40047"))
                {

                    var options = new DatabaseSmugglerImportOptions();
                    options.OperateOnTypes &= ~DatabaseItemType.CounterGroups;
                    options.SkipRevisionCreation = true;

                    var operation = await store40.Smuggler.ImportAsync(options, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store40.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(countOfDocuments, stats.CountOfDocuments);
                    Assert.Equal(countOfAttachments, stats.CountOfAttachments);
                    Assert.Equal(countOfIndexes, stats.CountOfIndexes);
                    Assert.Equal(countOfRevisions, stats.CountOfRevisionDocuments);

                    Assert.Equal(0, stats.CountOfCounterEntries);

                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportFrom41AndImportTo42()
        {
            var file = GetTempFileName();

            try
            {
                long countOfDocuments;
                long countOfAttachments;
                long countOfIndexes;
                long countOfRevisions;
                using (var store41 = await GetDocumentStoreAsync("4.1.4", new InterversionTestOptions
                {
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
                                TaskId = 4
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
                    store41.Maintenance.Send(new CreateSampleDataOperation());

                    var options = new DatabaseSmugglerExportOptions();
                    options.OperateOnTypes &= ~DatabaseItemType.Counters;

                    var operation = await store41.Smuggler.ExportAsync(options, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store41.Maintenance.SendAsync(new GetStatisticsOperation());

                    countOfDocuments = stats.CountOfDocuments;
                    countOfAttachments = stats.CountOfAttachments;
                    countOfIndexes = stats.CountOfIndexes;
                    countOfRevisions = stats.CountOfRevisionDocuments;

                }

                using (var store42 = GetDocumentStore())
                {
                    var options = new DatabaseSmugglerImportOptions
                    {
                        SkipRevisionCreation = true
                    };

                    var operation = await store42.Smuggler.ImportAsync(options, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(countOfDocuments, stats.CountOfDocuments);
                    Assert.Equal(countOfAttachments, stats.CountOfAttachments);
                    Assert.Equal(countOfIndexes, stats.CountOfIndexes);
                    Assert.Equal(countOfRevisions, stats.CountOfRevisionDocuments);

                    var record = await store42.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store42.Database));

                    record.Settings.TryGetValue("Patching.MaxNumberOfCachedScripts", out string value);
                    Assert.Null(value);
                    Assert.Null(record.ConflictSolverConfig);
                    Assert.Equal(0, record.Sorters.Count);
                    Assert.Equal(0, record.ExternalReplications.Count);
                    Assert.Equal(0, record.SinkPullReplications.Count);
                    Assert.Equal(0, record.HubPullReplications.Count);
                    Assert.Equal(0, record.RavenEtls.Count);
                    Assert.Equal(0, record.SqlEtls.Count);
                    Assert.Equal(0, record.PeriodicBackups.Count);
                }
            }
            finally
            {
                File.Delete(file);
            }

        }

        [Fact]
        public async Task CanExportFrom42AndImportTo41()
        {
            var file = GetTempFileName();
            try
            {
                long countOfDocuments;
                using (var store42 = GetDocumentStore(new Options
                {
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
                                TaskId = 4
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
                    using (var session = store42.OpenSession())
                    {
                        for (var i = 0; i < 5; i++)
                        {
                            session.Store(new User { Name = "raven" + i });
                        }
                        session.SaveChanges();
                    }
                        var operation = await store42.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

                    countOfDocuments = stats.CountOfDocuments;
                }

                using (var store41 = await GetDocumentStoreAsync("4.1.4"))
                {

                    var options = new DatabaseSmugglerImportOptions();
                    options.SkipRevisionCreation = true;

                    var operation = await store41.Smuggler.ImportAsync(options, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store41.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(countOfDocuments, stats.CountOfDocuments);

                    var record = await store41.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store41.Database));

                    record.Settings.TryGetValue("Patching.MaxNumberOfCachedScripts", out string value);
                    Assert.Null(value);
                    Assert.Null(record.ConflictSolverConfig);
                    Assert.Equal(0, record.Sorters.Count);
                    Assert.Equal(0, record.ExternalReplications.Count);
                    Assert.Equal(0, record.SinkPullReplications.Count);
                    Assert.Equal(0, record.HubPullReplications.Count);
                    Assert.Equal(0, record.RavenEtls.Count);
                    Assert.Equal(0, record.SqlEtls.Count);
                    Assert.Equal(0, record.PeriodicBackups.Count);

                }
            }
            finally
            {
                File.Delete(file);
            }

        }

        [Fact]
        public async Task CanExportAndImportClient42Server41()
        {
            var file = GetTempFileName();
            try
            {
                using (var store41 = await GetDocumentStoreAsync("4.1.4"))
                {
                    using (var session = store41.OpenSession())
                    {
                        for (var i = 0; i < 5; i++)
                        {
                            session.Store(new User { Name = "raven" + i });
                        }
                        session.SaveChanges();
                    }
                    var operation = await store41.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnDatabaseRecordTypes = DatabaseRecordItemType.PeriodicBackups,
                        OperateOnTypes = DatabaseItemType.DatabaseRecord
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store41.Maintenance.SendAsync(new GetStatisticsOperation());

                    long countOfDocuments = stats.CountOfDocuments;
                    var options = new DatabaseSmugglerImportOptions()
                    {
                        OperateOnDatabaseRecordTypes = DatabaseRecordItemType.PeriodicBackups,
                        OperateOnTypes = DatabaseItemType.DatabaseRecord
                    };
                    options.SkipRevisionCreation = true;

                    operation = await store41.Smuggler.ImportAsync(options, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    stats = await store41.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(countOfDocuments, stats.CountOfDocuments);
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
