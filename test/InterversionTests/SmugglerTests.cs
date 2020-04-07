using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
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
using Xunit.Abstractions;

namespace InterversionTests
{
    public class SmugglerTests : InterversionTestBase
    {
        //TODO Need to be changed to version with relevant fix
        private const string Server42Version = "4.2.101";

        public SmugglerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanExportFrom41AndImportToCurrent()
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
#pragma warning disable 618
                    options.OperateOnTypes &= ~DatabaseItemType.CounterGroups;
                    options.OperateOnTypes &= ~DatabaseItemType.Subscriptions;
                    options.OperateOnTypes &= ~DatabaseItemType.CompareExchangeTombstones;
#pragma warning restore 618

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
        public async Task CanExportFromCurrentAndImportTo41()
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
                    options.OperateOnTypes &= ~DatabaseItemType.CounterGroups;
                    options.OperateOnTypes &= ~DatabaseItemType.Subscriptions;
                    options.OperateOnTypes &= ~DatabaseItemType.CompareExchangeTombstones;
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
        public async Task CanExportAndImportCurrentClientWithServer41()
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

        [Fact]
        public async Task CanExportFrom42AndImportToCurrent()
        {
            var file = GetTempFileName();
            using var store42 = await GetDocumentStoreAsync(Server42Version);
            using var store5 = GetDocumentStore();

            store42.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = store42.OpenAsyncSession())
            {
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.CountersFor(user).Increment("Like");
                }
                await session.SaveChangesAsync();
            }

            //Export
            var exportOptions = new DatabaseSmugglerExportOptions();
            exportOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;

            var exportOperation = await store42.Smuggler.ExportAsync(exportOptions, file);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var expected = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var importOptions = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };

            var importOperation = await store5.Smuggler.ImportAsync(importOptions, file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var actual = await store5.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);
            Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
        }

        [Fact]
        public async Task CanExportFromCurrentAndImportTo42()
        {
            var file = GetTempFileName();
            using var store42 = await GetDocumentStoreAsync(Server42Version);
            using var store5 = GetDocumentStore();
            //Export
            store5.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = store5.OpenAsyncSession())
            {
                var dateTime = new DateTime(2020, 3, 29);

                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user, "Heartrate").Append(dateTime, 59d, "watches/fitbit");
                }
                await session.SaveChangesAsync();
            }
            var exportOperation = await store5.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            DatabaseStatistics expected = await store5.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var importOptions = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };
            importOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;
            var importOperation = await store42.Smuggler.ImportAsync(importOptions, file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var actual = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);
            Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
        }

        [Fact]
        public async Task CanExportAndImportCurrentClientWithServer42()
        {
            var file = GetTempFileName();
            using var store42 = await GetDocumentStoreAsync(Server42Version);

            store42.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = store42.OpenAsyncSession())
            {
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.CountersFor(user).Increment("Like");
                }
                await session.SaveChangesAsync();
            }

            //Export
            var exportOptions = new DatabaseSmugglerExportOptions();
            exportOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;
            var exportOperation = await store42.Smuggler.ExportAsync(exportOptions, file);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var expected = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var importOptions = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };
            importOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;

            var importOperation = await store42.Smuggler.ImportAsync(importOptions, file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var actual = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);
            Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
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
