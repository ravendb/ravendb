using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
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
                    options.OperateOnTypes &= ~DatabaseItemType.Counters;

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

                    Assert.Equal(1, stats.CountOfCounters);
                }

                using (var store40 = await GetDocumentStoreAsync("4.0.6-patch-40047"))
                {

                    var options = new DatabaseSmugglerImportOptions();
                    options.OperateOnTypes &= ~DatabaseItemType.Counters;
                    options.SkipRevisionCreation = true;

                    var operation = await store40.Smuggler.ImportAsync(options, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store40.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(countOfDocuments, stats.CountOfDocuments);
                    Assert.Equal(countOfAttachments, stats.CountOfAttachments);
                    Assert.Equal(countOfIndexes, stats.CountOfIndexes);
                    Assert.Equal(countOfRevisions, stats.CountOfRevisionDocuments);

                    Assert.Equal(0, stats.CountOfCounters);

                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportAndImportClient41Server42()
        {
            var file = GetTempFileName();
            try
            {
                long countOfDocuments;
                using (var store42export = await GetDocumentStoreAsync("4.2.0-nightly-20190223-0601", new InterversionTestOptions
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
                                ForceQueryRecompile = false,
                                ConnectionStringName = "connection",
                                Name = "sql",
                                ParameterizeDeletes = false,
                                MentorNode = "A"
                            }
                        };
                    }
                }))
                {
                    using (var session = store42export.OpenSession())
                    {
                        for (var i = 0; i < 5; i++)
                        {
                            session.Store(new User
                            {
                                Name = "raven" + i
                            });
                        }

                        session.SaveChanges();
                    }

                    var operation = await store42export.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store42export.Maintenance.SendAsync(new GetStatisticsOperation());
                    countOfDocuments = stats.CountOfDocuments;
                }
                using (var store42Import = await GetDocumentStoreAsync("4.2.0-nightly-20190223-0601", new InterversionTestOptions
                {
                    ModifyDatabaseName = s => $"{s}_2",

                }))
                {
                    var options = new DatabaseSmugglerImportOptions();
                    options.SkipRevisionCreation = true;

                    var operation = await store42Import.Smuggler.ImportAsync(options, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store42Import.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(countOfDocuments, stats.CountOfDocuments);
                    var record = await store42Import.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store42Import.Database));

                    record.Settings.TryGetValue("Patching.MaxNumberOfCachedScripts", out string value);
                    Assert.Null(value);

                    Assert.NotNull(record.ConflictSolverConfig);
                    Assert.Equal(false, record.ConflictSolverConfig.ResolveToLatest);
                    Assert.Equal(1, record.ConflictSolverConfig.ResolveByCollection.Count);
                    Assert.Equal(true, record.ConflictSolverConfig.ResolveByCollection.TryGetValue("ConflictSolver", out ScriptResolver sr));
                    Assert.Equal("Script", sr.Script);

                    Assert.Equal(1, record.ExternalReplications.Count);
                    Assert.Equal("tempDatabase", record.ExternalReplications[0].Database);
                    Assert.Equal(true, record.ExternalReplications[0].Disabled);

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
    }
}
