using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Commands.Indexes;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Company = Orders.Company;

namespace SlowTests.Issues
{
    public class RavenDB_21846 : ReplicationTestBase
    {
        public RavenDB_21846(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        public async Task CanCalculateIndexTombstoneStateCorrectly()
        {
            var index1Name = "MyTsIndex";
            var index2Name = "MyCounterIndex";

            using (var store = GetDocumentStore())
            {
                new MyTsIndex().Execute(store);
                new MyCounterIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company, "companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.UtcNow, new double[] { 3 }, "tag");
                    session.CountersFor(company).Increment("HeartRate", 6);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                var tombstoneCleaner = database.TombstoneCleaner;
                var state = tombstoneCleaner.GetState(true);

                var tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"Index/{index1Name}/Companies"];
                Assert.Equal("Index", tsIndexTombstoneInfo.Process);
                Assert.Equal(index1Name, tsIndexTombstoneInfo.Identifier);
                Assert.Equal("Companies", tsIndexTombstoneInfo.Collection);
                Assert.Equal(0, tsIndexTombstoneInfo.NumberOfTombstoneLeft);
                tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"Index/{index2Name}/Companies"];
                Assert.Equal("Index", tsIndexTombstoneInfo.Process);
                Assert.Equal(index2Name, tsIndexTombstoneInfo.Identifier);
                Assert.Equal("Companies", tsIndexTombstoneInfo.Collection);
                Assert.Equal(0, tsIndexTombstoneInfo.NumberOfTombstoneLeft);

                await  Server.ServerStore.Engine.PutToLeaderAsync(new SetIndexStateCommand(index1Name, IndexState.Disabled, database.Name, Guid.NewGuid().ToString()));
                await WaitForValueAsync(async () =>
                {
                    database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    return database.IndexStore.GetIndex(index1Name).State;
                }, IndexState.Disabled);
                Assert.Equal(IndexState.Disabled, database.IndexStore.GetIndex(index1Name).State);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Delete();

                    session.SaveChanges();
                }

                state = tombstoneCleaner.GetState(true);

                tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"Index/{index1Name}/Companies"];
                Assert.Equal("Index", tsIndexTombstoneInfo.Process);
                Assert.Equal(index1Name, tsIndexTombstoneInfo.Identifier);
                Assert.Equal("Companies", tsIndexTombstoneInfo.Collection);
                Assert.Equal(1, tsIndexTombstoneInfo.NumberOfTombstoneLeft);
                tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"Index/{index2Name}/Companies"];
                Assert.Equal("Index", tsIndexTombstoneInfo.Process);
                Assert.Equal(index2Name, tsIndexTombstoneInfo.Identifier);
                Assert.Equal("Companies", tsIndexTombstoneInfo.Collection);
                Assert.Equal(0, tsIndexTombstoneInfo.NumberOfTombstoneLeft);

                await Server.ServerStore.Engine.PutToLeaderAsync(new SetIndexStateCommand(index2Name, IndexState.Disabled, database.Name, Guid.NewGuid().ToString()));
                await WaitForValueAsync(async () =>
                {
                    database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    return database.IndexStore.GetIndex(index2Name).State;
                }, IndexState.Disabled);
                Assert.Equal(IndexState.Disabled, database.IndexStore.GetIndex(index2Name).State);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.Delete(company);

                    session.SaveChanges();
                }

                state = tombstoneCleaner.GetState(true);

                tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"Index/{index1Name}/Companies"];
                Assert.Equal("Index", tsIndexTombstoneInfo.Process);
                Assert.Equal(index1Name, tsIndexTombstoneInfo.Identifier);
                Assert.Equal("Companies", tsIndexTombstoneInfo.Collection);
                Assert.Equal(2, tsIndexTombstoneInfo.NumberOfTombstoneLeft);
                tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"Index/{index2Name}/Companies"];
                Assert.Equal("Index", tsIndexTombstoneInfo.Process);
                Assert.Equal(index2Name, tsIndexTombstoneInfo.Identifier);
                Assert.Equal("Companies", tsIndexTombstoneInfo.Collection);
                Assert.Equal(1, tsIndexTombstoneInfo.NumberOfTombstoneLeft);
            }
        }

        public readonly string DbName = "TestDB" + Guid.NewGuid();

        [RavenFact(RavenTestCategory.Replication)]
        public async Task CanCalculateReplicationTombstoneStateCorrectly()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var externalList = await SetupReplicationAsync(store1, store2);
                await Databases.GetDocumentDatabaseInstanceFor(store1);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "AA",
                        }, "users/1");
                    session.SaveChanges();
                }

                var external = new ExternalReplication(store1.Database, $"ConnectionString-{store2.Identifier}")
                {
                    Name = "Task1",
                    TaskId = externalList.First().TaskId,
                    Disabled = true
                };

                var res = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));
                Assert.Equal(externalList.First().TaskId, res.TaskId);


                using (var session = store1.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store1);
                var tombstoneCleaner = database.TombstoneCleaner;
                var state = tombstoneCleaner.GetState(true);

                var tsIndexTombstoneInfo = state.PerSubscriptionInfo.First().Value;
                Assert.Equal("Replication", tsIndexTombstoneInfo.Process);
                Assert.Equal("Task1", tsIndexTombstoneInfo.Identifier);
                Assert.Equal("", tsIndexTombstoneInfo.Collection);
                Assert.Equal(1, tsIndexTombstoneInfo.NumberOfTombstoneLeft);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task CanCalculateEtlTombstoneStateCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "AA",
                    }, "users/1");
                    session.SaveChanges();
                }

                var connectionStringName = "con1";
                var urls = new[] { Server.WebUrl };
                var config = new RavenEtlConfiguration()
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL1",
                            Collections = new List<string>(new[] {"Users"}),
                            Script = null,
                            ApplyToAllDocuments = false,
                            Disabled = false
                        }
                    },
                    LoadRequestTimeoutInSec = 30
                };
                var connectionString = new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = store.Database,
                    TopologyDiscoveryUrls = urls,
                };
                DocumentDatabase database;
                var result = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(result.RaftCommandIndex);

                var res = store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(config));

                config.Disabled = true;
                store.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(res.TaskId, config));

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var tombstoneCleaner = database.TombstoneCleaner;
                var state = tombstoneCleaner.GetState(true);

                var tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"ETL/{config.ConnectionStringName}/Users"];
                Assert.Equal("ETL", tsIndexTombstoneInfo.Process);
                Assert.Equal(config.Name, tsIndexTombstoneInfo.Identifier);
                Assert.Equal("Users", tsIndexTombstoneInfo.Collection);
                Assert.Equal(1, tsIndexTombstoneInfo.NumberOfTombstoneLeft);

                tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"ETL/{config.ConnectionStringName}/{AttachmentsStorage.AttachmentsTombstones}"];
                Assert.Equal("ETL", tsIndexTombstoneInfo.Process);
                Assert.Equal(config.Name, tsIndexTombstoneInfo.Identifier);
                Assert.Equal(AttachmentsStorage.AttachmentsTombstones, tsIndexTombstoneInfo.Collection);
                Assert.Equal(0, tsIndexTombstoneInfo.NumberOfTombstoneLeft);

                tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"ETL/{config.ConnectionStringName}/"];
                Assert.Equal("ETL", tsIndexTombstoneInfo.Process);
                Assert.Equal(config.Name, tsIndexTombstoneInfo.Identifier);
                Assert.Equal("", tsIndexTombstoneInfo.Collection);
                Assert.Equal(0, tsIndexTombstoneInfo.NumberOfTombstoneLeft);

            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CanCalculateBackupTombstoneStateCorrectly()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "AA" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "1 0 0 0 0", name: "BackUp1");
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                var backupStatus = store.Maintenance.Send(operation);
                var backupOperationId = backupStatus.Status.LastOperationId;

                var backupOperation = store.Maintenance.Send(new GetOperationStateOperation(backupOperationId.Value));
                Assert.Equal(OperationStatus.Completed, backupOperation.Status);

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                var  database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var tombstoneCleaner = database.TombstoneCleaner;
                var state = tombstoneCleaner.GetState(true);

                var tsIndexTombstoneInfo = state.PerSubscriptionInfo[$"Periodic Backup/BackUp1/"];
                Assert.Equal("Periodic Backup", tsIndexTombstoneInfo.Process);
                Assert.Equal("BackUp1", tsIndexTombstoneInfo.Identifier);
                Assert.Equal("", tsIndexTombstoneInfo.Collection);
                Assert.Equal(1, tsIndexTombstoneInfo.NumberOfTombstoneLeft);
            }
        }

        private class MyTsIndex : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public MyTsIndex()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });
            }
        }

        private class MyCounterIndex : AbstractCountersIndexCreationTask<Company>
        {
            public MyCounterIndex()
            {
                AddMap("HeartRate", counters => from counter in counters
                                                select new
                                                {
                                                    HeartBeat = counter.Value,
                                                    counter.Name,
                                                    User = counter.DocumentId
                                                });
            }
        }
    }
}
