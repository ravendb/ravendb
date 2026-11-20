using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_28078 : ClusterTestBase
    {
        public RavenDB_28078(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.BackupExportImport)]
        public void NewPeriodicBackupTaskGetsCreatedAtFromSendingNode()
        {
            var command = new UpdatePeriodicBackupCommand(new PeriodicBackupConfiguration { Name = "backup", FullBackupFrequency = "0 * * * *" }, "db", "id");
            var createdAt = command.Configuration.CreatedAt;
            Assert.NotNull(createdAt);

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.ReadObject(command.ToJson(context), "command");

                var first = ApplyOnNewRecord<UpdatePeriodicBackupCommand>(json);
                Thread.Sleep(20);
                var second = ApplyOnNewRecord<UpdatePeriodicBackupCommand>(json);

                Assert.Equal(createdAt, first.PeriodicBackups[0].CreatedAt);
                Assert.Equal(createdAt, second.PeriodicBackups[0].CreatedAt);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Configuration, LicenseRequired = true)]
        public async Task SchemaDefinitionKeepsLastModifiedTimeWhenTheRecordIsRewritten()
        {
            using var store = GetDocumentStore();

            var definition = new SchemaDefinition { Schema = "{\"properties\":{\"Name\":{\"type\":\"string\"}}}" };
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition> { ["Users"] = definition }
            }));

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(definition.LastModifiedTime, record.SchemaValidation.ValidatorsPerCollection["Users"].LastModifiedTime);

            await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 60 }));

            record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(definition.LastModifiedTime, record.SchemaValidation.ValidatorsPerCollection["Users"].LastModifiedTime);
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task ClusterTransactionResultIsTheSameOnAllNodes()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "Oren" }, "users/1");
                await session.SaveChangesAsync();
            }

            await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(leader.ServerStore.LastRaftCommitIndex, nodes);

            var results = nodes.Select(GetClusterTransactionResults).ToList();
            Assert.Single(results[0]);
            Assert.All(results, x => Assert.Equal(results[0], x));
        }

        private static List<string> GetClusterTransactionResults(RavenServer server)
        {
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                return server.ServerStore.Engine.LogHistory.GetHistoryLogs(context)
                    .Where(x => (string)x["Type"] == nameof(ClusterTransactionCommand))
                    .Select(x => (string)x["Result"])
                    .ToList();
            }
        }

        private static DatabaseRecord ApplyOnNewRecord<T>(BlittableJsonReaderObject json) where T : UpdateDatabaseCommand
        {
            var command = (T)JsonDeserializationCluster.Commands[typeof(T).Name](json);
            var record = new DatabaseRecord(command.DatabaseName);
            command.UpdateDatabaseRecord(record, 10);
            return record;
        }

    }
}
